"""
sheets_db.py — Google Sheet as Vida's living contact database.
==============================================================
Replaces the old Mailchimp source. The sheet (tab `mailchimp_export`) is the
single source of truth: we read eligible rows, and write a result back to the
exact row every time we act on it (sent / bounced / replied / SQL / etc.).

Auth: a service account. Its JSON key is provided whole in the env var
GOOGLE_SERVICE_ACCOUNT_JSON (a GitHub Actions secret). The sheet must be
shared with the service account's email as Editor.

Column layout of `mailchimp_export` (1-indexed / A1):
  A email            H member_rating      O do_not_contact
  B first_name       I last_changed       P touches        (managed here)
  C last_name        J status (sub)       Q last_result    (managed here)
  D company          K source_list
  E tags             L contacted
  F avg_open_rate    M date_sent
  G avg_click_rate   N reply_status  <- pipeline status enum lives here
"""
import json
import os
import time

SHEET_TAB = "mailchimp_export"

# 0-indexed column positions
COL = {
    "email": 0, "first": 1, "last": 2, "company": 3, "tags": 4,
    "open": 5, "click": 6, "rating": 7, "last_changed": 8, "sub_status": 9,
    "source_list": 10, "contacted": 11, "date_sent": 12, "reply_status": 13,
    "do_not_contact": 14, "touches": 15, "last_result": 16,
}
# A1 letters for the writable fields (1-indexed columns)
CELL = {
    "first": "B", "last": "C",
    "contacted": "L", "date_sent": "M", "reply_status": "N",
    "do_not_contact": "O", "touches": "P", "last_result": "Q",
}
LAST_COL = "Q"

PLACEHOLDER_ADDRESSES = {
    "email@yourbusiness.com", "test@test.com", "example@example.com",
    "user@example.com", "name@domain.com",
}


def service():
    """Build an authenticated Sheets API client from the service-account JSON."""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not set")
    info = json.loads(raw)
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _get(svc, spreadsheet_id, a1):
    r = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"{SHEET_TAB}!{a1}"
    ).execute()
    return r.get("values", [])


def ensure_headers(svc, spreadsheet_id):
    """Label the two columns we manage, if they aren't already."""
    hdr = _get(svc, spreadsheet_id, "P1:Q1")
    row = hdr[0] if hdr else []
    p = row[0] if len(row) > 0 else ""
    q = row[1] if len(row) > 1 else ""
    if p.strip().lower() != "touches" or q.strip().lower() != "last_result":
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=f"{SHEET_TAB}!P1:Q1",
            valueInputOption="RAW", body={"values": [["touches", "last_result"]]},
        ).execute()


def _cell(row, i):
    return row[i].strip() if i < len(row) and row[i] is not None else ""


def _valid_email(e):
    return "@" in e and "." in e.split("@")[-1] and e.lower() not in PLACEHOLDER_ADDRESSES


def parse_row(row_number, row):
    """Turn a raw sheet row into a contact dict (or None if unusable)."""
    email = _cell(row, COL["email"])
    if not _valid_email(email):
        return None
    return {
        "row": row_number,
        "email": email,
        "first_name": _cell(row, COL["first"]),
        "last_name": _cell(row, COL["last"]),
        "company": _cell(row, COL["company"]),
        "tags": [t for t in _cell(row, COL["tags"]).replace("|", ",").split(",") if t],
        "source_list": _cell(row, COL["source_list"]),
        "sub_status": _cell(row, COL["sub_status"]).lower(),
        "contacted": _cell(row, COL["contacted"]),
        "reply_status": _cell(row, COL["reply_status"]),
        "do_not_contact": _cell(row, COL["do_not_contact"]),
    }


def is_eligible(c):
    """A row we may email: not yet contacted, not suppressed, still subscribed."""
    if not c:
        return False
    if c["contacted"]:
        return False
    if c["do_not_contact"].lower() in ("yes", "true", "1", "y"):
        return False
    if c["sub_status"] and c["sub_status"] not in ("subscribed", ""):
        return False
    return True


def read_window(svc, spreadsheet_id, start_row, count):
    """
    Read `count` rows starting at sheet row `start_row` (1-indexed; row 1 is the
    header, so real data starts at row 2). Returns (contacts, last_row_read).
    """
    if start_row < 2:
        start_row = 2
    end_row = start_row + count - 1
    a1 = f"A{start_row}:{LAST_COL}{end_row}"
    rows = _get(svc, spreadsheet_id, a1)
    contacts = []
    for offset, raw in enumerate(rows):
        c = parse_row(start_row + offset, raw)
        if c:
            contacts.append(c)
    last_row_read = start_row + len(rows) - 1 if rows else start_row - 1
    return contacts, last_row_read


def update_row(svc, spreadsheet_id, row_number, **fields):
    """
    Write specific fields back to a single row. Accepts any of:
      first, last, contacted, date_sent, reply_status, do_not_contact,
      touches, last_result
    Only the fields passed are written (no clobbering of the rest).
    """
    data = []
    for key, value in fields.items():
        if key not in CELL or value is None:
            continue
        data.append({
            "range": f"{SHEET_TAB}!{CELL[key]}{row_number}",
            "values": [[str(value)]],
        })
    if not data:
        return
    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"valueInputOption": "USER_ENTERED", "data": data},
    ).execute()


def build_email_index(svc, spreadsheet_id, max_rows=200000):
    """
    Map lowercased email -> sheet row number, for matching inbound replies/bounces
    back to their row. One column read; fine to run per reply_check.
    """
    rows = _get(svc, spreadsheet_id, f"A2:A{max_rows}")
    index = {}
    for offset, r in enumerate(rows):
        if r and r[0]:
            index[r[0].strip().lower()] = offset + 2  # +2: header + 0-index
    return index


def snapshot_status(svc, spreadsheet_id, max_rows=200000):
    """
    email_lower -> {row, contacted, reply_status, do_not_contact} for every valid
    row. One bulk read; used by the one-time reconcile so we never downgrade a row.
    """
    rows = _get(svc, spreadsheet_id, f"A2:O{max_rows}")
    out = {}
    for offset, r in enumerate(rows):
        email = _cell(r, COL["email"])
        if not _valid_email(email):
            continue
        out[email.lower()] = {
            "row": offset + 2,
            "contacted": _cell(r, COL["contacted"]),
            "reply_status": _cell(r, COL["reply_status"]),
            "do_not_contact": _cell(r, COL["do_not_contact"]),
        }
    return out


def batch_update(svc, spreadsheet_id, updates, chunk=500):
    """
    updates: list of (row_number, {field: value}). Writes only the given cells,
    chunked to keep each batchUpdate request reasonable. Returns cells written.
    """
    data = []
    for row_number, fields in updates:
        for key, value in fields.items():
            if key in CELL and value is not None:
                data.append({
                    "range": f"{SHEET_TAB}!{CELL[key]}{row_number}",
                    "values": [[str(value)]],
                })
    for i in range(0, len(data), chunk):
        piece = data[i:i + chunk]
        for attempt in range(5):
            try:
                svc.spreadsheets().values().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={"valueInputOption": "USER_ENTERED", "data": piece},
                ).execute()
                break
            except Exception:
                if attempt == 4:
                    raise
                time.sleep(2 * (attempt + 1))  # transient TLS/network drop — retry
    return len(data)
