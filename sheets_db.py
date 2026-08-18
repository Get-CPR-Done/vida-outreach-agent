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
  D company          K source_list        R reply_date     (managed here)
  E tags             L contacted
  F avg_open_rate    M date_sent     <- the day WE emailed them
  G avg_click_rate   N reply_status  <- pipeline status enum lives here

reply_date (R) is the day the status in N was set — i.e. the day they replied and we
handed them to sales. date_sent (M) only says when we emailed out, so without R there
was no way to answer "which leads came in today" (Chris, 2026-08-11).
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
    "do_not_contact": 14, "touches": 15, "last_result": 16, "reply_date": 17,
}
# A1 letters for the writable fields (1-indexed columns)
CELL = {
    "first": "B", "last": "C",
    "contacted": "L", "date_sent": "M", "reply_status": "N",
    "do_not_contact": "O", "touches": "P", "last_result": "Q",
    "reply_date": "R",
}
LAST_COL = "R"

PLACEHOLDER_ADDRESSES = {
    "email@yourbusiness.com", "test@test.com", "example@example.com",
    "user@example.com", "name@domain.com",
}


def _retry(fn, tries=5):
    """Run a Sheets API call, retrying transient TLS/network drops (SSLEOFError,
    broken pipe, token-refresh failures) that Google's API throws intermittently
    from CI runners. Raises the last error only after all retries are exhausted."""
    last = None
    for attempt in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            time.sleep(2 * (attempt + 1))
    raise last


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
    r = _retry(lambda: svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"{SHEET_TAB}!{a1}"
    ).execute())
    return r.get("values", [])


def ensure_columns(svc, spreadsheet_id, needed=18):
    """Make sure the tab's grid is at least `needed` columns wide.

    The Mailchimp export arrived exactly 17 columns wide (A..Q), and Sheets rejects a
    write past the grid edge outright ("exceeds grid limits") rather than growing it —
    so adding reply_date in R needs the grid widened first. Idempotent: reads the
    current columnCount and appends only the shortfall.
    """
    meta = _retry(lambda: svc.spreadsheets().get(
        spreadsheetId=spreadsheet_id, fields="sheets(properties)").execute())
    for sh in meta.get("sheets", []):
        props = sh.get("properties", {})
        if props.get("title") != SHEET_TAB:
            continue
        have = props.get("gridProperties", {}).get("columnCount", 0)
        if have >= needed:
            return False
        _retry(lambda: svc.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"appendDimension": {
                "sheetId": props["sheetId"],
                "dimension": "COLUMNS",
                "length": needed - have,
            }}]},
        ).execute())
        return True
    raise RuntimeError(f"tab {SHEET_TAB} not found")


def ensure_headers(svc, spreadsheet_id):
    """Label the columns we manage, if they aren't already."""
    ensure_columns(svc, spreadsheet_id, needed=len(COL))
    want = ["touches", "last_result", "reply_date"]
    hdr = _get(svc, spreadsheet_id, "P1:R1")
    row = hdr[0] if hdr else []
    have = [(row[i].strip().lower() if i < len(row) and row[i] else "")
            for i in range(len(want))]
    if have != want:
        _retry(lambda: svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=f"{SHEET_TAB}!P1:R1",
            valueInputOption="RAW", body={"values": [want]},
        ).execute())


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
        "date_sent": _cell(row, COL["date_sent"]),
        "reply_status": _cell(row, COL["reply_status"]),
        "do_not_contact": _cell(row, COL["do_not_contact"]),
        "touches": _cell(row, COL["touches"]),
    }


# Our own domains. Six of these were sitting in the purchased list (chris@getcprdone.com,
# colleens@joffeemergencyservices.com and friends), which means the agents could cold-pitch
# their own colleagues — and any reply would then loop back in as a "lead"
# (Chris, 2026-08-17). Suppressed in the sheet too; this is the belt to that pair of braces.
INTERNAL_EMAIL_DOMAINS = {
    "getcprdone.com", "getcprdone.net", "getcbr.net",
    "joffeemergencyservices.com", "joffeschoolsafety.com",
}


def is_internal(email):
    e = (email or "").strip().lower()
    return "@" in e and e.rsplit("@", 1)[-1] in INTERNAL_EMAIL_DOMAINS


def is_eligible(c):
    """A row we may email: not yet contacted, not suppressed, still subscribed."""
    if not c:
        return False
    if is_internal(c.get("email")):
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


def read_range(svc, spreadsheet_id, start_row, end_row):
    """Read rows start_row..end_row (inclusive) in ONE request and parse them.

    The windowed read_window() costs one API call per 1,000 rows, which is fine when
    walking a short stretch but blows the Sheets quota (60 reads/min/user) when the
    stretch is tens of thousands of rows — e.g. the follow-up scan after a cursor jump
    leaves a large never-contacted gap behind the cursor. One read handles the whole
    span instead. Returns a list of contact dicts (unusable rows dropped).
    """
    if start_row < 2:
        start_row = 2
    if end_row < start_row:
        return []
    rows = _get(svc, spreadsheet_id, f"A{start_row}:{LAST_COL}{end_row}")
    out = []
    for offset, raw in enumerate(rows):
        c = parse_row(start_row + offset, raw)
        if c:
            out.append(c)
    return out


def update_row(svc, spreadsheet_id, row_number, **fields):
    """
    Write specific fields back to a single row. Accepts any of:
      first, last, contacted, date_sent, reply_status, do_not_contact,
      touches, last_result, reply_date
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
    _retry(lambda: svc.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"valueInputOption": "USER_ENTERED", "data": data},
    ).execute())


def read_name(svc, spreadsheet_id, row):
    """Return (first, last) from a single row's B/C cells — the cleaned name we already
    hold, more reliable than parsing a reply's From header."""
    vals = _get(svc, spreadsheet_id, f"B{row}:C{row}")
    r = vals[0] if vals else []
    first = r[0].strip() if len(r) > 0 and r[0] else ""
    last  = r[1].strip() if len(r) > 1 and r[1] else ""
    return first, last


def read_company(svc, spreadsheet_id, row):
    """Return the company (column D) for a single row, or '' if blank."""
    vals = _get(svc, spreadsheet_id, f"D{row}:D{row}")
    r = vals[0] if vals else []
    return r[0].strip() if r and r[0] else ""


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
    rows = _get(svc, spreadsheet_id, f"A2:R{max_rows}")
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
            "date_sent": _cell(r, COL["date_sent"]),
            "reply_date": _cell(r, COL["reply_date"]),
            "first": _cell(r, COL["first"]),
            "last": _cell(r, COL["last"]),
            "company": _cell(r, COL["company"]),
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
