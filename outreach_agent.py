"""
Get CPR Done — Batch Outreach Agent
====================================
Runs M–F only. Sources contacts from a Google Sheet (the living database),
cross-checks HubSpot, routes existing customers to Manae's weekly roster,
and sends personalized outreach emails from Vida to prospects — writing the
result (sent / bounced / replied / SQL) back to each row as it goes.

On any failure      → emails Chris.
On any reply        → forwards to Vida + CCs Manae.
On hard bounce      → logs to do_not_contact, queues org for replacement search.
End of day          → summary report to Chris.

Schedule:
  Daily (M–F) 9:00 AM  → python outreach_agent.py --mode daily
  Monday      9:05 AM  → python outreach_agent.py --mode roster
  Every 2 hrs 9AM–5PM  → python outreach_agent.py --mode reply_check

Usage:
  python outreach_agent.py --mode daily
  python outreach_agent.py --mode roster
  python outreach_agent.py --mode reply_check
  python outreach_agent.py --mode daily --dry-run
"""

import argparse
import base64
import fcntl
import json
import logging
import os
import random
import re
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

import sheets_db

# ─── Config ───────────────────────────────────────────────────────────────────

VIDA_EMAIL    = "vida@getcprdone.com"
MANAE_EMAIL   = "Manae@GetCPRDone.com"
CHRIS_EMAIL   = "chris@getcprdone.com"
SENDING_NAME  = "Vida Monroe"
COMPANY_NAME  = "Get CPR Done"

# Daily send cap. 750/day is the target (working through the full list roughly
# quarterly). Runs unattended on GitHub Actions, so the send spacing is tightened
# to fit the 6-hour job limit (750 * ~15s avg ≈ 3.1h). Single mailbox — Google's
# hard cap is ~2,000/day; to scale past this, add mailboxes/subdomains, don't just
# raise the number. (Sustained 750/day requires the repo to be PUBLIC for free
# unlimited Actions minutes — see README.)
BATCH_SIZE    = 750
MIN_DELAY_SEC = 10
MAX_DELAY_SEC = 20

STATE_FILE = Path(__file__).parent / "state.json"
LOG_FILE   = Path(__file__).parent / "outreach.log"

# ─── Credentials (env vars override these fallbacks) ─────────────────────────

ANTHROPIC_API_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")
HUBSPOT_TOKEN      = os.environ.get("HUBSPOT_TOKEN", "")

# Google Sheet that IS Vida's contact database (both source list and status log)
SPREADSHEET_ID = os.environ.get(
    "SPREADSHEET_ID", "1FWMATFGLXFI-cPm0bAWnU_uh20pRgatq49db_iGNQ2w"
)
# Model used to write emails and classify replies. Haiku is plenty for short,
# templated outreach + reply triage and is ~3-5x cheaper than Sonnet. Bump to
# "claude-sonnet-5" here if the emails ever read flat.
GEN_MODEL = "claude-haiku-4-5"

# Scheduling link used as the call-to-action in Vida's emails (Manae's public
# HubSpot meetings link — safe to hardcode; env var can override).
MANAE_CALENDAR_LINK = os.environ.get(
    "MANAE_CALENDAR_LINK",
    "https://meetings.hubspot.com/manae-deguchi?uuid=76b99631-517e-4aa3-baa3-537375a5db77",
)

INDUSTRY_MAP = [
    (["school","academy","learning","education","elementary","preschool","montessori","kipp","charter"],
     "school or educational organization — staff CPR/AED recertification before the school year"),
    (["construction","contracting","industrial","engineering","trades","builder"],
     "construction or industrial company — OSHA workplace safety compliance"),
    (["church","ministry","congregation","parish","fellowship"],
     "faith-based or community organization — congregation and volunteer safety"),
    (["hospitality","restaurant","hotel","dining","food","catering","bistro"],
     "hospitality or food service business — front-of-house staff safety"),
    (["healthcare","medical","clinic","dental","therapy","wellness","pharmacy"],
     "healthcare organization — clinical staff renewal"),
    (["nonprofit","foundation","shelter","community center"],
     "nonprofit or social services organization — community program safety"),
    (["aviation","aerospace","airline","airport"],
     "aviation organization — high-stakes emergency preparedness"),
    (["financial","finance","bank","insurance","investment","wealth","advisory"],
     "financial services firm — employee safety and compliance"),
    (["law","legal","attorney"],
     "legal firm — employee safety readiness"),
]

# Role/generic address prefixes to skip — not a real person
ROLE_ADDRESS_PREFIXES = {
    "center", "info", "admin", "contact", "office", "main", "general",
    "licensing", "reporting", "mainoffice", "director", "noreply", "no-reply",
    "support", "help", "sales", "billing", "hr", "careers", "jobs",
    "ccc", "hello", "team", "staff",
}

# Placeholder/test addresses to always skip
PLACEHOLDER_ADDRESSES = {
    "email@yourbusiness.com", "test@test.com", "example@example.com",
    "user@example.com", "name@domain.com",
}

# API batch size for email generation (kept modest so each call finishes well
# under the HTTP timeout — 50 was timing out on larger completions)
GENERATION_BATCH_SIZE = 20

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("outreach")

# ─── State ────────────────────────────────────────────────────────────────────

LOCK_FILE = Path(__file__).parent / "outreach_agent.lock"

def _acquire_lock():
    """
    Acquire an exclusive file lock so only one instance of the agent
    can run at a time. Returns the lock file handle (keep it open).
    Exits immediately if another process holds the lock.
    """
    lf = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log.error(
            "Another instance of outreach_agent.py is already running "
            f"(lock held: {LOCK_FILE}). Exiting to prevent duplicate sends."
        )
        sys.exit(1)
    lf.write(str(os.getpid()))
    lf.flush()
    return lf

def _normalize_state(raw: dict) -> dict:
    """
    Migrate legacy key names and normalize all email lists to lowercase.
    Handles the ghost keys 'contacted' and 'daily_counts' that were
    written by older code versions.
    """
    # Canonical schema
    state = {
        "contacted_emails": [],
        "sent_thread_ids": [],
        "forwarded_thread_ids": [],
        "manae_roster_pending": [],
        "last_roster_send": None,
        "daily_sent_count": {},
        "daily_reply_count": {},
        "do_not_contact": [],
        "bounce_replacement_queue": [],
        "sheet_cursor": 2,
    }
    state.update(raw)

    # Merge legacy 'contacted' key into 'contacted_emails'
    legacy_contacted = raw.get("contacted", [])
    if legacy_contacted:
        merged = set(e.lower() for e in state["contacted_emails"])
        merged.update(e.lower() for e in legacy_contacted)
        state["contacted_emails"] = sorted(merged)
        log.info(f"  Migrated {len(legacy_contacted)} entries from legacy 'contacted' key")

    # Merge legacy 'daily_counts' key into 'daily_sent_count'
    legacy_daily = raw.get("daily_counts", {})
    if legacy_daily:
        for k, v in legacy_daily.items():
            if k not in state["daily_sent_count"]:
                state["daily_sent_count"][k] = v

    # Remove ghost keys
    state.pop("contacted", None)
    state.pop("daily_counts", None)

    # Normalize all email lists to lowercase
    state["contacted_emails"] = sorted({e.lower() for e in state["contacted_emails"] if e})
    state["do_not_contact"]   = sorted({e.lower() for e in state["do_not_contact"] if e})

    return state

def load_state():
    if STATE_FILE.exists():
        try:
            raw = json.loads(STATE_FILE.read_text())
            return _normalize_state(raw)
        except Exception as e:
            log.warning(f"Could not parse state.json: {e} — starting fresh")
    return {
        "contacted_emails": [],
        "sent_thread_ids": [],
        "forwarded_thread_ids": [],
        "manae_roster_pending": [],
        "last_roster_send": None,
        "daily_sent_count": {},
        "daily_reply_count": {},
        "do_not_contact": [],
        "bounce_replacement_queue": [],
        "sheet_cursor": 2,
    }

def save_state(state):
    # The Google Sheet is the source of truth for who has been contacted / suppressed
    # (the `contacted` and `do_not_contact` columns). We deliberately do NOT persist
    # those email lists to state.json, so the file — and the public repo — never
    # contains contact PII. Within a single run the in-memory sets still dedupe;
    # across runs the sheet is authoritative.
    state["contacted_emails"] = []
    state["do_not_contact"]   = []
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(STATE_FILE)

# ─── Name / email hygiene ─────────────────────────────────────────────────────

def normalize_name(name: str) -> str:
    """Title-case all-caps names; leave mixed-case alone."""
    if not name:
        return name
    if name == name.upper() and len(name) > 1:
        return name.title()
    return name

def is_role_address(email: str) -> bool:
    """Return True if the local part looks like a generic/role address."""
    local = email.split("@")[0].lower()
    # strip digits from end (e.g. center578 → center)
    base = re.sub(r"\d+$", "", local)
    return base in ROLE_ADDRESS_PREFIXES

# ─── Helpers ──────────────────────────────────────────────────────────────────

def is_weekday():
    return date.today().weekday() < 5

def is_monday():
    return date.today().weekday() == 0

def today_str():
    return date.today().isoformat()

def infer_industry(company="", tags=None):
    text = (company + " " + " ".join(tags or [])).lower()
    for keywords, context in INDUSTRY_MAP:
        if any(k in text for k in keywords):
            return context
    return "organization — general workforce CPR and First Aid readiness"

def ssl_ctx():
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        return ssl.create_default_context()

def http_get(url, headers):
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=30, context=ssl_ctx()) as r:
        return json.loads(r.read().decode())

def http_post(url, headers, payload, timeout=120):
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout, context=ssl_ctx()) as r:
        return json.loads(r.read().decode())

def http_post_raw(url, headers, payload):
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=120, context=ssl_ctx()) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())

# ─── Contact source: the Google Sheet ────────────────────────────────────────

def _sheet_service():
    return sheets_db.service()

def fetch_sheet_contacts(svc, state, needed, extra_skip=None):
    """
    Pull up to `needed` eligible contacts from the sheet, scanning forward from
    the stored cursor. Skips rows already contacted (per the sheet's `contacted`
    column OR our historical state), suppressed rows, and role/placeholder
    addresses. Returns (contacts, new_cursor, skipped_role_addresses).
    Each contact carries its sheet `row` so results can be written back.
    """
    skip      = set(e.lower() for e in (extra_skip or []))
    contacted = {e.lower() for e in state.get("contacted_emails", [])}
    dnc       = {e.lower() for e in state.get("do_not_contact", [])}
    cursor    = int(state.get("sheet_cursor", 2) or 2)
    if cursor < 2:
        cursor = 2

    contacts = []
    skipped_roles = []
    WINDOW = 500
    empty_windows = 0

    while len(contacts) < needed and empty_windows < 3:
        rows, last = sheets_db.read_window(svc, SPREADSHEET_ID, cursor, WINDOW)
        if last < cursor:            # scanned past the end of the sheet
            cursor = last + 1
            break
        if not rows:
            empty_windows += 1
        else:
            empty_windows = 0

        broke = False
        for c in rows:
            cursor = c["row"] + 1    # we've now examined through this row
            if not sheets_db.is_eligible(c):
                continue
            email_l = c["email"].lower()
            if email_l in contacted or email_l in dnc or email_l in skip:
                continue
            if c["email"] in PLACEHOLDER_ADDRESSES:
                continue
            contacts.append({
                "row": c["row"],
                "email": c["email"],
                "firstName": normalize_name(c["first_name"]),
                "lastName": normalize_name(c["last_name"]),
                "firstNameRaw": c["first_name"],
                "lastNameRaw": c["last_name"],
                "company": c["company"],
                "tags": c["tags"],
                "sourceList": c["source_list"],
                "is_role": is_role_address(c["email"]),  # shared inbox → connector email
            })
            skip.add(email_l)
            if len(contacts) >= needed:
                broke = True
                break
        if not broke:
            cursor = max(cursor, last + 1)

    role_ct = sum(1 for c in contacts if c.get("is_role"))
    log.info(
        f"  Sheet scan: {len(contacts)} eligible pulled "
        f"({role_ct} shared/role inboxes → connector email), cursor now {cursor}"
    )
    return contacts, cursor, skipped_roles

# ─── HubSpot ──────────────────────────────────────────────────────────────────

def check_hubspot(contact, retries=3):
    """
    Look up a contact in HubSpot. Returns isCustomer + deal info on success, or
    {"error": True} if the lookup can't be completed after `retries` attempts —
    the caller then SKIPS the contact (never emails on an unresolved check).
    A clean 200 with no match is a genuine prospect (error False).
    """
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }
    search_url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    payload = {
        "filterGroups": [{"filters": [{
            "propertyName": "email",
            "operator": "EQ",
            "value": contact["email"],
        }]}],
        "properties": ["email", "firstname", "lastname", "company", "lifecyclestage", "hs_lead_status"],
        "limit": 1,
    }
    last_err = None
    for attempt in range(retries):
        try:
            status, data = http_post_raw(search_url, headers, payload)
            if status != 200:
                last_err = f"HTTP {status}"
                time.sleep(2 * (attempt + 1))
                continue
            if not data.get("results"):
                return {"found": False, "isCustomer": False, "error": False}

            hs_contact = data["results"][0]
            props = hs_contact.get("properties", {})
            contact_id = hs_contact["id"]
            lifecycle = props.get("lifecyclestage", "")
            is_customer = lifecycle in ("customer", "evangelist")

            deals_url = (
                f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}/associations/deals"
            )
            try:
                deal_assoc = http_get(deals_url, {"Authorization": f"Bearer {HUBSPOT_TOKEN}"})
                deal_ids = [r["id"] for r in deal_assoc.get("results", [])]
            except Exception:
                deal_ids = []

            last_deal_name = None
            last_deal_value = 0
            last_deal_date = None
            for deal_id in deal_ids[:5]:
                deal_url = (
                    f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}"
                    f"?properties=dealname,dealstage,amount,closedate"
                )
                try:
                    deal_data = http_get(deal_url, {"Authorization": f"Bearer {HUBSPOT_TOKEN}"})
                    dp = deal_data.get("properties", {})
                    if dp.get("dealstage") == "closedwon":
                        is_customer = True
                        last_deal_name  = dp.get("dealname")
                        last_deal_value = float(dp.get("amount") or 0)
                        last_deal_date  = (dp.get("closedate") or "")[:10]
                        break
                except Exception:
                    continue

            return {
                "found": True,
                "isCustomer": is_customer,
                "name": f"{props.get('firstname','')} {props.get('lastname','')}".strip(),
                "company": props.get("company", ""),
                "lastDealName": last_deal_name,
                "lastDealValue": last_deal_value,
                "lastDealDate": last_deal_date,
                "lifecycleStage": lifecycle,
                "error": False,
            }
        except Exception as e:
            last_err = str(e)
            time.sleep(2 * (attempt + 1))

    # Exhausted retries — do NOT assume prospect. Signal error so the caller skips.
    log.warning(f"  HubSpot lookup unresolved for {contact['email']} after {retries} tries: "
                f"{last_err} — skipping (will retry next run)")
    return {"found": False, "isCustomer": False, "error": True}

# ─── Bounce recovery — web search for replacement contact ─────────────────────

def search_replacement_contact(bounced_email, state):
    """
    Given a bounced email, search the web for a replacement contact at the same org.
    Also checks HubSpot for any other contacts at that domain.
    Queues result into state['bounce_replacement_queue'] for the next batch.
    """
    domain = bounced_email.split("@")[-1].lower()
    # Skip personal email providers
    personal_domains = {
        "gmail.com","yahoo.com","hotmail.com","outlook.com","icloud.com",
        "aol.com","msn.com","ymail.com","me.com","mac.com","sbcglobal.net",
    }
    if domain in personal_domains:
        log.info(f"  Bounce recovery: personal email domain {domain} — skipping replacement search")
        return

    log.info(f"  Bounce recovery: searching for replacement at {domain}...")

    # 1. Check HubSpot for other contacts at same domain
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }
    search_url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    payload = {
        "filterGroups": [{"filters": [{
            "propertyName": "email",
            "operator": "CONTAINS_TOKEN",
            "value": f"*@{domain}",
        }]}],
        "properties": ["email", "firstname", "lastname", "jobtitle"],
        "limit": 5,
    }
    try:
        status, data = http_post_raw(search_url, headers, payload)
        if status == 200 and data.get("results"):
            for result in data["results"]:
                props = result.get("properties", {})
                alt_email = props.get("email", "")
                if alt_email and alt_email.lower() != bounced_email.lower():
                    queue_entry = {
                        "originalBounced": bounced_email,
                        "replacementEmail": alt_email,
                        "replacementName": f"{props.get('firstname','')} {props.get('lastname','')}".strip(),
                        "source": "hubspot",
                        "domain": domain,
                        "queuedAt": today_str(),
                    }
                    queue = state.get("bounce_replacement_queue", [])
                    # Don't add duplicates
                    if not any(q.get("replacementEmail") == alt_email for q in queue):
                        queue.append(queue_entry)
                        state["bounce_replacement_queue"] = queue
                        log.info(f"  → Replacement found in HubSpot: {alt_email}")
                    return
    except Exception as e:
        log.warning(f"  HubSpot domain search failed: {e}")

    # 2. Web search for org + CPR/safety contact
    try:
        search_query = f"site:{domain} OR \"{domain}\" CPR training safety director contact"
        search_url_api = (
            f"https://api.anthropic.com/v1/messages"
        )
        api_headers = {
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        }
        payload = {
            "model": GEN_MODEL,
            "max_tokens": 300,
            "tools": [{"type": "web_search_20250305", "name": "web_search"}],
            "system": (
                "You are a research agent. Search for a valid contact email at the given domain "
                "for someone who might handle CPR/First Aid training for their organization. "
                "Look for HR director, office manager, facilities manager, or training coordinator. "
                'Return ONLY JSON: {"found": true/false, "email": "...", "name": "...", "title": "..."} '
                "If no specific person found, return {\"found\": false}. "
                "Never guess or fabricate email addresses."
            ),
            "messages": [{"role": "user", "content": (
                f"Find a valid contact at domain: {domain}\n"
                f"This is for a CPR training company reaching out. "
                f"Search for their website and find an appropriate contact person."
            )}],
        }
        data = http_post(search_url_api, api_headers, payload)
        text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                text = block["text"]
                break
        if text:
            cleaned = text.strip()
            if cleaned.startswith("```"):
                cleaned = cleaned.split("```")[1]
                if cleaned.startswith("json"):
                    cleaned = cleaned[4:]
            result = json.loads(cleaned.strip())
            if result.get("found") and result.get("email"):
                queue_entry = {
                    "originalBounced": bounced_email,
                    "replacementEmail": result["email"],
                    "replacementName": result.get("name", ""),
                    "replacementTitle": result.get("title", ""),
                    "source": "web_search",
                    "domain": domain,
                    "queuedAt": today_str(),
                }
                queue = state.get("bounce_replacement_queue", [])
                if not any(q.get("replacementEmail") == result["email"] for q in queue):
                    queue.append(queue_entry)
                    state["bounce_replacement_queue"] = queue
                    log.info(f"  → Replacement found via web search: {result['email']} ({result.get('name','')})")
                return
    except Exception as e:
        log.warning(f"  Web search replacement failed for {domain}: {e}")

    log.info(f"  → No replacement found for {domain} — flagged for manual review")

# ─── Claude email generation (batch) ─────────────────────────────────────────

FALLBACK_SUBJECT = "Quick question about CPR training"

def fallback_body(contact):
    first = contact.get("firstName") or "there"
    company = contact.get("company") or "your team"
    return (
        f"Hi {first},\n\n"
        f"Wanted to check in — is {company} due for CPR/AED training this year? "
        f"We work with organizations across the country and can usually schedule within a few weeks.\n\n"
        f"Happy to answer any questions or send over details.\n\n"
        f"{SENDING_NAME} | {COMPANY_NAME}"
    )

def _fallback_result(contact):
    return {
        "subject": FALLBACK_SUBJECT,
        "body": fallback_body(contact),
        "personalization_notes": "fallback used",
    }

def generate_emails_batch(contacts):
    """
    Generate personalized emails for a list of contacts in a single Claude API call.
    Returns a list of dicts: [{subject, body, personalization_notes}, ...]
    in the same order as the input contacts.
    """
    if not contacts:
        return []

    cta_line = (
        f"- Close with a soft ask for a quick 15-minute call and include this scheduling "
        f"link verbatim: {MANAE_CALENDAR_LINK}"
        if MANAE_CALENDAR_LINK else
        "- Close with a soft ask for a quick 15-minute call (e.g. 'worth a quick chat?' — "
        "'reply and I'll send a couple of times')"
    )

    system = (
        f"You are {SENDING_NAME}, Business Development Associate at {COMPANY_NAME} — "
        "a national CPR and First Aid training company affiliated with the American Heart Association.\n\n"
        "APPROACH (from our SDR playbook): consultative and curious, never salesy. Open a "
        "conversation about a real problem the org may not have named yet — do not pitch, do "
        "not hard-close, and never mention price. Lead with a genuine question or a credible "
        "observation about teams like theirs. Prevention-first and confidence-building; never "
        "fear-based or liability-based.\n\n"
        "Pick ONE problem framing that best fits the org and use it naturally:\n"
        "  - Coordination burden: one person gets stuck finding training, scheduling, and "
        "tracking who's current.\n"
        "  - Readiness gap: certified on paper, but would the team freeze under real pressure?\n"
        "  - Annual scramble: compliance handled last-minute once a year by whoever has bandwidth.\n"
        "  - Customization: generic CPR training doesn't fit their specific environment and risks.\n\n"
        "SHARED / ROLE INBOXES: when a contact's addressType is 'shared/role inbox' "
        "(e.g. info@, office@, support@ — often monitored by a real person at smaller "
        "orgs), do NOT use a personal name or invent one. Open warmly and a little "
        "playfully, acknowledging it's a shared inbox (e.g. 'Hi there — whoever's keeping "
        "an eye on the inbox today'), introduce yourself briefly, and ask who the right "
        "person is to talk to about CPR / First Aid training so their team is ready to "
        "help save a life. Same rules: no price, no hard close; keep the 15-minute-call "
        "offer as a soft option. For these, clean_first_name must be empty.\n\n"
        "NAME SAFETY (important): the provided firstName may be ALL CAPS, miscapitalized, a "
        "surname mistakenly placed in the first-name field, or junk (a single letter, '&', "
        "blank). firstName and lastName may also be SWAPPED. Use the email address as the "
        "tiebreaker: if the email's local part clearly contains one of the two names, THAT is "
        "the real given name (e.g. email 'terri.carden@...' with names SCOTT / TERRI -> the "
        "person is Terri). Use a name in the greeting ONLY if it is clearly a real personal "
        "given name, fixing its capitalization (e.g. 'ILISE' -> 'Ilise'). If you cannot "
        "confidently determine the given name, open with 'Hi there,' and never guess.\n\n"
        "Rules for each email:\n"
        "- Subject: conversational, <=8 words, no clickbait\n"
        "- Body: 2-4 short sentences — open with the framing or a question, then the ask\n"
        "- Reference the org and its likely context naturally\n"
        f"{cta_line}\n"
        "- No exclamation points; no hard-close language ('ready to move forward?'); never mention price\n"
        "- No filler openers (do not start with 'I hope this finds you well' etc.)\n"
        f"- Sign off: {SENDING_NAME} | {COMPANY_NAME}\n\n"
        "You will receive a JSON array of contacts. "
        "Return ONLY a JSON array (no markdown, no preamble) with one object per contact "
        "in the SAME ORDER, each with keys: subject, body, personalization_notes, "
        "clean_first_name, clean_last_name.\n"
        "clean_first_name = the correctly-capitalized given name you used, or \"\" if you "
        "opened with 'Hi there'. clean_last_name = the correctly-capitalized surname if you "
        "can confidently identify one, else \"\".\n"
        "Example: [{\"subject\":\"...\",\"body\":\"...\",\"personalization_notes\":\"...\","
        "\"clean_first_name\":\"Ilise\",\"clean_last_name\":\"Faye\"}]"
    )

    contact_list = []
    for i, c in enumerate(contacts):
        industry = infer_industry(c.get("company", ""), c.get("tags", []))
        contact_list.append({
            "index": i,
            "email": c.get("email", ""),
            "addressType": "shared/role inbox" if c.get("is_role") else "individual",
            "firstName": c.get("firstNameRaw") or c.get("firstName") or "",
            "lastName": c.get("lastNameRaw") or c.get("lastName") or "",
            "company": c.get("company") or "your organization",
            "industry": industry,
            "tags": ", ".join(c.get("tags", [])) or "none",
            "sourceList": c.get("sourceList", "unknown"),
        })

    user_msg = (
        f"Generate reactivation emails for these {len(contacts)} contacts:\n\n"
        + json.dumps(contact_list, indent=2)
    )

    # Scale max_tokens with batch size — ~350 tokens per email is comfortable
    max_tok = min(16000, 400 * len(contacts) + 500)

    payload = {
        "model": GEN_MODEL,
        "max_tokens": max_tok,
        "system": system,
        "messages": [{"role": "user", "content": user_msg}],
    }
    headers = {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
    }

    try:
        data = http_post("https://api.anthropic.com/v1/messages", headers, payload, timeout=240)
        text = next((b["text"] for b in data.get("content", []) if b.get("type") == "text"), "")
        cleaned = text.strip()
        # Strip markdown fences if present
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            cleaned = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        results = json.loads(cleaned)
        if not isinstance(results, list):
            raise ValueError("Expected JSON array")
        # Pad with fallbacks if model returned fewer than expected
        while len(results) < len(contacts):
            results.append(_fallback_result(contacts[len(results)]))
        return results[:len(contacts)]
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        log.warning(f"  Batch generation error: HTTP {e.code} — {body} — using fallbacks for all {len(contacts)} contacts")
        return [_fallback_result(c) for c in contacts]
    except Exception as e:
        log.warning(f"  Batch generation error: {e} — using fallbacks for all {len(contacts)} contacts")
        return [_fallback_result(c) for c in contacts]


def generate_email(contact):
    """Single-contact wrapper around the batch function (kept for compatibility)."""
    results = generate_emails_batch([contact])
    r = results[0]
    return r.get("subject", FALLBACK_SUBJECT), r.get("body", fallback_body(contact)), r.get("personalization_notes", "")

# ─── Gmail SMTP ───────────────────────────────────────────────────────────────

def send_gmail_smtp(to, subject, body, cc=None):
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    gmail_user = os.environ.get("GMAIL_ADDRESS", "")
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD", "")

    if not gmail_user or not gmail_pass:
        log.error("  GMAIL_ADDRESS or GMAIL_APP_PASSWORD not set")
        return {"success": False, "error": "Gmail credentials not configured"}

    msg = MIMEMultipart()
    msg["From"]    = f"{SENDING_NAME} <{gmail_user}>"
    msg["To"]      = to
    msg["Subject"] = subject
    if cc:
        msg["Cc"] = cc

    full_body = body if SENDING_NAME in body else body + f"\n\n{SENDING_NAME} | {COMPANY_NAME}"
    msg.attach(MIMEText(full_body, "plain"))

    recipients = [to] + ([cc] if cc else [])
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ssl_ctx()) as server:
            server.login(gmail_user, gmail_pass)
            server.sendmail(gmail_user, recipients, msg.as_string())
        return {"success": True, "threadId": ""}
    except smtplib.SMTPRecipientsRefused as e:
        # Hard bounce / 550 — extract error details
        err_detail = str(e)
        return {"success": False, "error": err_detail, "hard_bounce": True}
    except Exception as e:
        err_str = str(e)
        is_hard = "550" in err_str or "5.7.1" in err_str or "does not exist" in err_str
        return {"success": False, "error": err_str, "hard_bounce": is_hard}

send_email = send_gmail_smtp

# ─── Error notification ───────────────────────────────────────────────────────

def notify_chris(error_msg, context=""):
    try:
        subject = f"[Outreach Agent ERROR] {today_str()}"
        body = (
            f"Hi Chris,\n\nThe Get CPR Done Batch Outreach Agent encountered an error.\n\n"
            f"Error: {error_msg}\n\nContext: {context or 'See log file.'}\n\n"
            f"Log: {LOG_FILE}\n\n—Outreach Agent (automated)"
        )
        result = send_email(CHRIS_EMAIL, subject, body)
        if result.get("success"):
            log.info("  → Error notification sent to Chris")
        else:
            log.error(f"  → Failed to notify Chris: {result.get('error')}")
    except Exception as e:
        log.error(f"  → Could not notify Chris: {e}")

# ─── End-of-day report ────────────────────────────────────────────────────────

def send_eod_report(sent_count, bounce_count, bounce_list, replacement_queue, skipped_role, dry_run=False, warmth_breakdown=None, state=None):
    """Email Manae + Chris a summary of today's outreach activity."""
    today = today_str()
    subject = f"[Outreach Report] {today} — {sent_count} sent"

    # Pull reply count from state
    reply_count = 0
    if state:
        reply_count = state.get("daily_reply_count", {}).get(today, 0)

    # Reply rate
    reply_rate = f"{reply_count/sent_count:.1%}" if sent_count > 0 else "—"
    bounce_rate = f"{bounce_count/sent_count:.1%}" if sent_count > 0 else "—"

    # Warmth breakdown
    warmth_lines = ""
    if warmth_breakdown:
        warmth_lines = (
            f"\n  Audience warmth breakdown:\n"
            f"    Hot  (opened <30 days ago):  {warmth_breakdown.get('hot', 0)}\n"
            f"    Warm (30–180 days):           {warmth_breakdown.get('warm', 0)}\n"
            f"    Cold (180+ days):             {warmth_breakdown.get('cold', 0)}\n"
        )

    # Observations — auto-generated based on the numbers
    observations = []
    if sent_count == 0:
        observations.append("No emails sent today — check the sheet source or daily cap.")
    if bounce_count > 0 and sent_count > 0 and bounce_count / sent_count > 0.05:
        observations.append(f"Bounce rate is elevated ({bounce_rate}) — consider list hygiene on the source segments.")
    if reply_count > 0:
        observations.append(f"{reply_count} repl{'y' if reply_count == 1 else 'ies'} received — forwarded to Manae for follow-up.")
    if warmth_breakdown:
        cold = warmth_breakdown.get('cold', 0)
        total_warm = warmth_breakdown.get('hot', 0) + warmth_breakdown.get('warm', 0)
        if cold > total_warm:
            observations.append("Majority of today's contacts are cold (180+ days since last open) — engagement may be lower than usual.")
    if replacement_queue:
        observations.append(f"{len(replacement_queue)} replacement contact(s) queued for bounced addresses.")
    if not observations:
        observations.append("No issues flagged. Normal run.")

    obs_lines = "\n".join(f"  • {o}" for o in observations)

    bounce_lines = ""
    if bounce_list:
        bounce_lines = "\n\nBounced addresses (added to do_not_contact):\n"
        bounce_lines += "\n".join(f"  • {b}" for b in bounce_list)

    replacement_lines = ""
    if replacement_queue:
        replacement_lines = "\n\nReplacement contacts queued for next batch:\n"
        for r in replacement_queue:
            src  = r.get("source", "unknown")
            name = r.get("replacementName", "")
            title = r.get("replacementTitle", "")
            replacement_lines += (
                f"  • {r.get('replacementEmail','')} ({name}{' — ' + title if title else ''}) "
                f"[replaces {r.get('originalBounced','')} via {src}]\n"
            )

    skipped_lines = ""
    if skipped_role:
        skipped_lines = f"\n\nSkipped {len(skipped_role)} role/generic addresses (not real people):\n"
        skipped_lines += "\n".join(f"  • {e}" for e in skipped_role[:10])
        if len(skipped_role) > 10:
            skipped_lines += f"\n  ... and {len(skipped_role) - 10} more"

    body = (
        f"Hi Manae and Chris,\n\n"
        f"Here's today's outreach summary for {today}:\n\n"
        f"  Emails sent:      {sent_count}\n"
        f"  Hard bounces:     {bounce_count} ({bounce_rate})\n"
        f"  Replies received: {reply_count} ({reply_rate} reply rate)\n"
        f"  Leads forwarded:  {reply_count}\n"
        f"{warmth_lines}"
        f"\nObservations & next steps:\n{obs_lines}"
        f"{bounce_lines}"
        f"{replacement_lines}"
        f"{skipped_lines}"
        f"\n\n—Outreach Agent (automated)"
    )

    if not dry_run:
        result = send_email(MANAE_EMAIL, subject, body, cc=CHRIS_EMAIL)
        if result.get("success"):
            log.info("  → EOD report sent to Manae + Chris")
        else:
            log.error(f"  → EOD report failed: {result.get('error')}")
    else:
        log.info(f"  [DRY RUN] Would send EOD report to Manae + Chris: {sent_count} sent, {bounce_count} bounced, {reply_count} replies")

# ─── SQL detection + HubSpot write ────────────────────────────────────────────

def parse_from(sender):
    """From a 'Name <email>' header return (display_name, email_lower)."""
    m = re.search(r"<([^>]+)>", sender)
    email = (m.group(1) if m else sender).strip().lower()
    name = re.sub(r"<[^>]+>", "", sender).strip().strip('"')
    if "@" in name:
        name = ""
    return name, email

def classify_interest(subject, body_text):
    """
    Decide whether an inbound reply shows interest in booking/scheduling/pricing
    CPR training (i.e. a sales-qualified lead). Returns (interested: bool, reason).
    On any error, returns (False, ...) — the Manae forward is the safety net, so
    we never manufacture a false SQL in HubSpot.
    """
    try:
        payload = {
            "model": GEN_MODEL,
            "max_tokens": 150,
            "system": (
                "You triage inbound replies to a CPR-training outreach email. "
                "Decide if the sender shows ANY interest in booking, scheduling, pricing, "
                "availability, or learning more (a sales-qualified lead). "
                'Return ONLY JSON: {"interested": true|false, "reason": "<=12 words"}. '
                "Questions about price/availability/scheduling count as interested. "
                "Pure rejections, 'no thanks', 'remove me', or unrelated do not."
            ),
            "messages": [{"role": "user", "content": f"Subject: {subject}\n\n{body_text[:1500]}"}],
        }
        headers = {"Content-Type": "application/json", "x-api-key": ANTHROPIC_API_KEY,
                   "anthropic-version": "2023-06-01"}
        data = http_post("https://api.anthropic.com/v1/messages", headers, payload)
        text = next((b["text"] for b in data.get("content", []) if b.get("type") == "text"), "")
        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("```")[1]
            if cleaned.startswith("json"):
                cleaned = cleaned[4:]
        r = json.loads(cleaned.strip())
        return bool(r.get("interested")), r.get("reason", "")
    except Exception as e:
        log.warning(f"  Interest classification failed: {e} — not flagging as SQL (Manae still gets it)")
        return False, "classifier error"

def hubspot_upsert_sql(email, first="", last="", company=""):
    """Create or update a HubSpot contact as a Sales Qualified Lead. Needs write scope."""
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"}
    props = {"email": email, "lifecyclestage": "salesqualifiedlead", "hs_lead_status": "OPEN"}
    if first:   props["firstname"] = first
    if last:    props["lastname"]  = last
    if company: props["company"]   = company
    try:
        status, data = http_post_raw(
            "https://api.hubapi.com/crm/v3/objects/contacts/search", headers,
            {"filterGroups": [{"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}],
             "properties": ["email"], "limit": 1},
        )
        if status == 200 and data.get("results"):
            cid = data["results"][0]["id"]
            body = json.dumps({"properties": props}).encode("utf-8")
            req = urllib.request.Request(
                f"https://api.hubapi.com/crm/v3/objects/contacts/{cid}",
                data=body, headers=headers, method="PATCH")
            with urllib.request.urlopen(req, timeout=60, context=ssl_ctx()) as r:
                r.read()
            log.info(f"    HubSpot: updated {email} → SQL")
            return True
        st, _ = http_post_raw("https://api.hubapi.com/crm/v3/objects/contacts", headers,
                              {"properties": props})
        ok = st in (200, 201)
        log.info(f"    HubSpot: {'created' if ok else f'create FAILED ({st})'} {email} → SQL")
        return ok
    except Exception as e:
        log.warning(f"    HubSpot SQL upsert failed for {email}: {e}")
        return False

# ─── Reply check ──────────────────────────────────────────────────────────────

def classify_reply(sender, subject, body_text):
    """Classify an inbound email. Returns: 'bounce', 'ooo', 'unsubscribe', or 'genuine'."""
    sender_l  = sender.lower()
    subject_l = subject.lower()
    body_l    = body_text.lower()

    # Hard bounce / delivery failure
    if any(k in sender_l for k in ["mailer-daemon", "postmaster", "mail delivery"]):
        return "bounce"
    if any(k in subject_l for k in [
        "undeliverable", "delivery failed", "delivery status notification",
        "mail delivery failed", "returned mail", "delivery failure",
        "failed to deliver", "unable to deliver",
    ]):
        return "bounce"

    # Out of office / auto-reply
    if any(k in subject_l for k in [
        "out of office", "auto-reply", "automatic reply", "away from",
        "on vacation", "ooo:", "i am out", "i\'m out", "i will be out",
        "currently out", "annual leave", "maternity leave", "on leave",
    ]):
        return "ooo"
    # Some OOOs don't put it in subject — check body first line
    first_lines = body_l[:300]
    if any(k in first_lines for k in [
        "i am currently out", "i\'m currently out", "i am away", "i\'m away",
        "i am on vacation", "i\'m on vacation", "i will be out of the office",
        "i\'m out of the office", "this is an automatic reply",
        "this is an automated response",
    ]):
        return "ooo"

    # Unsubscribe / opt-out
    if any(k in subject_l for k in [
        "unsubscribe", "remove me", "opt out", "opt-out",
        "stop emailing", "please remove", "take me off",
    ]):
        return "unsubscribe"

    return "genuine"


def archive_message(mail, mid):
    """Mark as read and archive (remove from INBOX) without deleting."""
    mail.store(mid, "+FLAGS", "\\Seen")
    # Gmail archiving = remove the \\Inbox label
    mail.store(mid, "-X-GM-LABELS", "\\Inbox")


def check_replies(state, dry_run=False):
    import imaplib
    import email as emaillib

    gmail_user = os.environ.get("GMAIL_ADDRESS", "")
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD", "")

    if not gmail_user or not gmail_pass:
        log.info("Gmail credentials not set — skipping reply check")
        return

    log.info("Checking Gmail inbox for replies / bounces / OOOs...")
    do_not_contact = set(state.get("do_not_contact", []))

    # Best-effort sheet connection so we can write reply outcomes back to rows.
    svc = None
    email_index = {}
    try:
        svc = _sheet_service()
        email_index = sheets_db.build_email_index(svc, SPREADSHEET_ID)
    except Exception as e:
        log.warning(f"  Sheet unavailable for reply write-back: {e} (continuing without it)")

    def mark_row(addr, **fields):
        """Write status back to the sheet row for this email address, if we can find it."""
        if dry_run or not svc:
            return
        r = email_index.get((addr or "").lower())
        if not r:
            return
        try:
            sheets_db.update_row(svc, SPREADSHEET_ID, r, **fields)
        except Exception as e:
            log.warning(f"    Could not write row {r} for {addr}: {e}")

    try:
        ctx  = ssl_ctx()
        mail = imaplib.IMAP4_SSL("imap.gmail.com", ssl_context=ctx)
        mail.login(gmail_user, gmail_pass)
        mail.select("INBOX")

        # Search ALL unread — SMTP sends don't give us real Gmail thread IDs.
        _, msg_ids = mail.search(None, "UNSEEN")
        all_mids = msg_ids[0].split() if msg_ids[0] else []
        log.info(f"  {len(all_mids)} unread messages in inbox")

        genuine_count = sql_count = archived_count = unsubscribe_count = 0
        today = today_str()

        for mid in all_mids:
            try:
                _, msg_data = mail.fetch(mid, "(RFC822)")
                msg = emaillib.message_from_bytes(msg_data[0][1])

                sender  = msg.get("From", "Unknown")
                subject = msg.get("Subject", "")
                body_text = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            body_text = part.get_payload(decode=True).decode(errors="replace")
                            break
                else:
                    body_text = msg.get_payload(decode=True).decode(errors="replace")

                sender_name, sender_email = parse_from(sender)
                kind = classify_reply(sender, subject, body_text)

                if kind == "bounce":
                    log.info(f"  Bounce from {sender} — archiving silently")
                    email_match = re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", body_text)
                    if email_match:
                        bounced = email_match.group(0).lower()
                        do_not_contact.add(bounced)
                        mark_row(bounced, reply_status="Bounced", do_not_contact="yes",
                                 last_result="hard bounce (inbox)")
                    if not dry_run:
                        archive_message(mail, mid)
                    archived_count += 1

                elif kind == "ooo":
                    log.info(f"  OOO from {sender} — archiving silently")
                    if not dry_run:
                        archive_message(mail, mid)
                    archived_count += 1

                elif kind == "unsubscribe":
                    log.info(f"  Unsubscribe from {sender} — archiving + blocking")
                    if sender_email:
                        do_not_contact.add(sender_email)
                        mark_row(sender_email, reply_status="Unsubscribed",
                                 do_not_contact="yes", last_result="unsubscribe request")
                    if not dry_run:
                        archive_message(mail, mid)
                    unsubscribe_count += 1
                    archived_count += 1

                elif kind == "genuine":
                    interested, reason = classify_interest(subject, body_text)
                    first = (sender_name.split()[0] if sender_name else "")
                    last  = (sender_name.split()[-1] if len(sender_name.split()) > 1 else "")

                    if interested:
                        log.info(f"  SQL reply from {sender} ({reason}) → HubSpot + Manae")
                        if not dry_run:
                            hubspot_upsert_sql(sender_email, first, last)
                        mark_row(sender_email, reply_status="SQL",
                                 last_result=f"SQL: {reason}"[:250])
                        tag = "SQL — booking interest"
                        sql_count += 1
                    else:
                        log.info(f"  Genuine reply from {sender} (not SQL) → Manae")
                        mark_row(sender_email, reply_status="Replied",
                                 last_result="replied (not SQL)")
                        tag = "reply"

                    fwd_subject = f"[CPR Lead {tag}] {sender}"
                    fwd_body = (
                        f"Hi Manae,\n\nNew reply to Vida's outreach email"
                        f"{' — flagged as a Sales-Qualified Lead and created in HubSpot' if interested else ''}.\n\n"
                        f"From: {sender}\nSubject: {subject}\n"
                        f"Vida's read: {'INTERESTED — ' + reason if interested else 'genuine reply, no clear booking intent'}\n"
                        f"---\n{body_text[:800]}\n---\n\n—Outreach Agent (automated)"
                    )
                    if not dry_run:
                        send_email(MANAE_EMAIL, fwd_subject, fwd_body, cc=CHRIS_EMAIL)
                        mail.store(mid, "+FLAGS", "\\Seen")
                    genuine_count += 1

            except Exception as e:
                log.warning(f"  Error processing message {mid}: {e}")
                continue

        mail.logout()

        state["do_not_contact"] = list(do_not_contact)
        daily_replies = state.get("daily_reply_count", {})
        daily_replies[today] = daily_replies.get(today, 0) + genuine_count
        state["daily_reply_count"] = daily_replies
        save_state(state)

        log.info(
            f"  Reply check done: {genuine_count} genuine ({sql_count} SQL → HubSpot), "
            f"{archived_count} archived (incl. {unsubscribe_count} unsubs)"
        )

    except Exception as e:
        log.error(f"Reply check error: {e}")
        notify_chris(f"Reply check failed: {e}")

# ─── Manae roster ─────────────────────────────────────────────────────────────

def send_manae_roster(state, dry_run=False):
    roster = state.get("manae_roster_pending", [])
    if not roster:
        log.info("Manae roster: nothing accumulated. Skipping.")
        return

    if state.get("last_roster_send") == today_str():
        log.info("Manae roster already sent today. Skipping.")
        return

    log.info(f"Sending Manae roster ({len(roster)} customers)...")
    rows = []
    for i, entry in enumerate(roster, 1):
        c  = entry.get("contact", {})
        hs = entry.get("hubspot", {})
        name    = f"{c.get('firstName','')} {c.get('lastName','')}".strip() or c.get("email","")
        company = c.get("company","")
        email   = c.get("email","")
        deal    = hs.get("lastDealName") or "N/A"
        value   = f"${hs.get('lastDealValue',0):,.0f}" if hs.get("lastDealValue") else ""
        ddate   = hs.get("lastDealDate","")
        rows.append(
            f"{i}. {name}{' — ' + company if company else ''} ({email})\n"
            f"   Last deal: {deal}{' · ' + value if value else ''}{' · ' + ddate if ddate else ''}"
        )

    week_end = (date.today() - timedelta(days=1)).strftime("%b %d")
    subject = f"[Weekly Roster] {len(roster)} existing customers to follow up — week ending {week_end}"
    body = (
        f"Hi Manae,\n\n"
        f"Here are {len(roster)} existing Get CPR Done customers surfaced from this week's "
        f"outreach list. They were filtered out of the cold batch because they already have a "
        f"relationship with us — these are yours to follow up directly.\n\n"
        + "\n\n".join(rows) +
        f"\n\n—Vida Monroe | {COMPANY_NAME} (via automated Outreach Agent)"
    )

    if not dry_run:
        result = send_email(MANAE_EMAIL, subject, body)
        if result.get("success"):
            state["last_roster_send"] = today_str()
            state["manae_roster_pending"] = []
            save_state(state)
            log.info(f"  → Roster sent to Manae ({len(roster)} customers)")
        else:
            err = result.get("error","unknown")
            log.error(f"  → Roster send failed: {err}")
            notify_chris(f"Manae roster send failed: {err}")
    else:
        log.info(f"  [DRY RUN] Would send {len(roster)}-contact roster to Manae")
        state["last_roster_send"] = today_str()
        state["manae_roster_pending"] = []
        save_state(state)

# ─── Main daily run ───────────────────────────────────────────────────────────

def run_daily(dry_run=False, limit=None):
    state    = load_state()
    today    = today_str()
    daily_counts = state.get("daily_sent_count", {})
    already_sent = daily_counts.get(today, 0)

    cap = BATCH_SIZE if limit is None else min(BATCH_SIZE, limit)
    if already_sent >= cap:
        log.info(f"Daily cap reached ({already_sent}/{cap}). Exiting.")
        return

    remaining      = cap - already_sent
    contacted      = {e.lower() for e in state.get("contacted_emails", [])}
    do_not_contact = {e.lower() for e in state.get("do_not_contact", [])}

    today_sent    = already_sent
    sent_this_run = 0
    today_bounces = []

    try:
        svc = _sheet_service()
        sheets_db.ensure_headers(svc, SPREADSHEET_ID)

        # 1 — Source eligible contacts from the sheet (cursor-based scan)
        candidates, new_cursor, skipped_roles = fetch_sheet_contacts(svc, state, remaining)
        if not candidates:
            log.info("No eligible contacts left at the current sheet cursor.")
            if not dry_run:
                state["sheet_cursor"] = new_cursor
                save_state(state)
            if already_sent > 0:
                send_eod_report(already_sent, 0, [], [], skipped_roles, dry_run, state=state)
            return

        # 2 — HubSpot cross-check: peel existing customers off to Manae.
        # On an unresolved HubSpot lookup (API error after retries) SKIP the contact
        # rather than risk emailing a customer — it's retried on the next run.
        prospects = []
        customers = []
        skipped_unresolved = []
        for c in candidates:
            hs = check_hubspot(c)
            if hs.get("error"):
                skipped_unresolved.append(c["row"])
                continue
            if hs.get("isCustomer"):
                customers.append({"contact": c, "hubspot": hs})
                if not dry_run:
                    sheets_db.update_row(
                        svc, SPREADSHEET_ID, c["row"],
                        contacted=today, date_sent=today, reply_status="Customer",
                        last_result="existing customer → Manae")
            else:
                prospects.append(c)
        if customers and not dry_run:
            state["manae_roster_pending"] = state.get("manae_roster_pending", []) + customers
            save_state(state)
        log.info(f"Routing: {len(prospects)} prospects, {len(customers)} customers → Manae, "
                 f"{len(skipped_unresolved)} unresolved (HubSpot) → retry next run")

        # 3 — Generate emails in batches
        to_send = prospects[:remaining]
        log.info(f"Generating {len(to_send)} emails in batches of {GENERATION_BATCH_SIZE}...")
        generated = []
        for start in range(0, len(to_send), GENERATION_BATCH_SIZE):
            batch = to_send[start:start + GENERATION_BATCH_SIZE]
            try:
                generated.extend(generate_emails_batch(batch))
            except Exception as e:
                log.warning(f"  Batch generation failed: {e} — using fallbacks")
                generated.extend([_fallback_result(c) for c in batch])

        # 4 — Send and write the result back to each row
        log.info(f"Sending {len(to_send)} emails...")
        for i, (contact, ed) in enumerate(zip(to_send, generated)):
            email       = contact.get("email", "")
            subject     = ed.get("subject", FALLBACK_SUBJECT)
            body        = ed.get("body", fallback_body(contact))
            clean_first = (ed.get("clean_first_name") or "").strip()
            clean_last  = (ed.get("clean_last_name") or "").strip()
            row         = contact["row"]

            name_updates = {}
            if clean_first and clean_first != contact.get("firstNameRaw", ""):
                name_updates["first"] = clean_first
            if clean_last and clean_last != contact.get("lastNameRaw", ""):
                name_updates["last"] = clean_last

            log.info(f"Sending {i+1}/{len(to_send)} (row {row}): {email} | {subject}")

            if dry_run:
                log.info(f"  [DRY RUN] Would send. clean_first={clean_first!r} clean_last={clean_last!r}")
                continue

            result = send_email(email, subject, body)
            if result.get("success"):
                contacted.add(email.lower())
                today_sent    += 1
                sent_this_run += 1
                sheets_db.update_row(
                    svc, SPREADSHEET_ID, row,
                    contacted=today, date_sent=today, reply_status="Contacted",
                    touches=1, last_result="sent", **name_updates)
                log.info(f"  ✓ Sent ({today_sent}/{cap} today)")
            elif result.get("hard_bounce"):
                err = str(result.get("error", "unknown"))
                log.warning(f"  ✗ Hard bounce for {email}: {err}")
                do_not_contact.add(email.lower())
                contacted.add(email.lower())
                today_bounces.append(email)
                sheets_db.update_row(
                    svc, SPREADSHEET_ID, row,
                    contacted=today, date_sent=today, reply_status="Bounced",
                    do_not_contact="yes", touches=1,
                    last_result=f"hard bounce: {err}"[:250], **name_updates)
            else:
                err = result.get("error", "unknown")
                log.error(f"  ✗ Failed: {err}")
                notify_chris(f"Send failed for {email}", err)

            state["contacted_emails"] = list(contacted)
            state["do_not_contact"]   = list(do_not_contact)
            daily_counts[today]       = today_sent
            state["daily_sent_count"] = daily_counts
            save_state(state)

            if i < len(to_send) - 1:
                time.sleep(random.randint(MIN_DELAY_SEC, MAX_DELAY_SEC))

        # Advance the cursor past everything we resolved this run, but hold it at the
        # first HubSpot-unresolved row so those get retried on the next run.
        if not dry_run:
            resume_at = new_cursor
            if skipped_unresolved:
                resume_at = min(new_cursor, min(skipped_unresolved))
                log.info(f"  Cursor held at row {resume_at} to retry "
                         f"{len(skipped_unresolved)} unresolved contact(s) next run")
            state["sheet_cursor"] = resume_at
            save_state(state)

        log.info(f"Daily run complete. {sent_this_run} sent this run ({today_sent} total today).")
        send_eod_report(
            sent_count=today_sent, bounce_count=len(today_bounces),
            bounce_list=today_bounces, replacement_queue=[],
            skipped_role=skipped_roles, dry_run=dry_run, state=state,
        )

    except Exception as e:
        log.exception(f"Fatal error: {e}")
        notify_chris(str(e), f"Daily run failed on {today}")
        sys.exit(1)

# ─── One-time inbox reconciliation ────────────────────────────────────────────

def _email_date(date_header):
    """Parse an email Date header to an ISO date string, or today's date."""
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(date_header)
        return dt.date().isoformat() if dt else today_str()
    except Exception:
        return today_str()

# reply_status values ranked so reconcile never downgrades a row
_STATUS_RANK = {"": 0, "Contacted": 1, "Bounced": 2, "Unsubscribed": 2,
                "Replied": 3, "SQL": 4, "Customer": 5}

def run_reconcile(dry_run=False):
    """
    ONE-TIME backfill: read vida@'s Sent + Inbox and stamp the sheet so we never
    re-contact anyone we've already reached. Sheet-only — does NOT create HubSpot
    records or forward anything to Manae (these are historical). Genuine replies
    are marked 'Replied' (no per-message SQL triage on the backlog).
    """
    import imaplib
    import email as emaillib

    gmail_user = os.environ.get("GMAIL_ADDRESS", "")
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not gmail_user or not gmail_pass:
        log.error("Gmail credentials not set — cannot reconcile")
        sys.exit(1)

    svc = _sheet_service()
    sheets_db.ensure_headers(svc, SPREADSHEET_ID)
    log.info("Reconcile: snapshotting sheet status...")
    snap = sheets_db.snapshot_status(svc, SPREADSHEET_ID)
    log.info(f"  {len(snap)} rows indexed by email")

    staged = {}  # email_lower -> fields dict (merged)

    def stage(email, status=None, dnc=False, sent_date=None, note=""):
        email = (email or "").lower()
        row = snap.get(email)
        if not row:
            return
        fields = staged.setdefault(email, {})
        if sent_date and not row["contacted"] and "contacted" not in fields:
            fields["contacted"] = sent_date
            fields["date_sent"] = sent_date
        if status:
            cur = fields.get("reply_status", row["reply_status"])
            if _STATUS_RANK.get(status, 0) > _STATUS_RANK.get(cur, 0):
                fields["reply_status"] = status
        if dnc:
            fields["do_not_contact"] = "yes"
        if note:
            fields["last_result"] = note

    mail = imaplib.IMAP4_SSL("imap.gmail.com", ssl_context=ssl_ctx())
    mail.login(gmail_user, gmail_pass)

    # 1 — Sent Mail → everyone we emailed is Contacted
    for folder in ('"[Gmail]/Sent Mail"', "Sent", '"[Gmail]/All Mail"'):
        typ, _ = mail.select(folder, readonly=True)
        if typ == "OK":
            log.info(f"Reconcile: scanning sent folder {folder}")
            break
    _, ids = mail.search(None, "ALL")
    sent_ids = ids[0].split() if ids and ids[0] else []
    log.info(f"  {len(sent_ids)} sent messages")
    for i, mid in enumerate(sent_ids, 1):
        try:
            _, d = mail.fetch(mid, "(BODY.PEEK[HEADER.FIELDS (TO DATE)])")
            hdr = emaillib.message_from_bytes(d[0][1])
            day = _email_date(hdr.get("Date", ""))
            for raw in (hdr.get("To", "") or "").split(","):
                _, addr = parse_from(raw)
                if addr:
                    stage(addr, status="Contacted", sent_date=day, note="reconciled: sent")
        except Exception as e:
            log.warning(f"  sent[{i}] parse error: {e}")
        if i % 500 == 0:
            log.info(f"  sent {i}/{len(sent_ids)} scanned")

    # 2 — Inbox → replies / bounces / unsubscribes
    mail.select("INBOX", readonly=True)
    _, ids = mail.search(None, "ALL")
    in_ids = ids[0].split() if ids and ids[0] else []
    log.info(f"Reconcile: {len(in_ids)} inbox messages")
    for i, mid in enumerate(in_ids, 1):
        try:
            _, d = mail.fetch(mid, "(BODY.PEEK[HEADER.FIELDS (FROM SUBJECT DATE)])")
            hdr = emaillib.message_from_bytes(d[0][1])
            sender = hdr.get("From", ""); subject = hdr.get("Subject", "")
            _, sender_email = parse_from(sender)
            kind = classify_reply(sender, subject, "")
            if kind == "bounce":
                # best-effort: the bounced address is usually the sender's org; skip if unknown
                if sender_email:
                    stage(sender_email, status="Bounced", dnc=True, note="reconciled: bounce")
            elif kind == "unsubscribe":
                stage(sender_email, status="Unsubscribed", dnc=True, note="reconciled: unsubscribe")
            elif kind == "genuine":
                stage(sender_email, status="Replied", note="reconciled: replied")
            # ooo -> ignore
        except Exception as e:
            log.warning(f"  inbox[{i}] parse error: {e}")
        if i % 500 == 0:
            log.info(f"  inbox {i}/{len(in_ids)} scanned")

    mail.logout()

    updates = [(snap[e]["row"], f) for e, f in staged.items() if f]
    from collections import Counter
    breakdown = Counter(f.get("reply_status", "(contacted-only)") for _, f in updates)
    dnc_ct = sum(1 for _, f in updates if f.get("do_not_contact") == "yes")
    log.info(f"Reconcile: {len(updates)} rows to update | by status: {dict(breakdown)} "
             f"| suppressed (do_not_contact): {dnc_ct}")
    if dry_run:
        sample = updates[:15]
        for row, f in sample:
            log.info(f"  [DRY RUN] row {row}: {f}")
        log.info(f"[DRY RUN] would update {len(updates)} rows (showing {len(sample)})")
        return
    cells = sheets_db.batch_update(svc, SPREADSHEET_ID, updates)
    log.info(f"Reconcile complete: {len(updates)} rows, {cells} cells written")

# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["daily","roster","reply_check","reconcile"], required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force-weekday", action="store_true")
    parser.add_argument("--force", action="store_true")  # alias
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap sends this run (for a small first live test). Also bounded by BATCH_SIZE.")
    args = parser.parse_args()

    # Acquire exclusive lock — exits immediately if another instance is running
    _lock_fh = _acquire_lock()

    if not args.force_weekday and not args.force and not is_weekday():
        log.info(f"Today is {date.today().strftime('%A')} — agent only runs M–F. Exiting.")
        sys.exit(0)

    if args.dry_run:
        log.info("=== DRY RUN MODE — no emails will be sent ===")

    log.info(f"=== Outreach Agent | mode={args.mode} | {datetime.now().isoformat()} ===")

    state = load_state()

    if args.mode == "daily":
        run_daily(dry_run=args.dry_run, limit=args.limit)
    elif args.mode == "roster":
        if not is_monday() and not args.force_weekday and not args.force:
            log.info("Roster mode only runs Mondays. Exiting.")
            sys.exit(0)
        send_manae_roster(state, dry_run=args.dry_run)
    elif args.mode == "reply_check":
        check_replies(state, dry_run=args.dry_run)
    elif args.mode == "reconcile":
        run_reconcile(dry_run=args.dry_run)

    log.info(f"=== Complete | {datetime.now().isoformat()} ===\n")

if __name__ == "__main__":
    main()
