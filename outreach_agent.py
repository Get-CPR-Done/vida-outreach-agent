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
from html import escape as html_escape, unescape as html_unescape
from pathlib import Path

import sheets_db

# ─── Config ───────────────────────────────────────────────────────────────────

VIDA_EMAIL    = "vida@getcprdone.com"
MANAE_EMAIL   = "Manae@GetCPRDone.com"
CHRIS_EMAIL   = "chris@getcprdone.com"
REPORT_EMAIL  = "chris@joffeemergencyservices.com"   # lead dashboard → Chris's main inbox
SENDING_NAME  = "Vida Monroe"
COMPANY_NAME  = "Get CPR Done"
# Derived identity — keeps the code identity-agnostic so a second SDR (Elena Reyes) runs the
# exact same logic with only these config constants changed. SENDER_FIRST is used in short
# sign-offs; AGENT_KEY namespaces this agent's slice on the CEO command-center dashboard.
SENDER_FIRST  = SENDING_NAME.split()[0]
AGENT_KEY     = (os.environ.get("AGENT_KEY") or SENDER_FIRST).lower()

# Chris-confirmed, TRUE credibility line Vida may cite (especially the touch-3 value
# email). Deliberately non-specific — no invented schools, names, or exact numbers.
PROOF_POINT   = ("we've trained thousands of people across the country — teachers, school "
                 "and childcare staff, camp staff, and workplace teams")

# Daily send cap. 750/day is the target (working through the full list roughly
# quarterly). Runs unattended on GitHub Actions, so the send spacing is tightened
# to fit the 6-hour job limit (750 * ~15s avg ≈ 3.1h). Single mailbox — Google's
# hard cap is ~2,000/day; to scale past this, add mailboxes/subdomains, don't just
# raise the number. (Sustained 750/day requires the repo to be PUBLIC for free
# unlimited Actions minutes — see README.)
BATCH_SIZE    = 750
MIN_DELAY_SEC = 10
MAX_DELAY_SEC = 20

# Multi-touch email cadence (per SellingSara: outbound is 12-15 touches / building
# familiarity, not one-and-done). Email is Vida's only channel, so ~4 email touches
# over ~2 weeks. Gap in DAYS before the next touch, keyed by touches already sent:
# t1->t2 +3 (day 3), t2->t3 +4 (day 7), t3->t4 +7 (day 14). A reply/bounce/unsub
# ends the sequence (row is no longer reply_status == "Contacted").
FOLLOWUP_GAP_DAYS   = {1: 3, 2: 4, 3: 7}
MAX_TOUCHES         = 4
FOLLOWUP_STALE_DAYS = 30   # don't resurrect a contact whose last touch is older than this

STATE_FILE = Path(__file__).parent / "state.json"
LOG_FILE   = Path(__file__).parent / "outreach.log"

# Partition bounds. When two SDR identities share one contact sheet, each is confined to a
# disjoint row range so they can never email the same prospect (and each only follows up its
# own sends). Defaults (2 .. end-of-sheet) = unpartitioned, single-agent behavior. Set both
# via env per deployment: Vida = rows 2..62533, Elena = rows 62534..end.
ROW_RANGE_START = int(os.environ.get("ROW_RANGE_START", "2") or "2")
_row_range_end  = (os.environ.get("ROW_RANGE_END", "") or "").strip()
ROW_RANGE_END   = int(_row_range_end) if _row_range_end else None   # None → to end of sheet

# Agent launch date (YYYY-MM-DD). The shared sheet carries legacy Mailchimp statuses/dates
# that predate this agent; dashboard lifetime metrics count ONLY rows this agent actually
# contacted, i.e. date_sent >= this date. Vida went live 2026-06-12; Elena 2026-08-05.
AGENT_LAUNCH_DATE = (os.environ.get("AGENT_LAUNCH_DATE", "") or "2026-06-12").strip()

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
# NOTE: `or` (not a get-default) so an empty env var — which the workflow sets when
# the optional secret is absent — still falls back to the real hardcoded link.
MANAE_CALENDAR_LINK = os.environ.get("MANAE_CALENDAR_LINK") or \
    "https://meetings.hubspot.com/manae-deguchi?uuid=76b99631-517e-4aa3-baa3-537375a5db77"

# ─── Segment → course line ───────────────────────────────────────────────────
# The list is no longer one kind of buyer. A skilled nursing facility renews AHA BLS
# for Healthcare Providers on a two-year clock; a preschool needs pediatric CPR/First
# Aid to satisfy its licensor. Saying "CPR and First Aid class" to a hospital reads as
# someone who doesn't know the field, and "BLS provider card" means nothing to a camp.
# So each contact carries a canonical segment in its TAGS (written by the registry
# importer, see ~/registry-imports/build_import.py) and the generator is told the right
# course line and framing for that segment (Chris, 2026-08-13).
#
# Each entry: canonical segment -> (what they are, the course line to name, what to talk about)
SEGMENT_PLAYBOOK = {
    "hospital": (
        "acute-care hospital",
        "AHA BLS for Healthcare Providers, with ACLS and PALS for clinical staff who need them",
        "onboarding cohorts of new clinical hires, keeping per-diem and travel staff current, "
        "and running skills sessions on-site across shifts",
    ),
    "skilled-nursing": (
        "skilled nursing / long-term care facility",
        "AHA BLS for Healthcare Providers (two-year renewal cycle)",
        "nurses and aides whose cards all seem to expire in different months, staffing the "
        "floor while people train, and being survey-ready without a scramble",
    ),
    "home-health": (
        "home health agency",
        "AHA BLS for Healthcare Providers",
        "field clinicians who are rarely in the office at the same time, and onboarding "
        "new hires quickly so they can start seeing patients",
    ),
    "hospice": (
        "hospice provider",
        "AHA BLS for Healthcare Providers",
        "field staff spread across a wide service area and keeping renewals from piling up",
    ),
    "dialysis": (
        "dialysis clinic",
        "AHA BLS for Healthcare Providers",
        "keeping every clinical staff member current without closing chair time",
    ),
    "clinic": (
        "outpatient clinic",
        "AHA BLS for Healthcare Providers (and CPR/AED for non-clinical front-office staff)",
        "clinical staff renewals plus front-desk teams who are often first to reach someone "
        "in the waiting room",
    ),
    "adult-day": (
        "adult day health center",
        "CPR/AED and First Aid for staff, plus BLS for any licensed clinical staff",
        "participants with complex medical needs and staff who need to respond confidently",
    ),
    "pediatric-facility": (
        "pediatric day health / respite facility",
        "pediatric CPR/AED and First Aid, plus BLS for licensed clinical staff",
        "medically complex children and staff coverage across shifts",
    ),
    "other-healthcare": (
        "licensed healthcare facility",
        "AHA BLS for Healthcare Providers",
        "keeping clinical staff current without pulling them off the floor for a whole day",
    ),
    "center": (
        "licensed child care center or preschool",
        "pediatric CPR, AED and First Aid that satisfies their state licensing requirement",
        "keeping every teacher current for licensing, covering ratios while staff train, and "
        "infant/child skills specifically rather than generic adult CPR",
    ),
    "home": (
        "family child care home",
        "pediatric CPR, AED and First Aid for licensing",
        "being a one- or two-person operation where finding a class that fits around child "
        "care hours is the whole problem",
    ),
    "school-age": (
        "school-age / before-and-after-school program",
        "pediatric and child CPR, AED and First Aid for program staff",
        "seasonal staff turnover and getting new hires certified before the session starts",
    ),
    "school": (
        "school",
        "staff CPR, AED and First Aid",
        "getting staff recertified before the school year, and coaches and athletic staff "
        "who need AED confidence on the field",
    ),
    "camp": (
        "camp",
        "camp staff CPR, AED and First Aid (wilderness/remote first aid where relevant)",
        "certifying counselors before the season opens, on a timeline that fits staff training week",
    ),
}

# Coarse segment aliases seen in TAGS. The importer writes "healthcare|skilled-nursing|CA|..."
# so the specific token wins; the bare "healthcare"/"childcare" token is the fallback.
SEGMENT_ALIASES = {
    "healthcare": "other-healthcare",
    "childcare": "center",
    "daycare": "center",
}


def infer_segment(company="", tags=None):
    """Canonical segment for a contact, or "" when we genuinely don't know.

    Prefers an explicit token in TAGS (registry imports write one) over guessing from
    the company name, because the registry knows the facility's licensed type and a
    name like "Sunrise Center" tells us nothing.
    """
    toks = [t.strip().lower() for t in (tags or []) if t and t.strip()]
    for t in toks:                                  # specific segment token wins
        if t in SEGMENT_PLAYBOOK:
            return t
    for t in toks:                                  # coarse token as fallback
        if t in SEGMENT_ALIASES:
            return SEGMENT_ALIASES[t]
    text = (company or "").lower()
    if any(k in text for k in ("hospital", "medical center")):
        return "hospital"
    if any(k in text for k in ("nursing", "post acute", "post-acute", "convalescent",
                               "rehabilitation center", "care center")):
        return "skilled-nursing"
    if any(k in text for k in ("home health", "home care")):
        return "home-health"
    if "hospice" in text:
        return "hospice"
    if any(k in text for k in ("clinic", "dental", "urgent care", "surgery center")):
        return "clinic"
    if any(k in text for k in ("camp",)):
        return "camp"
    if any(k in text for k in ("preschool", "montessori", "child care", "childcare",
                               "daycare", "day care", "learning center", "early learning")):
        return "center"
    if any(k in text for k in ("school", "academy", "elementary", "charter", "district")):
        return "school"
    return ""


def segment_brief(company="", tags=None):
    """(segment, what-they-are, course-line, what-to-talk-about) for the prompt."""
    seg = infer_segment(company, tags)
    if not seg:
        return ("", "", "CPR, AED and First Aid for their team",
                "general workplace readiness — whoever is nearest when something happens")
    label, course, framing = SEGMENT_PLAYBOOK[seg]
    return (seg, label, course, framing)


# Short course names, for places where the full course line would not fit a sentence
# (the fallback body). Same keys as SEGMENT_PLAYBOOK.
SEGMENT_SHORT_COURSE = {
    "hospital": "BLS for Healthcare Providers",
    "skilled-nursing": "BLS for Healthcare Providers",
    "home-health": "BLS for Healthcare Providers",
    "hospice": "BLS for Healthcare Providers",
    "dialysis": "BLS for Healthcare Providers",
    "clinic": "BLS for Healthcare Providers",
    "adult-day": "CPR/AED and First Aid",
    "pediatric-facility": "pediatric CPR and First Aid",
    "other-healthcare": "BLS for Healthcare Providers",
    "center": "pediatric CPR and First Aid",
    "home": "pediatric CPR and First Aid",
    "school-age": "child CPR and First Aid",
    "school": "staff CPR/AED and First Aid",
    "camp": "staff CPR/AED and First Aid",
}


def short_course(company="", tags=None):
    """The course line trimmed to something that reads naturally mid-sentence."""
    return SEGMENT_SHORT_COURSE.get(infer_segment(company, tags), "CPR/AED and First Aid")


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
        "sheet_cursor": ROW_RANGE_START,
    }
    state.update(raw)

    # Cursor floor. A live agent commits state.json back to its repo at the end of every
    # run, so hand-editing sheet_cursor loses a race with any run already in flight
    # (hit on 2026-08-13: a send that started at 9:51a would have overwritten the jump to
    # the healthcare block). SHEET_CURSOR_MIN, set as a repo variable, is re-applied on
    # every load instead, so it survives those commits. To deliberately send the cursor
    # BACKWARD later — e.g. to reclaim the rows skipped by a jump, see state
    # "skipped_ranges" — clear or lower this variable first, or it will pull the cursor
    # straight back to the floor.
    floor = int(os.environ.get("SHEET_CURSOR_MIN", "0") or 0)
    if floor and int(state.get("sheet_cursor", 0) or 0) < floor:
        state["sheet_cursor"] = floor

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
        "sheet_cursor": ROW_RANGE_START,
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

def redact_email(addr: str) -> str:
    """Mask an email for the (now public) Actions logs — 'j***@gmail.com'."""
    addr = (addr or "").strip()
    if "@" not in addr:
        return "***"
    local, _, dom = addr.partition("@")
    return f"{local[:1]}***@{dom}"

# ─── Helpers ──────────────────────────────────────────────────────────────────

def is_weekday():
    return pacific_today().weekday() < 5

def is_monday():
    return pacific_today().weekday() == 0

try:
    from zoneinfo import ZoneInfo
    _PT = ZoneInfo("America/Los_Angeles")
except Exception:
    _PT = None
def pacific_now():
    """Timezone-aware 'now' in Pacific, falling back to naive local time if the tz
    database isn't available (same fallback as pacific_today)."""
    return datetime.now(_PT) if _PT else datetime.now()

def pacific_today():
    """The agent's business day in Pacific time. Fixes the UTC-boundary bug: server
    UTC rolls over at 5pm PT, so the 7pm PT report used to see 'tomorrow' and report
    0 sent. All daily counts + guards key off this so the day lines up with the work."""
    return (datetime.now(_PT) if _PT else datetime.now()).date()
def today_str():
    return pacific_today().isoformat()

def _bump(state, key, n=1):
    """Increment a per-day counter state[key][today] by n (feeds the dashboard)."""
    d = state.setdefault(key, {})
    day = today_str()
    d[day] = d.get(day, 0) + n

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
    if cursor < ROW_RANGE_START:
        cursor = ROW_RANGE_START

    contacts = []
    skipped_roles = []
    WINDOW = 500
    empty_windows = 0

    while (len(contacts) < needed and empty_windows < 3
           and (ROW_RANGE_END is None or cursor <= ROW_RANGE_END)):
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
            # Stop at the partition boundary — never pull a row outside this agent's range.
            if ROW_RANGE_END is not None and c["row"] > ROW_RANGE_END:
                cursor = ROW_RANGE_END + 1
                broke = True
                break
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


def _due_for_followup(c, today):
    """True if a contacted row is due for its next touch in the email cadence.
    Requires reply_status still 'Contacted' (a reply/bounce/unsub ends the sequence),
    a numeric touch 1-3 (reconciled rows have blank touches → excluded), and the last
    touch within [gap, STALE] days (due, but not ancient)."""
    if c.get("reply_status") != "Contacted":
        return False
    try:
        t = int(c.get("touches") or 0)
    except (TypeError, ValueError):
        return False
    if t < 1 or t >= MAX_TOUCHES:
        return False
    try:
        sent = date.fromisoformat((c.get("date_sent") or "")[:10])
    except ValueError:
        return False
    days = (today - sent).days
    return FOLLOWUP_GAP_DAYS.get(t, 999) <= days <= FOLLOWUP_STALE_DAYS


def fetch_followups(svc, state, needed):
    """Scan the already-contacted region (rows 2..cursor) for rows due for their
    next cadence touch. Returns contacts tagged with the touch number to send next."""
    cursor = int(state.get("sheet_cursor", 2) or 2)
    today = pacific_today()
    out = []
    # Only scan this agent's own partition, so it never follows up a row the other agent sent.
    scan_end = cursor if ROW_RANGE_END is None else min(cursor, ROW_RANGE_END + 1)
    # ONE read for the whole span, not one per 1,000 rows. A cursor jump can leave tens of
    # thousands of never-contacted rows behind the cursor (Elena, 2026-08-13), and windowed
    # reads across that gap blow the 60-reads/min Sheets quota and kill the run outright.
    for c in sheets_db.read_range(svc, SPREADSHEET_ID, ROW_RANGE_START, scan_end - 1):
        if len(out) >= needed:
            break
        if c["do_not_contact"].lower() in ("yes", "true", "1", "y"):
            continue
        if not _due_for_followup(c, today):
            continue
        out.append({
            "row": c["row"],
            "email": c["email"],
            "firstName": normalize_name(c["first_name"]),
            "lastName": normalize_name(c["last_name"]),
            "firstNameRaw": c["first_name"],
            "lastNameRaw": c["last_name"],
            "company": c["company"],
            "tags": c["tags"],
            "sourceList": c["source_list"],
            "is_role": is_role_address(c["email"]),
            "touch": int(c["touches"]) + 1,
        })
    log.info(f"  Follow-up scan (rows {ROW_RANGE_START}-{scan_end - 1}): {len(out)} due for next touch")
    return out

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
    log.warning(f"  HubSpot lookup unresolved for {redact_email(contact['email'])} after "
                f"{retries} tries: {last_err} — skipping (will retry next run)")
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
    # Even the fallback should name the right course line — a hospital getting a
    # "CPR/AED training" note reads as a wrong-list send.
    course = short_course(contact.get("company", ""), contact.get("tags", []))
    return (
        f"Hi {first},\n\n"
        f"Wanted to check in — is {company} due for {course} this year? "
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

_MEETING_LINK_RE = re.compile(r"meeting link", re.IGNORECASE)

def build_outreach_bodies(body):
    """
    Given a plaintext email body that contains the phrase "meeting link",
    return (plain, html):
      - plain: the phrase left readable but with the URL appended once so text-only
               clients can still reach the scheduler — e.g. 'meeting link (https://...)'
      - html:  an HTML rendering where the first "meeting link" is a real <a href>
    If there's no scheduler link configured, or the phrase isn't present, html is None
    and plain is returned unchanged (so nothing breaks for the no-link/fallback cases).
    """
    if not MANAE_CALENDAR_LINK or not _MEETING_LINK_RE.search(body):
        return body, None

    # Plaintext: keep the words, but make the URL reachable (only the first occurrence).
    plain = _MEETING_LINK_RE.sub(
        lambda m: f"{m.group(0)} ({MANAE_CALENDAR_LINK})", body, count=1)

    # HTML: escape everything, then hyperlink the first "meeting link", newlines -> <br>.
    esc = html_escape(body)
    href = html_escape(MANAE_CALENDAR_LINK, quote=True)
    esc = _MEETING_LINK_RE.sub(
        lambda m: f'<a href="{href}">{m.group(0)}</a>', esc, count=1)
    html = esc.replace("\n", "<br>\n")
    return plain, html

def generate_emails_batch(contacts):
    """
    Generate personalized emails for a list of contacts in a single Claude API call.
    Returns a list of dicts: [{subject, body, personalization_notes}, ...]
    in the same order as the input contacts.
    """
    if not contacts:
        return []

    cta_line = (
        "- Close with a soft ask for a quick 15-minute call. Point them to the scheduler "
        "using the exact phrase \"meeting link\" as the thing they click "
        "(e.g. 'grab whatever time works on my meeting link' or 'my meeting link has a "
        "few open times'). Do NOT paste a URL — write the words \"meeting link\" exactly "
        "once and nothing more; the words will be turned into the clickable link for you."
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
        "COURSE LINE — speak the language of the org you're writing to. Each contact "
        "carries an 'orgType', a 'courseLine', and 'whatMattersToThem'. Name the course "
        "line the way that org's own staff would say it, and build the note around what "
        "matters to them:\n"
        "  - Clinical / healthcare orgs (hospitals, skilled nursing, home health, hospice, "
        "dialysis, clinics) certify staff in BLS — AHA BLS for Healthcare Providers, on a "
        "two-year renewal. Call it BLS. Do NOT pitch them a 'CPR and First Aid class' or "
        "anything pediatric-flavored; they will read it as someone who doesn't know their "
        "world. Talk about clinical staff, renewals, onboarding cohorts, shift coverage.\n"
        "  - Child care centers, preschools and family child care homes need pediatric CPR, "
        "AED and First Aid to satisfy a licensor. Never call that BLS.\n"
        "  - Schools and school-age programs: staff CPR/AED and First Aid, timed to the "
        "school year; coaches and athletic staff for AED.\n"
        "  - Camps: certifying counselors before the season.\n"
        "  - If orgType is unknown, stay general (CPR/AED and First Aid) and do not guess "
        "at a specialty.\n"
        "Use the terminology naturally in ONE place — do not stack credentials or list "
        "course names like a catalog.\n\n"
        "Pick ONE problem framing that best fits the org and use it naturally:\n"
        "  - Coordination burden: one person gets stuck finding training, scheduling, and "
        "tracking who's current.\n"
        "  - Readiness gap: certified on paper, but would the team freeze under real pressure?\n"
        "  - Annual scramble: compliance handled last-minute once a year by whoever has bandwidth.\n"
        "  - Customization: generic CPR training doesn't fit their specific environment and risks.\n\n"
        "TOUCH (each contact has a 'touch' number 1-4 in a multi-email sequence):\n"
        "  1 = first email — the consultative opener described above.\n"
        "  2 = short follow-up (2-3 sentences) gently resurfacing the first note; assume "
        "they may have missed it, no guilt-trip.\n"
        f"  3 = value email — you may cite this TRUE proof point (tailor which audience you "
        f"emphasize to their org type; for clinical orgs lean on the AHA alignment instead, "
        f"since AHA is the standard their BLS cards come from): \"{PROOF_POINT}\". Then share the KIND of outcome we "
        "typically deliver (we take the compliance coordination off their plate; staff who "
        "are genuinely ready in an emergency). Do NOT invent specific schools, names, "
        "numbers, or testimonials beyond that proof point.\n"
        "  4 = soft breakup — brief and gracious ('I'll stop reaching out; the door's open "
        "anytime'), stay warm, no pressure.\n"
        "For touches 2-4 keep it shorter than touch 1 and reference that you're following "
        "up. Every touch still ends with the call-to-action below.\n\n"
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
        seg, seg_label, course_line, seg_framing = segment_brief(
            c.get("company", ""), c.get("tags", []))
        contact_list.append({
            "index": i,
            "touch": c.get("touch", 1),
            "email": c.get("email", ""),
            "addressType": "shared/role inbox" if c.get("is_role") else "individual",
            "firstName": c.get("firstNameRaw") or c.get("firstName") or "",
            "lastName": c.get("lastNameRaw") or c.get("lastName") or "",
            "company": c.get("company") or "your organization",
            "industry": industry,
            "orgType": seg_label or "unknown",
            "courseLine": course_line,
            "whatMattersToThem": seg_framing,
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

def send_gmail_smtp(to, subject, body, cc=None, html=None):
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    gmail_user = os.environ.get("GMAIL_ADDRESS", "")
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD", "")

    if not gmail_user or not gmail_pass:
        log.error("  GMAIL_ADDRESS or GMAIL_APP_PASSWORD not set")
        return {"success": False, "error": "Gmail credentials not configured"}

    msg = MIMEMultipart("alternative") if html else MIMEMultipart()
    msg["From"]    = f"{SENDING_NAME} <{gmail_user}>"
    msg["To"]      = to
    msg["Subject"] = subject
    if cc:
        msg["Cc"] = cc

    full_body = body if SENDING_NAME in body else body + f"\n\n{SENDING_NAME} | {COMPANY_NAME}"
    msg.attach(MIMEText(full_body, "plain"))
    if html:                       # HTML part last = clients prefer it, plain is the fallback
        msg.attach(MIMEText(html, "html"))

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

_HTML_TAG_RE = re.compile(r"<[^>]+>")

def _html_to_text(html):
    """Strip an HTML email part down to readable text (no external deps)."""
    html = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", html)
    html = re.sub(r"(?i)<br\s*/?>", "\n", html)
    html = re.sub(r"(?i)</(p|div|tr|li|h[1-6]|blockquote)>", "\n", html)
    text = html_unescape(_HTML_TAG_RE.sub("", html))
    text = re.sub(r"[ \t]+\n", "\n", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()

def _reply_body(msg):
    """Extract a reply's readable text. Prefer text/plain, but fall back to a tag-stripped
    text/html part so an HTML-only reply is NEVER seen as an empty message (which used to
    blank Manae's forward AND make the interest classifier default to 'interested')."""
    plain, html = "", ""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_disposition() == "attachment":
                continue
            ct = part.get_content_type()
            if ct == "text/plain" and not plain:
                p = part.get_payload(decode=True)
                if p:
                    plain = p.decode(part.get_content_charset() or "utf-8", errors="replace")
            elif ct == "text/html" and not html:
                p = part.get_payload(decode=True)
                if p:
                    html = p.decode(part.get_content_charset() or "utf-8", errors="replace")
    else:
        p = msg.get_payload(decode=True)
        if p:
            body = p.decode(msg.get_content_charset() or "utf-8", errors="replace")
            if msg.get_content_type() == "text/html":
                html = body
            else:
                plain = body
    if plain.strip():
        return plain
    if html.strip():
        return _html_to_text(html)
    return ""

def classify_interest(subject, body_text):
    """
    Triage a genuine-looking inbound reply.
    Returns (is_auto, opt_out, interested, tier, reason), where tier is "sql" for a real
    buying signal, "potential" for a warm reply with no ask, or "" when not interested.
      is_auto    = automated / no-action message (auto-ack, OOO, email-change notice,
                   ticket confirmation) that Manae does NOT need.
      opt_out    = the person wants off the list (stop / remove me / unsubscribe).
      interested = a genuine human showing ANY curiosity or engagement = a sales-qualified
                   lead (broad, per our SDR playbook — not just explicit booking).
    On error, returns (False, False, False, ...) so a real reply is still forwarded.
    """
    # Never guess "interested" from an empty/blank body — that manufactured a bogus SQL
    # (Barbara, 2026-08-05). Forward it for a human to eyeball instead; no HubSpot write.
    if not (body_text or "").strip():
        return False, False, False, "", "no readable message body — needs human review"
    try:
        payload = {
            "model": GEN_MODEL,
            "max_tokens": 150,
            "system": (
                "You triage inbound replies to a CPR-training outreach email. Return ONLY "
                'JSON: {"auto": bool, "opt_out": bool, "interested": bool, '
                '"buying_signal": bool, "reason": "<=12 words"}.\n'
                "auto = an automated/no-action message (out-of-office, 'thanks for reaching "
                "out' autoresponder, email-address-change or 'update your records' notice, "
                "ticket/case confirmation, no-reply/bulk). false for a genuine human reply.\n"
                "opt_out = the person wants off the list OR clearly declines. Set true for "
                "removal requests ('remove me', 'unsubscribe', 'stop emailing', 'take me off "
                "your list', 'do not contact us') AND for a clear rejection ('no', 'no thanks', "
                "'not interested', \"we're not interested\", 'please stop'). A SOFT deferral "
                "('not right now', 'maybe next year', 'circle back later') is NOT opt_out — "
                "that's a genuine not-interested reply we keep for later.\n"
                "interested = a genuine human showing ANY curiosity or engagement — this is a "
                "sales-qualified lead and the bar is BROAD: 'tell me more', a question about "
                "how it works / pricing / availability / scheduling, naming who handles this, "
                "'send info', 'we might be interested', looping in a colleague, or anything "
                "that opens a real conversation. Only a flat rejection ('not interested', "
                "'no thanks'), an opt_out, or an auto/unrelated message is NOT interested. "
                "When unsure between interested and not, lean interested — a human reviews it.\n"
                "buying_signal = true ONLY when the reply contains something a salesperson "
                "can act on TODAY: asks what it costs / for a quote, asks about dates, "
                "availability or scheduling a class, states how many people need training, "
                "or asks to book or sign up. Everything else is false — 'send me info', "
                "'tell me more', 'what do you offer', naming who handles this, forwarding us "
                "to a colleague, or confirming they train annually are all interested=true "
                "but buying_signal=false. A reply that defers to a future date ('next year', "
                "'at our renewal in March') is buying_signal=false even if it mentions cost. "
                "Do NOT lean true here: this flag decides whether a human is asked to drop "
                "what they are doing, and a false positive costs us their trust in the queue."
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
        interested = bool(r.get("interested"))
        tier = ("sql" if bool(r.get("buying_signal")) else "potential") if interested else ""
        return (bool(r.get("auto")), bool(r.get("opt_out")), interested, tier,
                r.get("reason", ""))
    except Exception as e:
        log.warning(f"  Reply triage failed: {e} — forwarding to Manae to be safe")
        return False, False, False, "", "classifier error"

_HS_PORTAL = None
_HS_TRAFFIC_PROP = None  # cache: (property_internal_name, option_value) or (None, None)

# Label of the contact property we stamp on AI-sourced leads, and the option we set it
# to. Overridable by env in case GCD renames them. We resolve the *internal* names from
# these labels at runtime so a rename in HubSpot doesn't silently break attribution.
# GCD's HubSpot has a built-in, writable "Latest Traffic Source" (hs_latest_source) enum
# whose options include "AI Referrals". That's the field we stamp on Vida-sourced leads.
# Every lead we hand over gets an owner in HubSpot and a task in that owner's queue.
# Before this, the SQL write set lifecycle + lead status and nothing else, so a handoff
# lived only as one email in one inbox: nothing appeared in a queue, nothing measured a
# response time, and a missed lead died silently (Chris, 2026-08-13). GCD = Manae.
LEAD_OWNER_EMAIL = os.environ.get("LEAD_OWNER_EMAIL", "manae@getcprdone.com")
# When the owner's task comes due, and — separately — when a lead with no logged
# follow-up escalates to Chris. The task is deliberately short-fused: it should be
# sitting at the top of the queue within the half hour, long before the 12h escalation
# that exists as a backstop, not as the target (Chris, 2026-08-13).
TASK_DUE_MINUTES = int(os.environ.get("TASK_DUE_MINUTES", "30") or 30)
# Leads are announced in Slack so the team sees the queue without waiting for an email
# digest: GCD leads to #GCD, Joffe leads to #GrowthTeam (Chris, 2026-08-13). Unset token
# = silently skipped, so a missing secret never costs us a run.
SLACK_BOT_TOKEN    = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_LEADS_CHANNEL = os.environ.get("SLACK_LEADS_CHANNEL") or "C0804PH0W0Z"   # #GCD (private)
STALL_HOURS = int(os.environ.get("STALL_HOURS", "12") or 12)

TRAFFIC_SOURCE_PROP_LABEL   = os.environ.get("HS_TRAFFIC_SOURCE_LABEL", "Latest Traffic Source")
TRAFFIC_SOURCE_OPTION_LABEL = os.environ.get("HS_TRAFFIC_SOURCE_OPTION", "AI Referrals")

_HS_OWNER_ID = None

def _resolve_owner_id(email=None):
    """HubSpot owner id for the person who works our leads, resolved by email.

    Cached for the process. Returns "" when the lookup fails or the address isn't a
    HubSpot user — the caller then writes the lead without an owner rather than losing
    the whole record.
    """
    global _HS_OWNER_ID
    if _HS_OWNER_ID is not None:
        return _HS_OWNER_ID
    _HS_OWNER_ID = ""
    want = (email or LEAD_OWNER_EMAIL or "").strip().lower()
    if not want or not HUBSPOT_TOKEN:
        return _HS_OWNER_ID
    try:
        data = http_get("https://api.hubapi.com/crm/v3/owners?limit=200",
                        {"Authorization": f"Bearer {HUBSPOT_TOKEN}"})
        for o in data.get("results", []):
            if (o.get("email") or "").strip().lower() == want:
                _HS_OWNER_ID = str(o.get("id", ""))
                log.info(f"    HubSpot owner resolved: {want} -> {_HS_OWNER_ID}")
                return _HS_OWNER_ID
        log.warning(f"    HubSpot owner {want} not found among portal users — "
                    f"leads will be written without an owner")
    except Exception as e:
        log.warning(f"    HubSpot owner lookup failed: {e}")
    return _HS_OWNER_ID


def hubspot_create_followup_task(cid, owner_id, name, why):
    """Put the handoff in the owner's HubSpot task queue, due in TASK_DUE_MINUTES.

    Best-effort: a failure here never blocks the lead write or the handoff email.
    """
    if not cid or not owner_id:
        return ""
    due_ms = int((time.time() + TASK_DUE_MINUTES * 60) * 1000)
    props = {
        "hs_task_subject": f"Follow up: {name or 'new lead'} (CPR/First Aid enquiry)",
        "hs_task_body": (why or "Replied to outreach.") + " — handed over by " + SENDING_NAME,
        "hs_task_status": "NOT_STARTED",
        "hs_task_priority": "HIGH",
        "hs_task_type": "EMAIL",
        "hs_timestamp": due_ms,
        "hubspot_owner_id": owner_id,
    }
    body = {"properties": props, "associations": [{
        "to": {"id": cid},
        "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 204}],
    }]}
    try:
        st, data = http_post_raw("https://api.hubapi.com/crm/v3/objects/tasks",
                                 {"Authorization": f"Bearer {HUBSPOT_TOKEN}",
                                  "Content-Type": "application/json"}, body)
        if st in (200, 201):
            return str(data.get("id", ""))
        log.warning(f"    HubSpot task create failed ({st}): {str(data)[:180]}")
    except Exception as e:
        log.warning(f"    HubSpot task create failed: {e}")
    return ""


def slack_post(text, dry_run=False):
    """Post a line to the leads channel. Best-effort and never fatal — a Slack outage or a
    missing token must not stop a lead reaching sales by email."""
    if not SLACK_BOT_TOKEN or not SLACK_LEADS_CHANNEL:
        return False
    if dry_run:
        log.info(f"  [DRY RUN] Slack -> {SLACK_LEADS_CHANNEL}: {text[:120]}")
        return False
    try:
        st, data = http_post_raw(
            "https://slack.com/api/chat.postMessage",
            {"Authorization": f"Bearer {SLACK_BOT_TOKEN}", "Content-Type": "application/json"},
            {"channel": SLACK_LEADS_CHANNEL, "text": text, "unfurl_links": False})
        if st == 200 and data.get("ok"):
            return True
        # Slack answers 200 with ok:false and a reason (not_in_channel, channel_not_found…)
        log.warning(f"  Slack post failed: {data.get('error', st)}")
    except Exception as e:
        log.warning(f"  Slack post failed: {e}")
    return False


def _track_open_lead(state, email, cid, name, company, why):
    """Remember a handed-over SQL so we can check that somebody actually worked it."""
    open_leads = state.get("open_leads", [])
    if any((l.get("email") or "").lower() == (email or "").lower() and not l.get("closed")
           for l in open_leads):
        return
    open_leads.append({
        "email": email, "cid": cid, "name": name, "company": company, "why": why,
        "handed_at": pacific_now().isoformat(timespec="seconds"),
        "alerted": False, "closed": False,
    })
    state["open_leads"] = open_leads


def _hubspot_followup_state(cid):
    """Has anyone touched this contact since we handed it over?

    Reads the sales-activity timestamps HubSpot maintains plus the lead status, so a call,
    an email, a logged note or a status change all count. Returns
    (touched_at_epoch_ms_or_0, lead_status, lifecycle) — (None, "", "") if unreadable, and
    the caller then leaves the lead alone rather than crying wolf on an API failure.
    """
    if not cid or not HUBSPOT_TOKEN:
        return None, "", ""
    props = ["notes_last_contacted", "hs_last_sales_activity_timestamp",
             "hs_lead_status", "lifecyclestage", "num_notes"]
    url = (f"https://api.hubapi.com/crm/v3/objects/contacts/{cid}"
           f"?properties={','.join(props)}")
    try:
        data = http_get(url, {"Authorization": f"Bearer {HUBSPOT_TOKEN}"})
    except Exception as e:
        log.warning(f"  stall check: could not read contact {cid}: {e}")
        return None, "", ""
    pr = data.get("properties", {}) or {}

    def _ms(v):
        if not v:
            return 0
        try:                                   # HubSpot returns ISO-8601 on v3 reads
            return int(datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp() * 1000)
        except Exception:
            try:
                return int(v)
            except Exception:
                return 0
    touched = max(_ms(pr.get("notes_last_contacted")),
                  _ms(pr.get("hs_last_sales_activity_timestamp")))
    return touched, (pr.get("hs_lead_status") or ""), (pr.get("lifecyclestage") or "")


def check_stalled_leads(state, dry_run=False):
    """Escalate any handed-over SQL with no logged follow-up after STALL_HOURS.

    Emails Chris and copies the lead owner, once per lead. This is the safety net that did
    not exist: once a row was marked SQL nothing ever looked at it again, so a missed lead
    was invisible (Chris, 2026-08-13).
    """
    open_leads = state.get("open_leads", [])
    if not open_leads:
        return 0
    now = pacific_now()
    stalled, closed = [], 0
    for lead in open_leads:
        if lead.get("closed") or lead.get("alerted"):
            continue
        try:
            handed = datetime.fromisoformat(lead["handed_at"])
        except Exception:
            lead["closed"] = True
            continue
        hours = (now - handed).total_seconds() / 3600.0
        if hours < STALL_HOURS:
            continue
        touched, lead_status, lifecycle = _hubspot_followup_state(lead.get("cid"))
        if touched is None:
            continue                            # unreadable — don't guess, try next run
        worked = (touched > int(handed.timestamp() * 1000)
                  or (lead_status or "").upper() not in ("", "NEW")
                  or (lifecycle or "") in ("opportunity", "customer"))
        if worked:
            lead["closed"] = True
            closed += 1
            log.info(f"  stall check: {redact_email(lead['email'])} was worked — clearing")
        else:
            lead["alerted"] = True
            stalled.append((lead, hours))
    if closed or stalled:
        state["open_leads"] = [l for l in open_leads
                              if not (l.get("closed") and l.get("alerted") is False)]
        if not dry_run:
            save_state(state)
    if not stalled:
        return 0

    lines = []
    for lead, hours in stalled:
        link = _hubspot_link(lead.get("cid"))
        lines.append(
            f"{lead.get('name') or lead.get('company') or 'Lead'} — {lead['email']}\n"
            f"    asked: {lead.get('why') or 'replied to outreach'}\n"
            f"    handed over: {lead['handed_at'][:16].replace('T', ' ')} "
            f"({hours:.0f}h ago)\n"
            + (f"    {link}\n" if link else "")
        )
    subject = (f"[{int(STALL_HOURS)}h no follow-up] {len(stalled)} lead"
               f"{'s' if len(stalled) != 1 else ''} waiting — "
               + (stalled[0][0].get("name") or stalled[0][0]["email"]))
    body = (
        f"These leads asked us something concrete and have no logged follow-up in HubSpot "
        f"{int(STALL_HOURS)} hours after handoff:\n\n"
        + "\n".join(lines)
        + f"\nEach one has a HubSpot task on {LEAD_OWNER_EMAIL}. Nothing has been logged "
        f"against the contact — a reply sent from outside HubSpot won't show here, so this "
        f"may be a logging gap rather than a missed lead.\n\n"
        f"— {SENDING_NAME} (automated watch on handed-over leads)\n"
    )
    if dry_run:
        log.info(f"[DRY RUN] stall alert would go to {CHRIS_EMAIL} (cc {LEAD_OWNER_EMAIL}):"
                 f"\n{subject}\n{body}")
    else:
        send_email(CHRIS_EMAIL, subject, body, cc=LEAD_OWNER_EMAIL)
        for lead, hours in stalled:
            slack_post(
                f":rotating_light: *No follow-up in {hours:.0f}h* — "
                f"{lead.get('name') or lead['email']}"
                + (f" · {lead.get('company')}" if lead.get("company") else "")
                + f"\n_{lead.get('why','')}_\n{lead['email']}"
                + (f" · <{_hubspot_link(lead.get('cid'))}|HubSpot>" if lead.get("cid") else "")
                + f"\nNothing logged against the contact since handoff — may be a logging gap.")
        log.info(f"  stall check: escalated {len(stalled)} lead(s) to Chris")
    return len(stalled)


def _resolve_traffic_source_prop():
    """Resolve the internal (property_name, option_value) for stamping AI-sourced leads
    with 'Original Traffic Source = AI Referral'. Matches by *label* against GCD's contact
    properties, and only returns a property that is (a) writable and (b) has the AI Referral
    option defined — so we never send a value HubSpot will reject and kill the whole write.
    Returns (None, None) if the property/option isn't set up yet (Vida just skips it then).
    Cached for the process."""
    global _HS_TRAFFIC_PROP
    if _HS_TRAFFIC_PROP is not None:
        return _HS_TRAFFIC_PROP
    _HS_TRAFFIC_PROP = (None, None)
    want_prop = TRAFFIC_SOURCE_PROP_LABEL.strip().lower()
    want_opt  = TRAFFIC_SOURCE_OPTION_LABEL.strip().lower()
    try:
        data = http_get("https://api.hubapi.com/crm/v3/properties/contacts",
                        {"Authorization": f"Bearer {HUBSPOT_TOKEN}"})
        for p in data.get("results", []):
            if p.get("label", "").strip().lower() != want_prop:
                continue
            # Skip HubSpot's read-only built-in "Original Traffic Source" (hs_analytics_source)
            # and anything else calculated/read-only — writing to it would 400.
            meta = p.get("modificationMetadata") or {}
            if meta.get("readOnlyValue"):
                continue
            for opt in p.get("options", []):
                if opt.get("label", "").strip().lower() == want_opt:
                    _HS_TRAFFIC_PROP = (p["name"], opt["value"])
                    log.info(f"    HubSpot: traffic-source attribution → {p['name']}={opt['value']}")
                    return _HS_TRAFFIC_PROP
        log.info("    HubSpot: no writable 'Original Traffic Source' property with an "
                 "'AI Referral' option found — skipping attribution (create it to enable).")
    except Exception as e:
        log.warning(f"    HubSpot traffic-source property lookup failed: {e}")
    return _HS_TRAFFIC_PROP

def _hubspot_portal_id():
    """Fetch (and cache) the GCD HubSpot portal id, for building record links."""
    global _HS_PORTAL
    if _HS_PORTAL is not None:
        return _HS_PORTAL
    try:
        data = http_get("https://api.hubapi.com/account-info/v3/details",
                        {"Authorization": f"Bearer {HUBSPOT_TOKEN}"})
        _HS_PORTAL = str(data.get("portalId", "") or "")
    except Exception:
        _HS_PORTAL = ""
    return _HS_PORTAL

def _hubspot_link(contact_id):
    pid = _hubspot_portal_id()
    return f"https://app.hubspot.com/contacts/{pid}/record/0-1/{contact_id}" if pid and contact_id else ""

def _clean_name(name):
    """Normalize a name for human-facing use. Export lists are often ALL-CAPS
    ('NOWLEN', 'REBECCA'), which reads badly in an email a person will forward. Title-case
    all-upper / all-lower tokens; leave already-mixed-case names (McDonald, O'Brien) alone.
    Handles hyphens and apostrophes so 'MARY-JANE' -> 'Mary-Jane', \"O'BRIEN\" -> \"O'Brien\"."""
    if not name:
        return ""
    def fix(tok):
        if tok and (tok.isupper() or tok.islower()):
            return tok[:1].upper() + tok[1:].lower()
        return tok
    words = []
    for word in name.split():
        parts = re.split(r"([-'])", word)  # keep the separators
        words.append("".join(p if p in "-'" else fix(p) for p in parts))
    return " ".join(words).strip()


def _extract_phone(text):
    """Pull a plausible US phone number from an email body/signature, or ''."""
    if not text:
        return ""
    m = re.search(r'(?:\+?1[\s.\-]?)?\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}', text)
    return m.group(0).strip() if m else ""

def hubspot_upsert_sql(email, first="", last="", company="", phone="",
                       stage="salesqualifiedlead", why=""):
    """Create or update a HubSpot contact as a lead. Returns the contact id (for a record
    link) or '' on failure. Needs contacts write scope.

    stage: "salesqualifiedlead" for a real buying signal (asked price / dates / headcount)
    or "marketingqualifiedlead" for a warm reply with no ask. Everything that engaged used
    to be written as SQL, which inflated the pipeline and taught sales to distrust the
    label — 27 of the first 46 "SQLs" had no buying signal in them (Chris, 2026-08-13).

    Only true SQLs get an owner task; an MQL still gets the owner so it has a home.
    """
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"}
    # Lifecycle stage per the tier above, Lead Status = NEW. Original Traffic Source =
    # AI Referral (stamped only when GCD's HubSpot has that writable property/option —
    # resolved by label).
    props = {"email": email, "lifecyclestage": stage, "hs_lead_status": "NEW"}
    owner_id = _resolve_owner_id()
    if owner_id:
        props["hubspot_owner_id"] = owner_id
    if first:   props["firstname"] = first
    if last:    props["lastname"]  = last
    if company: props["company"]   = company
    if phone:   props["phone"]     = phone
    ts_prop, ts_val = _resolve_traffic_source_prop()
    if ts_prop:
        props[ts_prop] = ts_val
    try:
        status, data = http_post_raw(
            "https://api.hubapi.com/crm/v3/objects/contacts/search", headers,
            {"filterGroups": [{"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}],
             "properties": ["email"], "limit": 1},
        )
        if status == 200 and data.get("results"):
            cid = str(data["results"][0]["id"])
            body = json.dumps({"properties": props}).encode("utf-8")
            req = urllib.request.Request(
                f"https://api.hubapi.com/crm/v3/objects/contacts/{cid}",
                data=body, headers=headers, method="PATCH")
            with urllib.request.urlopen(req, timeout=60, context=ssl_ctx()) as r:
                r.read()
            log.info(f"    HubSpot: updated {redact_email(email)} → {stage}"
                     + (f" (owner {owner_id})" if owner_id else " (no owner)"))
            if stage == "salesqualifiedlead":
                hubspot_create_followup_task(cid, owner_id,
                                             (first + " " + last).strip() or company, why)
            return cid
        st, cdata = http_post_raw("https://api.hubapi.com/crm/v3/objects/contacts", headers,
                                  {"properties": props})
        if st in (200, 201):
            cid = str(cdata.get("id", ""))
            log.info(f"    HubSpot: created {redact_email(email)} → {stage}"
                     + (f" (owner {owner_id})" if owner_id else " (no owner)"))
            if stage == "salesqualifiedlead":
                hubspot_create_followup_task(cid, owner_id,
                                             (first + " " + last).strip() or company, why)
            return cid
        log.info(f"    HubSpot: create FAILED ({st}) {redact_email(email)}")
        return ""
    except Exception as e:
        log.warning(f"    HubSpot SQL upsert failed for {redact_email(email)}: {e}")
        return ""

def draft_air_reply(subject, body_text):
    """Draft a suggested A-I-R reply (Acknowledge / Insert Value / Re-engage) for Manae
    to review and send. Returns '' on failure (the forward still goes out)."""
    try:
        payload = {
            "model": GEN_MODEL,
            "max_tokens": 400,
            "system": (
                f"You are {SENDING_NAME} at {COMPANY_NAME} (CPR/First Aid training). Draft a "
                "SHORT suggested reply for a human teammate to review and send to a prospect "
                "who just replied. Use the A-I-R framework: (A) Acknowledge their message "
                "warmly; (I) Insert Value — address their point (common ones: no budget yet, "
                "board/committee approval, a leadership transition, or only needing coverage "
                "part of the year) and reassure without pressure, never quote a price; "
                "(R) Re-engage with one concrete next step — offer a quick 15-minute call and "
                "the scheduling link. Consultative, warm, no hard close, no exclamation points, "
                "3-5 sentences. "
                + (f"Scheduling link: {MANAE_CALENDAR_LINK}. " if MANAE_CALENDAR_LINK else "")
                + "Return ONLY the suggested email body."
            ),
            "messages": [{"role": "user", "content": f"Prospect reply — Subject: {subject}\n\n{body_text[:1500]}"}],
        }
        headers = {"Content-Type": "application/json", "x-api-key": ANTHROPIC_API_KEY,
                   "anthropic-version": "2023-06-01"}
        data = http_post("https://api.anthropic.com/v1/messages", headers, payload, timeout=120)
        return next((b["text"] for b in data.get("content", []) if b.get("type") == "text"), "").strip()
    except Exception as e:
        log.warning(f"  A-I-R draft failed: {e}")
        return ""

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

    # Unsubscribe / opt-out — check subject AND the first lines of the body
    unsub_phrases = [
        "unsubscribe", "remove me", "remove us", "opt out", "opt-out", "take me off",
        "take us off", "please remove", "no longer wish", "do not contact",
        "stop emailing", "stop contacting", "please stop",
    ]
    if any(k in subject_l for k in unsub_phrases) or any(k in body_l[:400] for k in unsub_phrases):
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
        """Write status back to the sheet row for this email address, if we can find it.
        Any status change also stamps reply_date with today, so "which leads came in
        today" is answerable from the sheet itself instead of only from the in-state
        daily counters (which started mid-flight and bucket by run date)."""
        if dry_run or not svc:
            return
        r = email_index.get((addr or "").lower())
        if not r:
            return
        if fields.get("reply_status") and "reply_date" not in fields:
            fields["reply_date"] = today_str()
        try:
            sheets_db.update_row(svc, SPREADSHEET_ID, r, **fields)
        except Exception as e:
            log.warning(f"    Could not write row {r} for {addr}: {e}")

    try:
        ctx  = ssl_ctx()
        # timeout is essential: without it a stalled Gmail socket blocks forever, and because
        # all runs share one concurrency group, a hung reply_check holds the lock and jams the
        # daily send behind it (happened 2026-08-05 — a reply_check hung 1.5h+). The socket
        # timeout applies to login/select/search/fetch too, so any stall errors out fast.
        mail = imaplib.IMAP4_SSL("imap.gmail.com", ssl_context=ctx, timeout=60)
        mail.login(gmail_user, gmail_pass)
        mail.select("INBOX")

        # Search ALL unread — SMTP sends don't give us real Gmail thread IDs.
        _, msg_ids = mail.search(None, "UNSEEN")
        all_mids = msg_ids[0].split() if msg_ids[0] else []
        log.info(f"  {len(all_mids)} unread messages in inbox")

        genuine_count = sql_count = archived_count = unsubscribe_count = 0
        potential_count = 0
        today = today_str()

        for mid in all_mids:
            try:
                _, msg_data = mail.fetch(mid, "(RFC822)")
                msg = emaillib.message_from_bytes(msg_data[0][1])

                sender  = msg.get("From", "Unknown")
                subject = msg.get("Subject", "")
                body_text = _reply_body(msg)

                sender_name, sender_email = parse_from(sender)
                # RFC-3834 / common autoresponder headers — a cheap, reliable auto signal
                auto_hdr   = (msg.get("Auto-Submitted", "") or "").lower()
                precedence = (msg.get("Precedence", "") or "").lower()
                is_auto_header = (
                    "auto-replied" in auto_hdr or "auto-generated" in auto_hdr
                    or "auto_reply" in precedence
                    or bool(msg.get("X-Autoreply")) or bool(msg.get("X-Autorespond"))
                )
                kind = classify_reply(sender, subject, body_text)

                if kind == "bounce":
                    log.info(f"  Bounce from {redact_email(sender_email)} — archiving silently")
                    email_match = re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", body_text)
                    if email_match:
                        bounced = email_match.group(0).lower()
                        do_not_contact.add(bounced)
                        mark_row(bounced, reply_status="Bounced", do_not_contact="yes",
                                 last_result="hard bounce (inbox)")
                    if not dry_run:
                        archive_message(mail, mid)
                        _bump(state, "daily_bounce_count")
                    archived_count += 1

                elif kind == "ooo":
                    log.info(f"  OOO from {redact_email(sender_email)} — archiving silently")
                    if not dry_run:
                        archive_message(mail, mid)
                    archived_count += 1

                elif kind == "unsubscribe":
                    log.info(f"  Unsubscribe from {redact_email(sender_email)} — archiving + blocking")
                    if sender_email:
                        do_not_contact.add(sender_email)
                        mark_row(sender_email, reply_status="Unsubscribed",
                                 do_not_contact="yes", last_result="unsubscribe request")
                    if not dry_run:
                        archive_message(mail, mid)
                        _bump(state, "daily_unsub_count")
                    unsubscribe_count += 1
                    archived_count += 1

                elif kind == "genuine":
                    if is_auto_header:
                        log.info(f"  Auto-reply header from {redact_email(sender_email)} — archiving, not forwarding")
                        if not dry_run:
                            archive_message(mail, mid)
                        archived_count += 1
                        continue
                    auto, opt_out, interested, tier, reason = classify_interest(subject, body_text)
                    if auto:
                        log.info(f"  Auto-reply from {redact_email(sender_email)} — archiving, not forwarding")
                        if not dry_run:
                            archive_message(mail, mid)
                        archived_count += 1
                        continue
                    if opt_out:
                        log.info(f"  Opt-out reply from {redact_email(sender_email)} — unsubscribing + blocking")
                        if sender_email:
                            do_not_contact.add(sender_email)
                            mark_row(sender_email, reply_status="Unsubscribed",
                                     do_not_contact="yes", last_result="opt-out (reply)")
                        if not dry_run:
                            archive_message(mail, mid)
                            _bump(state, "daily_unsub_count")
                        unsubscribe_count += 1
                        archived_count += 1
                        continue
                    # Prefer the cleaned name/company we already hold in the sheet over
                    # parsing the reply's From header (fixes the "name doesn't sync" gap
                    # Manae hit). Fetched for every genuine reply so the context Manae
                    # receives — and the HubSpot record we write — is as complete as we can.
                    first = (sender_name.split()[0] if sender_name else "")
                    last  = (sender_name.split()[-1] if len(sender_name.split()) > 1 else "")
                    company = ""
                    row = email_index.get(sender_email)
                    if row and svc:
                        try:
                            sf, sl = sheets_db.read_name(svc, SPREADSHEET_ID, row)
                            # The purchased list has junk in the name columns: the same word in
                            # both ("Melody"/"MELODY" → the "Melody Melody" handoff Chris caught
                            # 2026-08-11), and first/last swapped on plenty of rows. When the
                            # sheet's pair is that self-duplicate, trust the reply's own From
                            # display name instead — the prospect typed that themselves.
                            if sf and sl and sf.strip().lower() == sl.strip().lower():
                                if sender_name:
                                    sf, sl = first, last     # keep the From-header name
                                else:
                                    sl = ""                  # else at least don't say it twice
                            first, last = (sf or first), (sl or last)
                        except Exception:
                            pass
                        try:
                            company = sheets_db.read_company(svc, SPREADSHEET_ID, row) or ""
                        except Exception:
                            pass
                    phone    = _extract_phone(body_text)
                    hs_link  = ""
                    hs_added = False

                    # Export data is frequently ALL-CAPS — normalize before it's written to
                    # HubSpot or shown to Manae so everything reads like a human wrote it.
                    first = _clean_name(first)
                    last  = _clean_name(last)

                    is_sql = interested and tier == "sql"
                    if interested:
                        # Two tiers. A buying signal (price / dates / headcount) is a real
                        # SQL: owner task + 12h escalation. A warm reply with no ask goes in
                        # as an MQL so the SQL queue stays trustworthy.
                        stage = "salesqualifiedlead" if is_sql else "marketingqualifiedlead"
                        label = "SQL" if is_sql else "Potential SQL"
                        log.info(f"  {label} from {redact_email(sender_email)} ({reason}) "
                                 f"→ HubSpot + Manae")
                        if not dry_run:
                            # Write the lead into GCD HubSpot with everything we know
                            # (name, company, phone if in signature), staged per tier.
                            cid      = hubspot_upsert_sql(sender_email, first, last,
                                                          company=company, phone=phone,
                                                          stage=stage, why=reason)
                            hs_link  = _hubspot_link(cid)
                            hs_added = bool(cid)   # only true when the write actually succeeded
                            if is_sql:
                                _bump(state, "daily_sql_count")
                                who_s = (first + " " + last).strip() or company or sender_email
                                slack_post(
                                    f":inbox_tray: *New SQL — {who_s}*"
                                    + (f" · {company}" if company else "")
                                    + f"\n_{reason}_\n{sender_email}"
                                    + (f" · <{hs_link}|HubSpot>" if hs_link else "")
                                    + f"\nOwner: {LEAD_OWNER_EMAIL} · task due in "
                                    f"{TASK_DUE_MINUTES} min · via {SENDING_NAME}")
                                # Watch it for follow-through; escalates after STALL_HOURS.
                                _track_open_lead(state, sender_email, cid,
                                                 (first + " " + last).strip() or company,
                                                 company, reason)
                            else:
                                _bump(state, "daily_potential_count")
                        mark_row(sender_email, reply_status=label,
                                 last_result=f"{label}: {reason}"[:250])
                        if is_sql:
                            sql_count += 1
                        else:
                            potential_count += 1
                    else:
                        log.info(f"  Genuine reply from {redact_email(sender_email)} (not SQL) → Manae")
                        mark_row(sender_email, reply_status="Replied",
                                 last_result="replied (not SQL)")

                    full_name = (first + " " + last).strip()
                    # Export data sometimes duplicates the name across both columns
                    # ("Melody Melody") — don't hand that to Manae.
                    if first and last and first.lower() == last.lower():
                        full_name = first
                    lead_name = first or full_name       # prefer a first name if we have one
                    who       = lead_name or company or "A new contact"
                    them      = lead_name or "them"

                    # A short, human note to Manae — reads like a colleague passing along a
                    # lead. No AI-tells (no "suggested reply", no "Vida's read", no automated
                    # signature). Always includes the prospect's own words so Manae can see
                    # and respond to the original message without hunting for it. Manae only.
                    # Give Manae the full context — the prospect's message plus the quoted
                    # back-and-forth the reply carries (Chris: sales needs the whole exchange).
                    _msg = (body_text or "").strip()
                    _msg = _msg[:4000] + ("\n…(truncated)" if len(_msg) > 4000 else "")
                    quoted = f"Here's the full exchange:\n\n---\n{_msg}\n---\n\n"
                    # Manae replies straight out of this handoff, so the prospect's own
                    # address (plus phone/company when the signature gave us one) goes
                    # right under the intro — she asked for it 2026-08-11.
                    contact_lines = [f"Email: {sender_email}"] if sender_email else []
                    if phone:
                        contact_lines.append(f"Phone: {phone}")
                    if company:
                        contact_lines.append(f"Company: {company}")
                    contact_block = ("\n".join(contact_lines) + "\n\n") if contact_lines else ""
                    if interested:
                        # Only mention the HubSpot add when the write actually landed — never
                        # claim it happened if the write failed.
                        hs_line = ""
                        if hs_added:
                            hs_line = (
                                f"I've added {them} to HubSpot as a lead"
                                + (f" — {hs_link}" if hs_link else ".") + "\n\n"
                            )
                        fwd_subject = (
                            (f"ACTION: {full_name or company or 'New lead'} — "
                             f"asked about training (reply today)")
                            if is_sql else
                            (f"FYI: {full_name or company or 'New'} — warm reply, no ask yet")
                        )
                        opener = (
                            f"{who} asked us something a quote or a date would answer — "
                            f"worth a reply today.\n\n"
                            f"What they asked: {reason}\n\n"
                            f"There's a task on your HubSpot queue for this one, due in "
                            f"{TASK_DUE_MINUTES} minutes. If nothing is logged against them "
                            f"{STALL_HOURS} hours from now, Chris gets a note — that's a "
                            f"backstop on the system, not a stopwatch on you.\n\n"
                            if is_sql else
                            f"{who} replied warmly but hasn't asked for anything specific "
                            f"yet, so this is an FYI rather than a to-do. Filed as a "
                            f"marketing-qualified lead.\n\n"
                        )
                        fwd_body = (
                            f"Hi Manae,\n\n"
                            f"{opener}"
                            f"{contact_block}{hs_line}{quoted}"
                            f"Thanks!\n{SENDER_FIRST}"
                        )
                    else:
                        fwd_subject = f"{full_name or sender} replied — worth a look"
                        fwd_body = (
                            f"Hi Manae,\n\n"
                            f"{who} just replied to my outreach — no clear booking intent yet, "
                            f"but wanted to flag it for you.\n\n{contact_block}{quoted}"
                            f"Thanks!\n{SENDER_FIRST}"
                        )
                    if not dry_run:
                        send_email(MANAE_EMAIL, fwd_subject, fwd_body)
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
            f"  Reply check done: {genuine_count} genuine ({sql_count} SQL, "
            f"{potential_count} potential → HubSpot), "
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

    week_end = (pacific_today() - timedelta(days=1)).strftime("%b %d")
    subject = f"[Weekly Roster] {len(roster)} existing customers to follow up — week ending {week_end}"
    body = (
        f"Hi Manae,\n\n"
        f"Here are {len(roster)} existing Get CPR Done customers surfaced from this week's "
        f"outreach list. They were filtered out of the cold batch because they already have a "
        f"relationship with us — these are yours to follow up directly.\n\n"
        + "\n\n".join(rows) +
        f"\n\n—{SENDING_NAME} | {COMPANY_NAME} (via automated Outreach Agent)"
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

    # Mark that the daily send ran today, so the reply-check catch-up doesn't
    # re-trigger it (belt-and-suspenders against GitHub dropping the 9am cron).
    if not dry_run and state.get("last_daily_run") != today:
        state["last_daily_run"] = today
        save_state(state)

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

        # 1 — Source: follow-ups first (warmer + time-sensitive), then new contacts
        #     from the cursor to fill the rest of today's cap. Each carries a `touch`.
        followups = fetch_followups(svc, state, remaining)
        new_needed = remaining - len(followups)
        new_contacts = []
        new_cursor = int(state.get("sheet_cursor", 2) or 2)
        skipped_roles = []
        if new_needed > 0:
            new_contacts, new_cursor, skipped_roles = fetch_sheet_contacts(svc, state, new_needed)
        for c in new_contacts:
            c["touch"] = 1
        candidates = followups + new_contacts
        if not candidates:
            log.info("Nothing due: no follow-ups and no new contacts at the cursor.")
            if not dry_run:
                state["sheet_cursor"] = new_cursor
                save_state(state)
            if already_sent > 0:
                send_eod_report(already_sent, 0, [], [], skipped_roles, dry_run, state=state)
            return
        log.info(f"Queued {len(followups)} follow-ups + {len(new_contacts)} new = {len(candidates)}")

        # 2 — HubSpot cross-check: peel existing customers off to Manae. On an
        # unresolved lookup (API error after retries) SKIP rather than risk emailing a
        # customer. An unresolved NEW contact holds the cursor; follow-ups just retry
        # next run (they stay due).
        prospects = []
        customers = []
        skipped_new_rows = []
        for c in candidates:
            hs = check_hubspot(c)
            if hs.get("error"):
                if c.get("touch", 1) == 1:
                    skipped_new_rows.append(c["row"])
                continue
            if hs.get("isCustomer"):
                customers.append({"contact": c, "hubspot": hs})
                if not dry_run:
                    sheets_db.update_row(
                        svc, SPREADSHEET_ID, c["row"],
                        contacted=today, date_sent=today, reply_status="Customer",
                        last_result="existing customer → Manae")
                    _bump(state, "daily_customer_count")
            else:
                prospects.append(c)
        if customers and not dry_run:
            state["manae_roster_pending"] = state.get("manae_roster_pending", []) + customers
            save_state(state)
        log.info(f"Routing: {len(prospects)} prospects, {len(customers)} customers → Manae, "
                 f"{len(skipped_new_rows)} unresolved (HubSpot) → retry next run")

        # 3 — Generate emails in batches (each carries its touch number 1-4)
        to_send = prospects
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
            touch       = contact.get("touch", 1)
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

            log.info(f"Sending {i+1}/{len(to_send)} (row {row}, touch {touch}): {subject}")

            if dry_run:
                # Log the body too: a dry run is the only way to read what the generator
                # actually wrote (e.g. confirming a hospital gets BLS wording and a
                # preschool gets pediatric CPR), and the subject alone doesn't show it.
                log.info(f"  [DRY RUN] Would send touch {touch}. clean_first={clean_first!r}"
                         f"\n  --- to {contact.get('email','')} "
                         f"[{contact.get('company','')}] tags={contact.get('tags')}\n  "
                         + "\n  ".join((body or "").strip().splitlines()[:8]))
                continue

            plain_body, html_body = build_outreach_bodies(body)
            result = send_email(email, subject, plain_body, html=html_body)
            if result.get("success"):
                today_sent    += 1
                sent_this_run += 1
                sheets_db.update_row(
                    svc, SPREADSHEET_ID, row,
                    contacted=today, date_sent=today, reply_status="Contacted",
                    touches=touch, last_result=f"sent (touch {touch})", **name_updates)
                if touch == 1:
                    _bump(state, "daily_new_count")     # a newly-reached person
                log.info(f"  ✓ Sent touch {touch} ({today_sent}/{cap} today)")
            elif result.get("hard_bounce"):
                err = str(result.get("error", "unknown"))
                log.warning(f"  ✗ Hard bounce (row {row}): {err}")
                do_not_contact.add(email.lower())
                today_bounces.append(email)
                sheets_db.update_row(
                    svc, SPREADSHEET_ID, row,
                    contacted=today, date_sent=today, reply_status="Bounced",
                    do_not_contact="yes", touches=touch,
                    last_result=f"hard bounce: {err}"[:250], **name_updates)
                _bump(state, "daily_bounce_count")
            else:
                err = result.get("error", "unknown")
                log.error(f"  ✗ Failed (row {row}): {err}")
                notify_chris(f"Send failed (row {row})", err)

            daily_counts[today]       = today_sent
            state["daily_sent_count"] = daily_counts
            save_state(state)

            if i < len(to_send) - 1:
                time.sleep(random.randint(MIN_DELAY_SEC, MAX_DELAY_SEC))

        # Advance the cursor past new contacts consumed this run, holding it at the
        # first HubSpot-unresolved NEW row so those retry next run. Follow-ups don't
        # move the cursor — they're re-found by the follow-up scan.
        if not dry_run:
            resume_at = new_cursor
            if skipped_new_rows:
                resume_at = min(new_cursor, min(skipped_new_rows))
                log.info(f"  Cursor held at row {resume_at} to retry "
                         f"{len(skipped_new_rows)} unresolved new contact(s) next run")
            state["sheet_cursor"] = resume_at
            save_state(state)

        fu = sum(1 for c in to_send if c.get("touch", 1) > 1)
        log.info(f"Daily run complete. {sent_this_run} sent this run "
                 f"({today_sent} total today; {fu} were follow-ups).")
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
                "Replied": 3, "Potential SQL": 4, "SQL": 5, "Customer": 6}

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

# ─── Lead dashboard / report ──────────────────────────────────────────────────

def _sum_recent(counts_by_date, days):
    """Sum a {date_iso: n} dict over the last `days` days (inclusive of today)."""
    window = {(pacific_today() - timedelta(days=i)).isoformat() for i in range(days)}
    return sum(v for k, v in counts_by_date.items() if k in window)

def update_agent_performance(*, sent_today, replies_7d, sql, customers, replied,
                             contacted, today, table=None, open_leads=None):
    """Write Vida's slice into command-center/data/agent-performance.json so the CEO
    morning-briefing 'Agent Performance' strip stays live without hand-editing. No-op
    (logs + returns) if COMMAND_CENTER_TOKEN is unset, so local/dry runs are unaffected.
    Reads-modifies-writes just the agents.vida key via the GitHub Contents API."""
    token = os.environ.get("COMMAND_CENTER_TOKEN")
    if not token:
        log.info("agent-performance: COMMAND_CENTER_TOKEN unset — skipping slice update")
        return
    api = "https://api.github.com/repos/chris-joffe/chris-joffe-command-center/contents/data/agent-performance.json"

    def _req(method, data=None):
        req = urllib.request.Request(api, data=data, method=method)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("User-Agent", "vida-agent")
        with urllib.request.urlopen(req, timeout=30,
                                    context=ssl.create_default_context()) as resp:
            return json.loads(resp.read().decode())

    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    reply_rate = f"{(100.0 * replied / contacted):.1f}%" if contacted else "—"
    slice_data = {
        "name": SENDER_FIRST,
        "role": "GCD · Sales Development",
        "status": "healthy",
        "last_run": now,
        "headline": f"{sql} SQLs · {customers} customers routed",
        "kpis": [
            {"label": "Sent today", "value": f"{sent_today:,}"},
            {"label": "Replies 7d", "value": f"{replies_7d:,}"},
            {"label": "SQLs (life)", "value": str(sql), "tone": "good"},
            {"label": "→ Manae", "value": str(customers)},
        ],
        "note": f"Reply rate {reply_rate} · {contacted:,} reached lifetime",
        # Detailed Today / 7-day / Lifetime table for the combined EOD email.
        "table": table or [],
        # Handed-over leads with nothing logged against them yet. Surfaced in the morning
        # briefing and the combined EOD email so a stalled lead is visible without anyone
        # remembering to look (Chris, 2026-08-13).
        "open_leads": open_leads or [],
    }
    # Read-modify-write with retry on 409: multiple SDR agents write this same file (Elena +
    # Joffe report in the same minute), so a stale-SHA conflict is expected — re-read and retry.
    for attempt in range(5):
        try:
            cur = _req("GET")
            doc = json.loads(base64.b64decode(cur["content"]).decode())
            sha = cur["sha"]
        except Exception as e:  # never let telemetry break the report
            log.error(f"agent-performance: read failed — {e}")
            return
        doc.setdefault("agents", {})[AGENT_KEY] = slice_data
        doc["generated_at"] = now
        payload = json.dumps({
            "message": f"agent-performance: {SENDER_FIRST} slice {today}",
            "content": base64.b64encode(json.dumps(doc, indent=2).encode()).decode(),
            "sha": sha,
        }).encode()
        try:
            _req("PUT", payload)
            log.info(f"agent-performance: {SENDER_FIRST} slice updated")
            return
        except urllib.error.HTTPError as e:
            if e.code == 409 and attempt < 4:
                time.sleep(1 + attempt)   # someone else wrote first — re-read fresh SHA, retry
                continue
            log.error(f"agent-performance: write failed — {e}")
            return
        except Exception as e:
            log.error(f"agent-performance: write failed — {e}")
            return


def run_report(dry_run=False):
    """Email Chris a lead dashboard: outbound volume, replies, SQLs, deliverability —
    lifetime + last-7-days + today. Reads live status from the sheet + counters from state."""
    from collections import Counter
    today = today_str()
    state = load_state()
    # Idempotency: at most one real dashboard per Pacific day. Belt-and-suspenders with the
    # single 6-7pm trigger + workflow concurrency, so an accidental re-dispatch can't double-send.
    if not dry_run and state.get("last_report_run") == today:
        log.info(f"Dashboard already sent today ({today}) — skipping duplicate.")
        return
    if not dry_run:
        state["last_report_run"] = today
        save_state(state)
    svc = _sheet_service()
    # Widen/label the managed columns before the snapshot read — Sheets errors on a read
    # that runs past the grid edge, and the snapshot now reaches column R.
    sheets_db.ensure_headers(svc, SPREADSHEET_ID)
    log.info("Report: reading sheet status snapshot...")
    snap = sheets_db.snapshot_status(svc, SPREADSHEET_ID)
    # Attribute rows to THIS agent: its partition AND date_sent >= its launch date. The row
    # range keeps Vida/Elena from double-counting the shared sheet; the date filter strips
    # pre-agent Mailchimp legacy (statuses/dates that predate the agent), so lifetime metrics
    # reflect only what this agent actually did.
    snap = {e: v for e, v in snap.items()
            if v.get("row", 0) >= ROW_RANGE_START
            and (ROW_RANGE_END is None or v.get("row", 0) <= ROW_RANGE_END)
            and (v.get("date_sent", "") or "")[:10] >= AGENT_LAUNCH_DATE}
    counts = Counter(v["reply_status"] for v in snap.values() if v.get("reply_status"))

    contacted = sum(counts.values())               # distinct people with any status set
    sql       = counts.get("SQL", 0)
    potential = counts.get("Potential SQL", 0)      # warm reply, no ask yet (MQL)
    replied   = counts.get("Replied", 0) + sql + potential   # all are interested replies
    bounced   = counts.get("Bounced", 0)
    unsub     = counts.get("Unsubscribed", 0)
    customers = counts.get("Customer", 0)
    awaiting  = counts.get("Contacted", 0)
    # Name each SQL, not just its address, and say when it came in — this list is what
    # Chris forwards to sales to confirm follow-up (Chris, 2026-08-11).
    def _label(email, v):
        f, l = _clean_name(v.get("first", "")), _clean_name(v.get("last", ""))
        if f and l and f.lower() == l.lower():
            l = ""
        nm = (f + " " + l).strip()
        when = (v.get("reply_date") or "")[:10]
        return f"{nm + ' — ' if nm else ''}{email}{f' (in {when})' if when else ''}"
    sql_rows  = {e: v for e, v in snap.items() if v.get("reply_status") == "SQL"}
    sql_list  = [_label(e, v) for e, v in
                 sorted(sql_rows.items(), key=lambda kv: (kv[1].get("reply_date") or "", kv[0]),
                        reverse=True)]

    # Reply-day counts read off the sheet's reply_date (column R) — the authoritative
    # answer to "what came in today", since date_sent only records when we emailed out.
    # The state counters remain the fallback for days before column R existed, and for
    # any row whose stamp is missing; take the larger so a partial day never reads low.
    def _by_reply_day(status):
        return Counter((v.get("reply_date") or "")[:10] for v in snap.values()
                       if v.get("reply_status") == status and v.get("reply_date"))
    sql_days = _by_reply_day("SQL")
    pot_days = _by_reply_day("Potential SQL")

    dsc = state.get("daily_sent_count", {})
    drc = state.get("daily_reply_count", {})
    nc  = state.get("daily_new_count", {})
    sqc = state.get("daily_sql_count", {})
    bc  = state.get("daily_bounce_count", {})
    uc  = state.get("daily_unsub_count", {})
    cc  = state.get("daily_customer_count", {})
    sent_total = sum(dsc.values())
    sql_today = max(sql_days.get(today, 0), sqc.get(today, 0))
    sql_7d    = max(_sum_recent(sql_days, 7), _sum_recent(sqc, 7))
    pqc       = state.get("daily_potential_count", {})
    pot_today = max(pot_days.get(today, 0), pqc.get(today, 0))
    pot_7d    = max(_sum_recent(pot_days, 7), _sum_recent(pqc, 7))
    # Leads handed over and still showing no logged follow-up — the number Chris actually
    # wants to see every morning.
    open_leads = [l for l in state.get("open_leads", []) if not l.get("closed")]
    def _age_h(l):
        try:
            return (pacific_now() - datetime.fromisoformat(l["handed_at"])).total_seconds() / 3600.0
        except Exception:
            return 0.0
    open_leads = sorted(open_leads, key=_age_h, reverse=True)
    open_lines = [f"{l.get('name') or l.get('company') or 'Lead'} — {l['email']} "
                  f"({_age_h(l):.0f}h, {l.get('why','')[:48]})" for l in open_leads]

    def pct(n, d):
        return f"{(100.0 * n / d):.1f}%" if d else "—"

    def r(label, t, w, life):
        return f"  {label:<26}{t:>8,}{w:>11,}{life:>13,}\n"

    subject = (f"[{SENDER_FIRST} Lead Dashboard] {today} — {sql_today} SQLs today "
               f"({sql} lifetime), {dsc.get(today, 0):,} sent")
    body = (
        f"{SENDER_FIRST} — {COMPANY_NAME} outreach dashboard\n"
        f"As of {today}\n"
        f"{'=' * 60}\n\n"
        f"  {'':<26}{'TODAY':>8}{'7 DAYS':>11}{'LIFETIME':>13}\n"
        f"  {'-' * 58}\n"
        + r("Emails sent (all touches)", dsc.get(today, 0), _sum_recent(dsc, 7), sent_total)
        + r("New people reached",        nc.get(today, 0),  _sum_recent(nc, 7),  contacted)
        + r("Replies",                   drc.get(today, 0), _sum_recent(drc, 7), replied)
        + r("Sales-Qualified Leads",     sql_today, sql_7d, sql)
        + r("Potential SQLs (warm)",      pot_today, pot_7d, potential)
        + r("Existing cust -> Manae",    cc.get(today, 0),  _sum_recent(cc, 7),  customers)
        + r("Hard bounces",              bc.get(today, 0),  _sum_recent(bc, 7),  bounced)
        + r("Unsubscribes",              uc.get(today, 0),  _sum_recent(uc, 7),  unsub)
        + (f"\n  AWAITING FOLLOW-UP ({len(open_lines)}) — handed over, nothing logged yet:\n"
           + "\n".join(f"    - {x}" for x in open_lines) + "\n" if open_lines else "")
        + f"\n  Currently awaiting reply:  {awaiting:,}\n"
        + f"  Lifetime reply rate: {pct(replied, contacted)}   "
          f"SQL rate: {pct(sql, contacted)}\n\n"
        + f"SALES-QUALIFIED LEADS ({len(sql_list)}) — confirm Manae has reached out:\n"
        + (("\n".join(f"  - {e}" for e in sql_list)) if sql_list else "  (none yet)")
        + "\n\n"
        f"Note: SQL counts come from the sheet's reply_date column, so TODAY / 7-DAY /\n"
        f"LIFETIME agree. The per-day counters for reached / bounces / unsubscribes began\n"
        f"after launch, so those TODAY and 7-DAY columns can read low against LIFETIME,\n"
        f"which is exact (live tracking sheet).\n"
        f"\n—{SENDER_FIRST} (automated). Full per-contact status is in the tracking sheet.\n"
    )

    # HTML version (a real table so the columns hold their shape in email clients)
    def hrow(label, t, w, life, hi=False):
        bg = " background:#f7fbff;" if hi else ""
        return (f"<tr style='border-top:1px solid #e5e5e5;{bg}'>"
                f"<td style='padding:6px 14px'>{label}</td>"
                f"<td align='right' style='padding:6px 14px'>{t:,}</td>"
                f"<td align='right' style='padding:6px 14px'>{w:,}</td>"
                f"<td align='right' style='padding:6px 14px;font-weight:600'>{life:,}</td></tr>")
    html = (
        "<div style='font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#222'>"
        f"<h2 style='margin:0 0 2px'>{SENDER_FIRST} — {COMPANY_NAME} outreach dashboard</h2>"
        f"<div style='color:#888;margin-bottom:14px'>As of {today}</div>"
        "<table style='border-collapse:collapse;font-size:14px;min-width:460px'>"
        "<thead><tr style='background:#efefef'>"
        "<th align='left' style='padding:8px 14px'>&nbsp;</th>"
        "<th align='right' style='padding:8px 14px'>Today</th>"
        "<th align='right' style='padding:8px 14px'>7 days</th>"
        "<th align='right' style='padding:8px 14px'>Lifetime</th>"
        "</tr></thead><tbody>"
        + hrow("Emails sent (all touches)", dsc.get(today, 0), _sum_recent(dsc, 7), sent_total)
        + hrow("New people reached", nc.get(today, 0), _sum_recent(nc, 7), contacted)
        + hrow("Replies", drc.get(today, 0), _sum_recent(drc, 7), replied)
        + hrow("Sales-Qualified Leads", sql_today, sql_7d, sql, hi=True)
        + hrow("Potential SQLs (warm)", pot_today, pot_7d, potential)
        + hrow("Existing customers &rarr; Manae", cc.get(today, 0), _sum_recent(cc, 7), customers)
        + hrow("Hard bounces", bc.get(today, 0), _sum_recent(bc, 7), bounced)
        + hrow("Unsubscribes", uc.get(today, 0), _sum_recent(uc, 7), unsub)
        + "</tbody></table>"
        + (f"<p style='margin:14px 0 4px'><b>Awaiting follow-up ({len(open_lines)})</b> "
           f"&mdash; handed over, nothing logged against them yet:</p>"
           "<ul style='margin:4px 0 0;padding-left:22px'>"
           + "".join(f"<li>{x}</li>" for x in open_lines) + "</ul>" if open_lines else "")
        + f"<p style='margin:14px 0 4px'><b>Currently awaiting reply:</b> {awaiting:,}<br>"
        + f"<b>Lifetime reply rate:</b> {pct(replied, contacted)} &nbsp;&nbsp; "
        + f"<b>SQL rate:</b> {pct(sql, contacted)}</p>"
        + f"<p style='margin:14px 0 4px'><b>Sales-Qualified Leads ({len(sql_list)})</b> "
        + f"&mdash; confirm Manae has reached out:</p>"
        + ("<ul style='margin:4px 0 0;padding-left:22px'>"
           + "".join(f"<li>{e}</li>" for e in sql_list) + "</ul>"
           if sql_list else "<p style='color:#888'>(none yet)</p>")
        + f"<p style='color:#999;font-size:12px;max-width:560px'>SQL counts come from the "
        f"tracking sheet's reply_date, so Today / 7-day / Lifetime all agree. Per-day counters "
        f"for reached / bounces / unsubscribes began later than launch, so those Today &amp; "
        f"7-day columns can read low against Lifetime, which is exact (live tracking sheet).</p>"
        "</div>"
    )

    # Always keep the CEO briefing's Agent Performance strip live + feed the combined EOD
    # email (best-effort; never fatal). This is the reason Vida's report still runs daily.
    # Today/7d come from event-counters (dated), Lifetime from the attributed sheet (current
    # status). Enforce the logical invariant today <= 7d <= lifetime so a counter that logs
    # more events than persist as status (e.g. a bounce backlog) can't read as "7d > lifetime".
    def _pt(label, today_v, week_v, life_v):
        week_v = min(week_v, life_v)
        return [label, min(today_v, week_v), week_v, life_v]
    perf_table = [
        _pt("Emails sent (all touches)", dsc.get(today, 0), _sum_recent(dsc, 7), sent_total),
        _pt("New people reached", nc.get(today, 0),  _sum_recent(nc, 7),  contacted),
        _pt("Replies",        drc.get(today, 0), _sum_recent(drc, 7), replied),
        _pt("SQLs",           sql_today, sql_7d, sql),
        _pt("Potential SQLs", pot_today, pot_7d, potential),
        _pt("Cust → Manae",   cc.get(today, 0),  _sum_recent(cc, 7),  customers),
        _pt("Hard bounces",   bc.get(today, 0),  _sum_recent(bc, 7),  bounced),
        _pt("Unsubscribes",   uc.get(today, 0),  _sum_recent(uc, 7),  unsub),
    ]
    update_agent_performance(
        sent_today=dsc.get(today, 0), replies_7d=_sum_recent(drc, 7), sql=sql,
        customers=customers, replied=replied, contacted=contacted, today=today,
        table=perf_table,
        open_leads=[{"name": l.get("name") or l.get("company") or "Lead",
                     "email": l["email"], "why": l.get("why", ""),
                     "hours": round(_age_h(l)), "link": _hubspot_link(l.get("cid")),
                     "alerted": bool(l.get("alerted"))} for l in open_leads],
    )

    if dry_run:
        log.info("[DRY RUN] Lead dashboard:\n" + body)
        return
    # Vida no longer emails her own dashboard to Chris — his EOD view is the single combined
    # Vida+Elena+Joffe email (mode=combined_report). Set REPORT_EMAIL_ENABLED=1 to re-enable.
    if os.environ.get("REPORT_EMAIL_ENABLED") == "1":
        result = send_email(REPORT_EMAIL, subject, body, html=html)
        if result.get("success"):
            log.info(f"Lead dashboard emailed to {REPORT_EMAIL} ({sql} SQLs, {replied} replies)")
        else:
            log.error(f"Report send failed: {result.get('error')}")
    else:
        log.info("Standalone Vida dashboard suppressed (combined SDR email handles Chris's view); slice written.")

# ─── Entry point ──────────────────────────────────────────────────────────────

# ─── Combined all-SDR EOD email ───────────────────────────────────────────────

# Which command-center agent slices are SDRs, grouped by company for the combined email.
# (agent-performance.json also holds non-SDR agents like miles/sage — excluded here.)
SDR_GROUPS = [
    ("Get CPR Done", ["vida", "elena"]),
    ("Joffe School Safety", ["joffe"]),
]

def run_combined_report(dry_run=False):
    """Email Chris ONE combined HTML dashboard covering every SDR (Vida, Elena, Jessica &
    Ryan), grouped by company. Reads each agent's slice from command-center's
    agent-performance.json — which every SDR keeps fresh via its own report run — so this is
    a pure render+send with no per-agent data access. Chris's single end-of-day view."""
    token = os.environ.get("COMMAND_CENTER_TOKEN")
    if not token:
        log.error("combined_report: COMMAND_CENTER_TOKEN unset — cannot read agent slices")
        return
    api = "https://api.github.com/repos/chris-joffe/chris-joffe-command-center/contents/data/agent-performance.json"
    try:
        req = urllib.request.Request(api, headers={
            "Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
            "User-Agent": "vida-combined-report"})
        with urllib.request.urlopen(req, timeout=30, context=ssl_ctx()) as r:
            doc = json.loads(base64.b64decode(json.loads(r.read().decode())["content"]).decode())
    except Exception as e:
        log.error(f"combined_report: read failed — {e}")
        return
    agents = doc.get("agents", {})
    today = today_str()

    def card(a):
        dot = {"healthy": "#1a8f4c", "paused": "#b9911f"}.get(a.get("status"), "#999")
        tbl = a.get("table") or []
        if tbl:
            th = ("<tr style='background:#f4f4f4'>"
                  "<th align='left' style='padding:5px 12px;font:600 11px Arial;color:#666;text-transform:uppercase;letter-spacing:.4px'>Metric</th>"
                  "<th align='right' style='padding:5px 12px;font:600 11px Arial;color:#666'>Today</th>"
                  "<th align='right' style='padding:5px 12px;font:600 11px Arial;color:#666'>7 days</th>"
                  "<th align='right' style='padding:5px 12px;font:600 11px Arial;color:#666'>Lifetime</th></tr>")
            trs = "".join(
                f"<tr style='border-top:1px solid #eee'>"
                f"<td style='padding:5px 12px;font-size:13px;color:#333'>{row[0]}</td>"
                f"<td align='right' style='padding:5px 12px;font-size:13px'>{row[1]:,}</td>"
                f"<td align='right' style='padding:5px 12px;font-size:13px'>{row[2]:,}</td>"
                f"<td align='right' style='padding:5px 12px;font-size:13px;font-weight:600'>{row[3]:,}</td></tr>"
                for row in tbl)
            detail = (f"<table style='border-collapse:collapse;width:100%;margin-top:8px'>{th}{trs}</table>")
        else:
            # Fallback to summary KPIs if a slice predates the table field.
            detail = "".join(
                f"<span style='display:inline-block;margin:4px 14px 0 0'>"
                f"<b style='font-size:15px'>{k.get('value','—')}</b> "
                f"<span style='color:#888;font-size:12px'>{k.get('label','')}</span></span>"
                for k in a.get("kpis", []))
        # Leads handed to sales with nothing logged against them yet — the part of this
        # email Chris is meant to act on, so it sits directly under the numbers.
        open_leads = a.get("open_leads") or []
        if open_leads:
            def _lead_li(l):
                nm = l.get("name") or "Lead"
                link = l.get("link") or ""
                title = (f"<a href='{link}' style='color:#b1442e;text-decoration:none'>{nm}</a>"
                         if link else nm)
                flag = ", escalated" if l.get("alerted") else ""
                return (f"<li style='margin:2px 0'>{title}"
                        f" <span style='color:#666'>&mdash; {l.get('email','')}</span>"
                        f" <span style='color:#999'>({int(l.get('hours',0))}h{flag})</span>"
                        f"<div style='color:#888;font-size:12px;margin-left:2px'>"
                        f"{l.get('why','')}</div></li>")
            items = "".join(_lead_li(l) for l in open_leads[:12])
            waiting = (
                f"<div style='margin-top:10px;padding:8px 10px;border-left:3px solid #b1442e;"
                f"background:#fdf6f4'>"
                f"<div style='font-size:13px;font-weight:600;color:#b1442e'>"
                f"Awaiting follow-up ({len(open_leads)})</div>"
                f"<ul style='margin:4px 0 0;padding-left:18px;font-size:13px'>{items}</ul></div>")
        else:
            waiting = ""
        return (
            f"<div style='border:1px solid #e6e6e6;border-radius:10px;padding:14px 16px;margin:0 0 12px'>"
            f"<div style='font-size:15px;font-weight:700;color:#111'>"
            f"<span style='display:inline-block;width:8px;height:8px;border-radius:50%;background:{dot};margin-right:8px'></span>"
            f"{a.get('name','?')}</div>"
            f"<div style='color:#666;font-size:13px;margin:2px 0 4px'>{a.get('headline','')}</div>"
            f"{detail}{waiting}"
            f"<div style='color:#999;font-size:12px;margin-top:8px'>{a.get('note','')}</div>"
            f"</div>")

    sections, txt = "", []
    for company, keys in SDR_GROUPS:
        cards = [card(agents[k]) for k in keys if k in agents]
        if not cards:
            continue
        sections += (f"<h3 style='font:600 13px Arial;color:#888;text-transform:uppercase;"
                     f"letter-spacing:.6px;margin:18px 0 8px'>{company}</h3>" + "".join(cards))
        for k in keys:
            if k in agents:
                a = agents[k]
                txt.append(f"\n  {a.get('name')} — {a.get('headline')}")
                txt.append(f"    {'Metric':<16}{'Today':>8}{'7d':>9}{'Life':>11}")
                for row in (a.get("table") or []):
                    txt.append(f"    {row[0]:<16}{row[1]:>8,}{row[2]:>9,}{row[3]:>11,}")
    if not sections:
        log.info("combined_report: no SDR slices found — nothing to send.")
        return

    subject = f"[SDR Daily] {today} — Vida, Elena & Joffe outreach"
    html = (f"<div style='font-family:Arial,Helvetica,sans-serif;max-width:620px'>"
            f"<h2 style='margin:0 0 2px;color:#111'>SDR Daily — all outreach</h2>"
            f"<div style='color:#888;font-size:13px;margin-bottom:6px'>As of {today}</div>"
            f"{sections}"
            f"<p style='color:#aaa;font-size:11px;margin-top:16px'>One combined view of every SDR. "
            f"Per-agent detail lives on each command-center tile.</p></div>")
    body = f"SDR Daily — {today}\n\n" + "\n".join(txt) + "\n\n—Combined SDR report (automated)"

    if dry_run:
        log.info("[DRY RUN] combined SDR email:\n" + body)
        return
    result = send_email(REPORT_EMAIL, subject, body, html=html)
    log.info(f"Combined SDR email sent to {REPORT_EMAIL}" if result.get("success")
             else f"Combined SDR email failed: {result.get('error')}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["daily","roster","reply_check","stall_check",
                                           "reconcile","report","combined_report"], required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force-weekday", action="store_true")
    parser.add_argument("--force", action="store_true")  # alias
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap sends this run (for a small first live test). Also bounded by BATCH_SIZE.")
    args = parser.parse_args()

    # Acquire exclusive lock — exits immediately if another instance is running
    _lock_fh = _acquire_lock()

    # Outbound sending stays weekdays-only, but reading replies and watching handed-over
    # leads now runs every day: a Friday-afternoon reply used to sit until Monday 9am
    # before Manae even heard about it (Chris, 2026-08-13).
    WEEKDAY_ONLY_MODES = {"daily", "roster"}
    if (args.mode in WEEKDAY_ONLY_MODES and not args.force_weekday and not args.force
            and not is_weekday()):
        log.info(f"Today is {pacific_today().strftime('%A')} — {args.mode} only runs M–F. Exiting.")
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
        # Safety net on handed-over leads — cheap (one HubSpot read per open lead) and it
        # runs on the same cadence as the reply check, including weekends.
        check_stalled_leads(load_state(), dry_run=args.dry_run)
        # Self-healing: GitHub occasionally drops a scheduled run entirely. reply_check
        # fires several times a day, so use it to catch up the daily send + dashboard if
        # their own cron didn't fire today. Both self-guard (daily cap + last_*_run date),
        # so this is safe to attempt on every reply-check.
        if not args.dry_run:
            if is_weekday() and load_state().get("last_daily_run") != today_str():
                log.info("Catch-up: daily send hasn't run today — running it now.")
                run_daily()
            # NOTE: the dashboard/report is no longer self-healed here. It now has a
            # single reliable trigger — cjclaude-intake pings workflow_dispatch mode=report
            # at 6pm PT. reply_check ends at 5pm PT so it could never back up a 6pm report
            # anyway, and the old catch-up + the report cron were double-sending. One trigger
            # = one email. (Backstop: the morning briefing's Vida tile shows a missed day.)
    elif args.mode == "stall_check":
        check_stalled_leads(state, dry_run=args.dry_run)
    elif args.mode == "reconcile":
        run_reconcile(dry_run=args.dry_run)
    elif args.mode == "report":
        run_report(dry_run=args.dry_run)
    elif args.mode == "combined_report":
        run_combined_report(dry_run=args.dry_run)

    log.info(f"=== Complete | {datetime.now().isoformat()} ===\n")

if __name__ == "__main__":
    main()
