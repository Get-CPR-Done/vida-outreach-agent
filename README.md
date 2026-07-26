# Get CPR Done — Outreach Agent
## Setup & Scheduling Guide

---

## What This Does

| Mode | When | What happens |
|------|------|-------------|
| `daily` | M–F 9:00 AM | Pulls 100 Mailchimp contacts, HubSpot cross-checks, sends personalized emails from Vida |
| `roster` | Monday 9:05 AM | Emails Manae the week's accumulated customer list |
| `reply_check` | M–F every 2 hrs (9AM–6PM) | Checks Gmail for replies → forwards to Vida, CCs Manae |

On any failure → Chris gets an email automatically.

---

## Step 1 — Install

```bash
# Create a virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate          # Mac/Linux
# venv\Scripts\activate           # Windows

# Install dependencies
pip install -r requirements.txt
```

---

## Step 2 — Set Your API Key

The script uses the Anthropic Python SDK, which reads your API key from the environment.

**Mac/Linux — add to your shell profile (~/.zshrc or ~/.bashrc):**
```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```
Then: `source ~/.zshrc`

**Windows — set as a system environment variable:**
```
System Properties → Environment Variables → New
Name:  ANTHROPIC_API_KEY
Value: sk-ant-...
```

---

## Step 3 — Test It (Dry Run)

Always test before scheduling. Dry run generates emails but sends nothing:

```bash
python outreach_agent.py --mode daily --dry-run
python outreach_agent.py --mode roster --dry-run --force-weekday
python outreach_agent.py --mode reply_check --dry-run
```

Check `outreach.log` in the same folder to see exactly what it would have done.

---

## Step 4 — Schedule It

### Option A: Mac/Linux — cron

Open your crontab:
```bash
crontab -e
```

Add these lines (adjust the path to wherever you saved the script):
```cron
# Activate venv and run daily batch — M–F at 9:00 AM
0 9 * * 1-5 cd /path/to/outreach_agent && /path/to/outreach_agent/venv/bin/python outreach_agent.py --mode daily >> outreach.log 2>&1

# Send Manae's roster — Monday only at 9:05 AM
5 9 * * 1 cd /path/to/outreach_agent && /path/to/outreach_agent/venv/bin/python outreach_agent.py --mode roster >> outreach.log 2>&1

# Reply check — M–F every 2 hours from 9AM to 6PM
0 9,11,13,15,17 * * 1-5 cd /path/to/outreach_agent && /path/to/outreach_agent/venv/bin/python outreach_agent.py --mode reply_check >> outreach.log 2>&1
```

Save and exit. Verify with: `crontab -l`

**Important:** cron doesn't load your shell profile, so you need to either:
- Add `ANTHROPIC_API_KEY=sk-ant-...` at the top of your crontab (before any job lines), OR
- Use the full path to a wrapper script that exports it first

```cron
# At the top of your crontab:
ANTHROPIC_API_KEY=sk-ant-your-key-here
```

---

### Option B: Windows — Task Scheduler

Create 3 tasks. For each:

1. Open **Task Scheduler** → **Create Basic Task**
2. Set the trigger:
   - Daily batch: Daily, 9:00 AM, repeat M–F (set "Run only if logged on" → off, enable "Run whether user is logged on or not")
   - Roster: Weekly, Monday, 9:05 AM
   - Reply check: Daily, starting 9:00 AM, repeat every 2 hours for 8 hours, M–F
3. Action: **Start a program**
   - Program: `C:\path\to\outreach_agent\venv\Scripts\python.exe`
   - Arguments: `outreach_agent.py --mode daily` (change per task)
   - Start in: `C:\path\to\outreach_agent\`
4. Under **Conditions**: uncheck "Start only if on AC power" if on a laptop
5. Under **Settings**: check "Run task as soon as possible after a scheduled start is missed"

Set your API key as a System environment variable (see Step 2) so Task Scheduler can see it.

---

### Option C: Claude Cowork (Mac Desktop)

If you're using the Claude desktop app with Cowork:

1. Open Cowork → **New Automation**
2. Set schedule: Daily weekdays 9:00 AM
3. Command: `python /path/to/outreach_agent/outreach_agent.py --mode daily`
4. Repeat for roster (Monday 9:05 AM) and reply_check (every 2 hrs weekdays)

Cowork will handle the venv activation if you point it at the venv Python binary.

---

## File Structure

```
outreach_agent/
├── outreach_agent.py     ← main script
├── requirements.txt      ← pip dependencies
├── state.json            ← auto-created: tracks sent emails, roster, thread IDs
├── outreach.log          ← auto-created: full activity log
└── README.md             ← this file
```

**state.json** is the agent's memory. It persists between runs and tracks:
- Every email address ever contacted (prevents repeat sends)
- Gmail thread IDs for reply monitoring
- Customers accumulated in Manae's pending roster
- Daily send counts (enforces the 100/day cap)
- Date of last roster send (prevents duplicate Monday sends)

Don't delete state.json unless you want to reset everything.

---

## Recommended Schedule Summary

| Time | Mode | Days |
|------|------|------|
| 9:00 AM | `daily` | M–F |
| 9:05 AM | `roster` | Monday only |
| 9:00 AM, 11:00 AM, 1:00 PM, 3:00 PM, 5:00 PM | `reply_check` | M–F |

The 5-minute gap between `daily` and `roster` on Monday ensures the daily run has started logging contacts before the roster fires.

---

## Failure Notifications

Any uncaught error automatically emails **Chris (chris@getcprdone.com)** with:
- The error message
- Context about what was happening
- Path to the log file

The script exits with code 1 on fatal errors, which most schedulers will flag as a failed run.

---

## Resetting / Maintenance

**Prune old thread IDs** (optional, monthly): The sent_thread_ids list grows over time. Gmail only keeps threads for a limited window anyway, so you can safely clear it:
```bash
# In Python or a one-liner:
python -c "import json; s=json.load(open('state.json')); s['sent_thread_ids']=[]; json.dump(s,open('state.json','w'),indent=2)"
```

**View today's send count:**
```bash
python -c "import json,datetime; s=json.load(open('state.json')); print(s['daily_sent_count'].get(str(datetime.date.today()),0),'sent today')"
```

**Force a test roster send (any day):**
```bash
python outreach_agent.py --mode roster --force-weekday --dry-run
```
