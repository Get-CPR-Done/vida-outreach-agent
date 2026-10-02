"""
campaigns.py — fixed-copy campaigns that run alongside Vida's generated outreach.
================================================================================
A campaign is a block of rows appended to the shared sheet AFTER every partition (so
neither agent's cursor ever reaches it) and handed to one agent via CAMPAIGN_ROWS.
Touch 1 is Chris's copy, word for word, picked by a variant token in the row's TAGS;
touches 2-4 go back to the generator with the campaign's segment framing.

AB 310 youth sports (Chris, 2026-10-02): 1,447 California league contacts scraped from
public league/association/city pages, ZeroBounce-verified (188 suppressed in the sheet,
role inboxes kept — president@/safety@ ARE the decision makers at a volunteer league).
Variants by role: v1 president / commissioner / board, v2 safety director / CVPA, v3
city parks & rec. Signed by Vida, not Chris: the From line is Vida's and so is the inbox
that reads the replies. V2/V3 originally offered "a one-page checklist" / "a sample
plan"; those don't exist as documents and each league's plan is different, so they now
offer a discovery call instead (Chris, 2026-10-02).

Facts checked against the chaptered bill and Cal North's summary on 2026-10-02: coaches
CPR/AED certified by 2027-01-01 and every two years after; written cardiac emergency
response plan by 2027-01-01 with an annual electronic communication to parents; AED
access at official practices and matches from 2028-01-01.
"""

SIGNATURE = "Vida Monroe\nGet CPR Done, in partnership with Joffe"
POSTAL = "Get CPR Done · 2052 Bundy Drive #1014, Los Angeles, CA 90025"

AB310 = {
    "v1": {
        "subject": "{org} + AB 310",
        "body": (
            "Hi {first},\n\n"
            "Quick heads-up in case it's not on the board's radar yet: under California's "
            "AB 310, every {org} coach needs current CPR/AED certification by January 1, "
            "2027, and the league needs a written cardiac emergency response plan shared "
            "with parents each year.\n\n"
            "That's a lot to land on a volunteer board. We'd like to make it the easiest "
            "item on your list.\n\n"
            "Get CPR Done has trained staff at thousands of schools across California, and "
            "we partner with Joffe, a child-centered safety firm that has spent 15+ years "
            "helping schools and communities prepare for emergencies. Together we can:\n\n"
            "- Certify your whole coaching staff in one on-site session (at your field, "
            "clubhouse or a coaches' meeting night)\n"
            "- Help you write the required emergency response plan, so you aren't starting "
            "from a blank page\n"
            "- Remind you before the 2-year recertification comes due\n\n"
            "If you reply with roughly how many coaches you have, I'll send back a quote and "
            "a few open dates before the season fills up."
        ),
        "footer": "Not the right person, or rather not hear from us? Just reply \"no\" and "
                  "we won't reach out again.",
    },
    "v2": {
        "subject": "AB 310 checklist for {org}",
        "body": (
            "Hi {first},\n\n"
            "Since you oversee safety at {org}, you're probably the one AB 310 lands on. By "
            "January 1, 2027, every coach needs CPR/AED certification, and the league needs "
            "a written cardiac emergency response plan with annual parent notification. AEDs "
            "at practices and games follow in 2028.\n\n"
            "We help leagues knock this out in one step. Get CPR Done has trained staff at "
            "thousands of California schools, and together with our partner Joffe, a "
            "child-centered safety firm, we:\n\n"
            "- Run one on-site class that certifies your coaches together\n"
            "- Build your cardiac emergency response plan with you\n"
            "- Keep track of the 2-year renewals so you stay compliant\n\n"
            "Every league's fields, schedule and volunteers are different, so we build the "
            "checklist and plan around yours rather than send a generic one. Would a short "
            "discovery call be useful to figure out what {org} actually needs? Grab any "
            "time on my meeting link, or reply with how many coaches you have and I'll send "
            "a quote."
        ),
        "footer": "Reply \"no\" anytime and we won't email again.",
    },
    "v3": {
        "subject": "AB 310 for {org} youth sports",
        "body": (
            "Hi {first},\n\n"
            "AB 310 applies to city and county youth sports programs too. By January 1, "
            "2027, coaches in {org}'s leagues need CPR/AED certification, and each program "
            "needs a written cardiac emergency response plan.\n\n"
            "Get CPR Done has trained staff at thousands of California schools, and we "
            "partner with Joffe, a child-centered safety firm with 15+ years in school and "
            "community safety. For public agencies, we can:\n\n"
            "- Certify coaches and rec staff on-site in group sessions, at your facility\n"
            "- Help draft an emergency response plan your programs can share\n"
            "- Handle the 2-year recertification cycle and provide documentation for your "
            "records\n\n"
            "Who's the best person to talk to about this for your department? If it's you, "
            "every program's plan ends up a little different, so we build ours around yours "
            "rather than send a template. A short discovery call is the quickest way to "
            "figure out what fits; my meeting link has a few open times."
        ),
        "footer": "Reply \"no\" and we won't reach out again.",
    },
}

# Framing the generator uses for touches 2-4 (and anything else tagged youth-sports).
YOUTH_SPORTS_SEGMENT = (
    "California youth sports league or program (usually run by a volunteer board)",
    "CPR/AED certification for every coach, which California's AB 310 requires by "
    "January 1, 2027 and every two years after",
    "the AB 310 deadline landing on a volunteer board; certifying all the coaches in one "
    "on-site session at the field or a coaches' meeting night; and the written cardiac "
    "emergency response plan the league must also have by January 1, 2027 and send to "
    "parents every year (we help write it, with our partner Joffe). AEDs at practices and "
    "games follow in 2028. State only these facts about the law; never invent penalties, "
    "fines or other requirements",
)


def variant_of(tags):
    toks = {t.strip().lower() for t in (tags or [])}
    for v in ("v3", "v2", "v1"):
        if v in toks:
            return v
    return "v1"


def is_campaign_row(tags):
    return "ab310" in {t.strip().lower() for t in (tags or [])}


def render_ab310(contact):
    """Touch-1 copy for an AB 310 row. Returns (subject, body, footer) with the body
    ending at the ask — the caller adds the signature and the footer."""
    t = AB310[variant_of(contact.get("tags"))]
    first = (contact.get("firstName") or "").strip() or "there"
    org = display_org(contact.get("company"))
    body = t["body"].format(first=first, org=org)
    return t["subject"].format(org=org), body, t["footer"]


def display_org(name):
    """The scraped names carry a source note in parentheses — "AYSO Region 2
    (Arcadia/Monrovia/Sierra Madre)", "Beach Little League (Beach Baseball)". Fine in a
    sheet, odd in a subject line, so drop it."""
    import re
    org = re.sub(r"\s*\([^)]*\)\s*", " ", (name or "")).strip()
    return re.sub(r"\s{2,}", " ", org) or "your league"
