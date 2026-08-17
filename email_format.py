"""email_format.py — make our mail read like a person wrote it.

Three problems this fixes, all reported by Chris on 2026-08-17 after reading Jessica's and
Ryan's mail:

  1. `>` characters down the left of every line. Those come from the recipient's mail client
     quoting our message when they reply — normal plumbing, but we then embedded that raw
     quote verbatim in the handoff note to sales, so it reached a human's eyes looking like
     machine output. clean_quote() strips the markers and re-joins the hard wraps.

  2. Text breaking at odd places mid-sentence. Plain-text mail is wrapped at ~70 columns, so
     a quoted paragraph arrives chopped ("...about emergency readiness and decision / support").
     Re-flowing the paragraph and sending real HTML fixes it.

  3. Bare URLs. A naked https://... link reads as computer output. link_html() renders words
     that carry the link instead.

Everything here is dependency-free and safe on empty input.
"""
import re
from html import escape as html_escape

_QUOTE_PREFIX = re.compile(r"^\s*(?:>\s?)+")
_SENTENCE_END = re.compile(r"[.!?:;,\"')\]]$")
_BULLET_START = re.compile(r"^\s*(?:[-*•·]|\d+[.)])\s")
_URL = re.compile(r"https?://[^\s<>()\[\]]+")


def clean_quote(text, limit=4000):
    """Turn a raw quoted email into readable prose.

    Strips quote markers at any depth, drops the mail-client attribution lines and signature
    dividers that add nothing, then re-joins lines that were hard-wrapped mid-sentence so
    paragraphs read as paragraphs.
    """
    raw = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = []
    for line in raw.split("\n"):
        lines.append(_QUOTE_PREFIX.sub("", line).rstrip())

    out = []
    for line in lines:
        joinable = (
            out and line and out[-1]
            and not _BULLET_START.match(line)
            and not _BULLET_START.match(out[-1])
            and not _SENTENCE_END.search(out[-1])
            and (line[0].islower() or line[0] in "(\"'")
        )
        if joinable:
            out[-1] = f"{out[-1]} {line}"
        else:
            out.append(line)

    text = "\n".join(out)

    # Drop the client's attribution line now that wrapped halves are joined — it often
    # arrives split across two lines ("...Jessica Dean <x@y>" / "wrote:").
    text = re.sub(r"^On .{0,120}?\bwrote:\s*$", "", text, flags=re.M | re.S)

    # Gmail renders HTML as text when it quotes: <b> becomes *stars* and a link becomes
    # "words <https://url>". Turn that back into words-carrying-a-link, and unwrap bare
    # bracketed URLs, so the handoff doesn't show either artifact.
    text = re.sub(r"\*([^*]{2,200}?)\*\s*<(https?://[^>\s]+)>",
                  lambda m: f"[{' '.join(m.group(1).split())}]({m.group(2)})", text, flags=re.S)
    text = re.sub(r"<(https?://[^>\s]+)>", r"\1", text)
    text = re.sub(r"\*([^*\n]{2,200}?)\*", r"\1", text)

    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    if len(text) > limit:
        text = text[:limit].rstrip() + "\n…(truncated)"
    return text


def link_html(url, words):
    """A hyperlink carrying words, never a bare URL."""
    if not url:
        return html_escape(words)
    return f'<a href="{html_escape(url, quote=True)}" style="color:#1a4f8a">{html_escape(words)}</a>'


def paras_html(text, margin="0 0 12px"):
    """Plain text -> HTML paragraphs.

    Real <p> blocks rather than a chain of <br>, so the recipient's client wraps the text
    itself and the spacing looks like an email instead of a wall with arbitrary breaks. Bare
    URLs left in the text become links on their own words where we can infer them.
    """
    blocks = [b.strip() for b in re.split(r"\n\s*\n", text or "") if b.strip()]
    html_blocks = []
    for block in blocks:
        safe = html_escape(block).replace("\n", "<br>")

        # Build links in two passes with placeholders in between. Doing it in one pass let the
        # bare-URL autolinker run over anchors this function had just written and mangle their
        # href into nested <a> tags.
        held = []

        def _hold(markup):
            held.append(markup)
            return f"\x00{len(held) - 1}\x00"

        # 1. [words](url) — an explicit label, e.g. from a client's text rendering of a link
        safe = re.sub(r"\[([^\]]{1,200})\]\((https?://[^)\s]+)\)",
                      lambda m: _hold(link_html(m.group(2), m.group(1))), safe)
        # 2. anything still bare gets labelled with its host — words, not a query string
        def _autolink(m):
            url = m.group(0)
            host = re.sub(r"^www\.", "", url.split("/")[2]) if "//" in url else url
            return _hold(link_html(url, host))
        safe = _URL.sub(_autolink, safe)
        # 3. put the finished anchors back
        safe = re.sub(r"\x00(\d+)\x00", lambda m: held[int(m.group(1))], safe)

        html_blocks.append(f'<p style="margin:{margin}">{safe}</p>')
    return "".join(html_blocks)


def quote_block_html(text):
    """The prospect's own words, set off the way a person would paste them — indented with a
    rule down the side, not wrapped in --- dividers."""
    return (
        '<div style="border-left:3px solid #d8d8d8;padding:2px 0 2px 14px;margin:10px 0 16px;'
        'color:#333">' + paras_html(clean_quote(text), margin="0 0 10px") + '</div>'
    )


def wrapper_html(inner, signature=None):
    """Wrap a body in the minimal shell a normal mail client would produce."""
    sig = f'<p style="margin:16px 0 0">{html_escape(signature).replace(chr(10), "<br>")}</p>' if signature else ""
    return (
        '<div style="font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;'
        'font-size:14.5px;line-height:1.55;color:#1a1a1a">'
        + inner + sig + '</div>'
    )
