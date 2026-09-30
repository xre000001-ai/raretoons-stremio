#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RareToons 2.9 — Stremio addon.  Streams-only, Phoenix-style cards.

WHAT IT DOES
  For any id (legacy raretoons2:slug or foreign tt... via Cinemeta) the
  addon emits ONE card per LANGUAGE that actually plays in-app:

      name   ◫ MQ ◫
      title  ⧉ <show/movie title> ⌗ <Language>[ · sub]
             ⬡ Movie | ⬡ S01E05 · <episode title>
             ⊞ RareToons ◧ MQ 1080·720·360[ · note]

  Languages: Hindi, Tamil, Telugu, Bengali, Malayalam, Urdu, English,
  Japanese, Anime Times, Hindi Uncut ... (whatever the show's hub has).
  PLAYABLE-ONLY: a language we cannot resolve to our HLS gets no card.

PIPELINE
  1. episodes_index.jsonl (crawler snapshot) gives first-guess zippers.
  2. The site ROTATES movie zippers ~hourly and adds new episodes, so
     on any miss handle_stream/handle_meta re-parse the live hub page
     (_live_rows -> _absorb_live, TTL 15 min) and retry fresh.
  3. codedew zipper links (all non-Hindi languages + movies) sit behind
     a 3-step "Security Scan" wall: _zipper_walk follows the data-href
     chain with a cookie session (semaphore(3) - codedew rate-limits
     parallel walks).
  4. Wall-free pages embed  https://argon.razorshell.space/embed/<id>
     which carries _juicycodes("<blob>") - symbol-map cipher (ported
     below) decoding to the JWPlayer config, whose sources.file is the
     1080p/720p/360p HLS master (groovy.monster, JUICYCODES).
  5. /mq/... proxies rewrite the whole HLS tree same-origin.  The site's
     MQ variant playlists are #EXT-X-BYTERANGE slices of ONE resource -
     exploded to ?rt=start-end so no player ever downloads the whole
     file (the old "full download" bug).  Content is single-video +
     single-audio TS by site encoding.

Run:  python3 addon.py [port]     (binds 0.0.0.0)
"""
import base64
import json
import os
import re
import sys
import time
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote, quote, urljoin

import requests
from curl_cffi import requests as creq

# ---------------------------------------------------------------- config --
def _env_int(k, d):
    try: return int(os.environ.get(k) or d)
    except Exception: return d

PORT = _env_int("PORT", 7700)
HOST_SUFFIX = os.environ.get("HOST_SUFFIX", ".baby-beamup.club")
RESOLVE_TIMEOUT = _env_int("RESOLVE_TIMEOUT", 8)
LIST_BUDGET = _env_int("LIST_BUDGET", 10)          # whole /stream request
HLS_TTL = _env_int("HLS_TTL", 1800)               # zipper -> master URL
NEG_TTL = _env_int("NEG_TTL", 60)                 # failed resolve cache
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
VERSION = "2.9.1"
ADDON_ID = "community.raretoons2"
BASE = os.path.dirname(os.path.abspath(__file__))
SEG_SITE = "https://www.rareanimes.mov/"

CONTENT_TYPES = {
    ".m3u8": "application/vnd.apple.mpegurl",
    ".ts": "video/mp2t", ".mp4": "video/mp4", ".vtt": "text/vtt",
    ".key": "application/octet-stream", ".jpeg": "image/jpeg",
    ".png": "image/png", ".bin": "application/octet-stream",
}

# ------------------------------------------------------------------ data --
def _slug(text):
    return re.sub(r"[^a-z0-9]+", "_", (text or "").lower())[:80].strip("_") or "x"

ROWS = []
with open(os.path.join(BASE, "episodes_index.jsonl"), encoding="utf-8") as _f:
    for _l in _f:
        _l = _l.strip()
        if _l:
            ROWS.append(json.loads(_l))

POSTERS = {}
try:
    with open(os.path.join(BASE, "posters.json"), encoding="utf-8") as _f:
        POSTERS = json.load(_f)
except Exception:
    pass

SHOWS = {}


def _display(show):
    s = re.split(r"\s+(?:[-–]\s+)?(?:Hindi|Tamil|Telugu|WatchMulti|Multi)\b",
                 show or "", maxsplit=1)[0].strip(" -–")
    s = re.sub(r"\s*(?:[-–]\s*)?Episodes?\.?\s*$", "", s, flags=re.I).strip(" -–")
    # Subbed/Dubbed variants of the same show must NOT collapse into one
    # name (Mushoku Tensei S3 exists on the site in BOTH flavours).
    if re.search(r"subbed", show or "", re.I):
        s += " (Hindi Sub)"
    elif re.search(r"dubbed", show or "", re.I):
        s += " (Hindi Dub)"
    return s or show


def _build_shows():
    for r in ROWS:
        show = r.get("show") or ""
        slug = _slug(show)
        rec = SHOWS.get(slug)
        if rec is None:
            rec = {"slug": slug, "name": _display(show), "site_name": show,
                   "hub": r.get("hub_url") or "",
                   "poster": POSTERS.get(show, ""), "movie": None, "eps": {}}
            SHOWS[slug] = rec
        s = int(r.get("season") or 0)
        e = int(r.get("episode") or 0)
        rec["eps"].setdefault((s, e), []).append(r)
    for rec in SHOWS.values():
        rec["movie"] = all((k[0] or 0) == 0 for k in rec["eps"]) if rec["eps"] else False
        rec["poster"] = rec["poster"] or POSTERS.get(rec["site_name"], "")


_build_shows()
SERIES = [r for r in SHOWS.values() if not r["movie"]]
MOVIES = [r for r in SHOWS.values() if r["movie"]]

# ------------------------------------------------------------------ http --
def cf_get(url, referer=None, timeout=RESOLVE_TIMEOUT):
    """codedew/argon/groovy sit behind Cloudflare - the impersonating
    client is required (plain requests get a 403 challenge)."""
    headers = {"User-Agent": UA, "Accept": "*/*",
               "Accept-Language": "en-US,en;q=0.9"}
    if referer:
        headers["Referer"] = referer
    return creq.get(url, headers=headers, timeout=timeout,
                    allow_redirects=True, impersonate="chrome")


def cf_stream(url, referer=None, rng=None, timeout=30):
    """Streaming variant (no lock) for /mq child proxying."""
    headers = {"User-Agent": UA, "Accept": "*/*"}
    if referer:
        headers["Referer"] = referer
    if rng:
        headers["Range"] = rng
    return creq.get(url, headers=headers, timeout=timeout, stream=True,
                    allow_redirects=True, impersonate="chrome")


# ------------------------------------------------------- MQ-HLS resolver --
_JUICY_SYMBOLS = ["`", "%", "-", "+", "*", "$", "!", "_", "^", "="]


def _juicy_decode(blob):
    """Port of the site's _juicycodes(): base64 -> symbol-map digits ->
    4-digit groups -> chr(group % 1000 - salt).  Salt comes from the last
    3 chars (each ord()-100).  Returns the JWPlayer config JS/JSON."""
    salt = int("".join(str(ord(c) - 100) for c in blob[-3:]))
    body = blob[:-3]
    # site JS: input.replace(/_/g,"+").replace(/-/g,"/") then a base64 loop
    # whose alphabet is the STANDARD one and which simply skips characters
    # outside it - replicate exactly (plain urlsafe_b64decode would instead
    # count +/- inconsistently and break on mixed blobs).
    t = re.sub(r"[^A-Za-z0-9+/=]",
               "", body.replace("-", "+").replace("_", "/"))
    s = base64.b64decode(t + "=" * (-len(t) % 4)).decode("latin-1")
    digits = "".join(
        str(_JUICY_SYMBOLS.index(c)) if c in _JUICY_SYMBOLS else "0"
        for c in s)
    return "".join(chr(int(digits[i:i + 4]) % 1000 - salt)
                   for i in range(0, len(digits) // 4 * 4, 4))


_hls_cache = {}
_hls_lock = threading.Lock()
_EMB_RE = re.compile(r'<iframe[^>]+src="([^"]*argon\.razorshell\.space[^"]*)"')
_BLOB_RE = re.compile(r'_juicycodes\(\s*((?:"[A-Za-z0-9+/=_-]{1,}"\s*\+\s*)+'
                      r'"[A-Za-z0-9+/=_-]{1,}")\s*\)')
_MASTER_RE = re.compile(r'"file":"(https:[^"]+\.m3u8)"')


_WALL_RE = re.compile(r'data-href="([^"]+)"')


_WALK_SEM = threading.Semaphore(3)


def _zipper_walk(zipper, max_hops=4, deadline=None):
    """codedew zipper links (all non-Hindi languages, and movies) sit
    behind a 3-step "Security Scan" wall: each wall page carries a
    goBtn data-href and the NEXT hop only resolves when the cookies
    set on the first response are resent.  Walk the chain with a
    cookie session; stop early once the argon embed iframe (watch /
    multiquality page) is visible.  Returns (final_text, embed)."""
    sess = creq.Session(impersonate="chrome")
    prev = SEG_SITE
    url = zipper
    t = ""
    for _ in range(max_hops + 1):
        # codedew rate-limits parallel wall-walks (busy interstitials) -
        # throttle globally; NEG_TTL makes dropped ones retry soon.
        with _WALK_SEM:
            if deadline and time.monotonic() > deadline:
                return t, None          # resolve budget exhausted - no card
            r = sess.get(url, headers={"User-Agent": UA, "Accept": "*/*",
                                       "Accept-Language": "en-US,en;q=0.9",
                                       "Referer": prev},
                         timeout=RESOLVE_TIMEOUT, allow_redirects=True)
        t = r.text or ""
        if deadline and time.monotonic() > deadline:
            return t, None          # resolve budget exhausted - no card
        r = sess.get(url, headers={"User-Agent": UA, "Accept": "*/*",
                                   "Accept-Language": "en-US,en;q=0.9",
                                   "Referer": prev},
                     timeout=RESOLVE_TIMEOUT, allow_redirects=True)
        t = r.text or ""
        m = _EMB_RE.search(t)
        if m:
            return t, m.group(1)
        gh = _WALL_RE.search(t)
        if not gh:
            return t, None
        prev = url
        url = gh.group(1).replace("&amp;", "&")
        if url.startswith("/"):
            url = "https://codedew.com" + url
    return t, None


def _mq_resolve(zipper):
    """zipper -> (master_m3u8_url, embed_url) or (None, embed_url).
    zipper page (after the link-scan wall) -> argon iframe -> embed
    page -> juicy blob -> config."""
    now = time.time()
    with _hls_lock:
        hit = _hls_cache.get(zipper)
        if hit and hit[0] > now:
            return hit[1], hit[2]
    embed = None
    master = None
    deadline = time.monotonic() + 10.0
    try:
        zp, embed = _zipper_walk(zipper, deadline=deadline)
        if embed and time.monotonic() < deadline:
            ep = cf_get(embed, referer=zipper).text or ""
            b = _BLOB_RE.search(ep)
            if b:
                blob = "".join(re.findall(r'"([A-Za-z0-9+/=_-]*)"', b.group(1)))
                dec = _juicy_decode(blob)
                mm = _MASTER_RE.search(dec)
                if mm:
                    master = mm.group(1).replace("\\/", "/")
        else:
            # link-scan wall ended on a non-player page (hubcloud drive
            # for multi-language episode files): embed stays None and
            # the card falls back to the show hub on the site.
            pass
    except Exception:
        pass
    ttl = HLS_TTL if master else NEG_TTL
    with _hls_lock:
        _hls_cache[zipper] = (now + ttl, master, embed)
    return master, embed


def public_base_from(handler):
    host = handler.headers.get("Host") or ""
    if "." not in host and HOST_SUFFIX and not host.endswith(HOST_SUFFIX):
        host = host + HOST_SUFFIX
    proto = handler.headers.get("X-Forwarded-Proto") or "http"
    return f"{proto}://{host}"


def _mq_token(zipper):
    return base64.urlsafe_b64encode(zipper.encode()).decode().rstrip("=")


def _mq_decode_token(tok):
    try:
        pad = "=" * (-len(tok) % 4)
        return base64.urlsafe_b64decode((tok + pad).encode()).decode()
    except Exception:
        return ""


# ---------------------------------------------------------------- cards ----
def _clip(t, n):
    t = re.sub(r"\s+", " ", t or "").strip()
    if len(t) <= n:
        return t
    cut = t[:n]
    sp = cut.rfind(" ")
    return cut[:sp] if sp > n // 2 else cut


def _phx_card(rec, lang, prefix, ep_title, note, z, base):
    """Phoenix-style stream card:

        name   ◫ MQ ◫
        title  ⧉ <title> ⌗ <Language>[ · sub]
               ⬡ Movie | ⬡ S01E05 · <ep title>
               ⊞ RareToons ◧ MQ 1080·720·360[ · note]
    """
    sub = " · sub" if lang.lower().endswith("sub") else ""
    lang0 = re.sub(r"\s*sub\s*$", "", lang, flags=re.I).strip() or lang
    # the hub names carry long "(Hindi Dubbed) ..." tails; the language
    # badge already says it - keep line 1 clean
    clean = re.sub(r"\s*\([^)]*(?:Hindi|Download|HD)[^)]*\)\s*$", "",
                   rec["name"], flags=re.I).strip()
    line1 = f"⧉ {_clip(clean or rec['name'], 56)} ⌗ {lang0}{sub}"
    if rec["movie"]:
        line2 = "⬡ Movie"
    else:
        et = ep_title if ep_title and ep_title != rec["name"] else ""
        line2 = f"⬡ {prefix}" + (f" · {_clip(et, 34)}" if et else "")
    line3 = "⊞ RareToons ◧ MQ 1080·720·360"
    if note:
        line3 += f" · {note}"
    return {
        "name": "◫ MQ ◫",
        "title": f"{line1}\n{line2}\n{line3}",
        "url": f"{base}/mq/{_mq_token(z)}.m3u8",
        "behaviorHints": {
            "notWebReady": False,
            "bingeGroup": f"rt2|mq|{lang.lower()}",
            "filename": f"{prefix}.m3u8",
        },
    }


def _lang_label(raw):
    lang = re.sub(r"\s*(uncut|censored)\s*$", "", raw or "", flags=re.I).strip()
    if not lang or lang.lower() == "default":
        return "Hindi"
    if lang.lower() == "animetimes":
        return "Anime Times"
    if lang.lower() == "watchmultiquality":
        # the site's generic MultiQuality button = the show's primary
        # player (Hindi on these hubs)
        return "Hindi"
    return lang



# -------------------------------------------------- other-catalog streams --
_CINEMETA = "https://v3-cinemeta.strem.io/meta/{t}/{id}.json"
_cine_cache = {}
_cine_lock = threading.Lock()


def _cinemeta_name(mtype, ext_id):
    now = time.time()
    key = (mtype, ext_id)
    with _cine_lock:
        hit = _cine_cache.get(key)
        if hit and hit[0] > now:
            return hit[1]
    name = None
    try:
        r = requests.get(_CINEMETA.format(t=mtype, id=ext_id), timeout=6,
                         headers={"User-Agent": UA, "Accept": "application/json"})
        if r.status_code == 200:
            name = (r.json().get("meta") or {}).get("name")
    except Exception:
        name = None
    with _cine_lock:
        _cine_cache[key] = (now + (6 * 3600 if name else 90), name)
    return name


def _norm_title(t):
    return re.sub(r"[^a-z0-9]+", "", (t or "").lower())


def _base_title(rec):
    """Franchise key: no season, no (…)/language/dub tails - works on
    every hub-title shape ("Season 2 Hindi Episodes ...", "Season 1
    Hindi Dubbed ...", "Season 4 (Hashira Training Arc) ...")."""
    n = rec["name"]
    n = re.sub(r"\([^)]*\)", " ", n)
    n = re.sub(r"\s*[-–]?\s*(hindi|tamil|telugu|malayalam|bengali|multi|"
               r"dubbed|subbed|dub|sub|uncut|crunchyroll|jio\s*cinema)\b.*$",
               "", n, flags=re.I)
    n = re.sub(r"\bseason\s*\d+\b", " ", n, flags=re.I)
    n = re.sub(r"\bepisodes\b", " ", n, flags=re.I)
    n = re.sub(r"\s+", " ", n)
    return n.strip(" -–")


def _match_show(title, season):
    """Franchise match: all hubs whose base title matches the Cinemeta
    title, then pick the hub for the REQUESTED season.  No hub with
    that season -> None (no cards beats wrong-season episodes).  When
    both a Subbed and a Dubbed hub carry the season, prefer Dubbed."""
    tn = _norm_title(title)
    if len(tn) < 4:
        return None
    cands, exact_base = [], []
    for rec in SHOWS.values():
        bn = _norm_title(_base_title(rec))
        if not bn:
            continue
        if bn == tn or bn in tn or tn in bn:
            cands.append(rec)
            if bn == tn:
                exact_base.append(rec)
    if not cands:
        return None
    if exact_base:
        # "Naruto" must not swallow "Naruto Shippuden" seasons: when a
        # hub title matches exactly, substring hubs lose.
        cands = exact_base
    if season is not None:
        exact = [r for r in cands if any(k[0] == season for k in r["eps"])]
        if exact:
            d = [r for r in exact
                 if "subbed" not in (r.get("site_name") or "").lower()]
            return (d or exact)[0]
        return None                    # never play another season's episodes
    # no season requested: first season of the franchise (dub preferred)
    def _s0(r):
        ss = sorted(k[0] for k in r["eps"] if k[0])
        return ss[0] if ss else 99
    cands.sort(key=lambda r: (r["movie"], _s0(r)))
    d = [r for r in cands
         if "subbed" not in (r.get("site_name") or "").lower()]
    return (d or cands)[0]


def _rows_for(rec, s_filter, e_filter):
    eps = rec["eps"]
    if rec["movie"]:
        return eps.get((0, 1)) or next(iter(eps.values()), [])
    if s_filter is None:
        for k in sorted(eps):
            return eps[k]
        return []
    rows = (eps.get((s_filter, e_filter)) or eps.get((0, e_filter))
            or eps.get((1, e_filter)) or [])
    if not rows:
        for (s0, e0), v in sorted(eps.items()):
            if e0 == e_filter:
                rows = v
                break
    return rows


# -------------------------------------------------------------- handlers ---
def handle_manifest():
    base = _public_base_holder.get("base") or ""
    return {
        "id": ADDON_ID,
        "version": VERSION,
        "name": "RareToons 2.0",
        "description": ("Anime & toons in Hindi / Tamil / Telugu — MQ "
                        "player streams in-app as HLS (multi-quality), "
                        "plus other-catalog support."),
        "logo": (base + "/logo.png") if base else "https://5a16d5684c14-raretoons.baby-beamup.club/logo.png",
        "background": None,
        "types": ["movie", "series"],
        # streams-only: no catalogs of our own (user request) — streams
        # resolve for foreign tt... ids from any catalog via Cinemeta,
        # and for legacy raretoons2: ids.
        "resources": ["stream", "meta"],
        "idPrefixes": ["raretoons2", "tt"],
        "catalogs": [],
        "behaviorHints": {"configurable": False},
    }


_EP_POS_RE = re.compile(r"Episode\s*0*(\d{1,3})", re.I)
_LANG_ZIP_RE = re.compile(r"color:\s*#[0-9a-fA-F]+;?[^>]*>\s*([A-Za-z][A-Za-z ]{1,18})"
                          r"\s*</span>[^<]{0,60}?<a[^>]+href=\"([^\"]*codedew\.com/zipper[^\"]*)\"",
                          re.S)
_HUB_ZIP_RE = re.compile(r'href="(https://codedew\.com/zipper/\?url=[^"]+)"')
_LIVE_LANGS = ("Hindi", "Tamil", "Telugu", "English", "Japanese",
               "Bengali", "Malayalam", "Urdu")
_LIVE_TTL = _env_int("LIVE_TTL", 900)      # fresh hub parse validity
_LIVE_NEG = _env_int("LIVE_NEG", 240)
_live_cache = {}                            # hub -> (expires, rows)


def _live_rows(rec):
    """Re-parse the show's hub page on the site.  This is the freshness
    path: brand-new episodes AND rotated movie zippers appear without a
    redeploy.  Episode hubs expose  <color span>Language</span><a zipper>;
    movie hubs expose  'Hindi - Download' / 'Tamil - [ WatchMultiQuality ]'
    style button blocks (HubCloud/DLBeta/4k/GB rows are download-only and
    skipped)."""
    hub = rec.get("hub")
    if not hub:
        return []
    now = time.time()
    with _hls_lock:
        hit = _live_cache.get(hub)
        if hit and hit[0] > now:
            return hit[1]
    rows = []
    try:
        t = cf_get(hub, referer=SEG_SITE).text or ""
        if rec.get("movie"):
            for mm in _HUB_ZIP_RE.finditer(t):
                ctx = re.sub(r"<[^>]+>", " ", t[max(0, mm.start() - 300):mm.start()])
                ctx = re.sub(r"\s+", " ", ctx).strip()
                if not any(b in ctx for b in ("WatchMultiQuality", "WatchNow",
                                              "Download")):
                    continue
                for L in _LIVE_LANGS:
                    if re.search(rf"\b{L}\b", ctx, re.I):
                        rows.append({"show": rec["site_name"], "season": 0,
                                     "episode": 1, "ep_title": "", "lang": L,
                                     "hub_url": hub, "mq": mm.group(1),
                                     "sb": ""})
                        break
        else:
            eps = [(mm.start(), int(mm.group(1))) for mm in _EP_POS_RE.finditer(t)]
            for lm in _LANG_ZIP_RE.finditer(t):
                ep = 0
                for pos, n in eps:
                    if pos < lm.start():
                        ep = n
                    else:
                        break
                if not ep:
                    continue
                rows.append({"show": rec["site_name"], "season": None,
                             "episode": ep, "ep_title": "",
                             "lang": lm.group(1).strip(), "hub_url": hub,
                             "mq": lm.group(2), "sb": ""})
    except Exception:
        rows = []
    with _hls_lock:
        _live_cache[hub] = (now + (_LIVE_TTL if rows else _LIVE_NEG), rows)
    return rows


def _absorb_live(rec, live_rows, s_filter=None):
    """merge live hub rows into rec['eps'] live-first, so rotated zippers
    are replaced by fresh ones and unseen episodes become visible."""
    if not live_rows:
        return
    buckets = {}
    for r in live_rows:
        if rec["movie"]:
            key = (0, 1)
        else:
            s0 = s_filter or r.get("season")
            if not s0:
                s0 = (next(iter(rec["eps"]))[0] if rec["eps"] else 1)
            key = (s0, r.get("episode") or 1)
        r["season"] = key[0]
        buckets.setdefault(key, []).append(r)
    with _hls_lock:
        for key, rs in buckets.items():
            langs = {r.get("lang") for r in rs}
            old = rec["eps"].get(key, [])
            # all fresh candidates first (document order - resolve tries
            # them in sequence), then untouched leftovers
            rec["eps"][key] = rs + [o for o in old
                                    if o.get("lang") not in langs]
    return


def _resolve_picks(picks, done=None, budget=18.0):
    """{lang: [zippers]} -> {lang: (master, zipper)}.  One thread per
    language (wall-walks are I/O bound), up to three zippers per
    language, all inside one shared time budget."""
    out = {}
    if not picks:
        return out
    deadline = time.monotonic() + budget
    lock = threading.Lock()

    def _run(lang, zs):
        if done and lang in done:
            with lock:
                out[lang] = done[lang]
            return
        for z in zs[:3]:
            if time.monotonic() > deadline:
                return
            master, embed = _mq_resolve(z)
            if master:
                with lock:
                    out[lang] = (master, z)
                return

    threads = [threading.Thread(target=_run, args=(l, zs), daemon=True)
               for l, zs in picks.items()]
    for t in threads:
        t.start()
    for t in threads:
        t.join(max(0.1, deadline - time.monotonic()) + 1.0)
    return out


def handle_meta(mtype, mid):
    slug = mid.split(":", 1)[1] if ":" in mid else mid
    rec = SHOWS.get(slug)
    if not rec:
        return None
    meta = {
        "id": mid,
        "type": mtype,
        "name": rec["name"],
        "poster": rec["poster"] or None,
        "background": rec["poster"] or None,
        "description": (f"{rec['name']} — Hindi / Tamil / Telugu dubs, "
                        "streamed via RareToons 2.0."),
        "genres": ["Animation"],
        "videos": [],
    }
    if not rec["movie"]:
        try:
            _absorb_live(rec, _live_rows(rec))
        except Exception:
            pass
        vids = []
        for (s, e), rows in sorted(rec["eps"].items()):
            if not s:
                continue
            r0 = rows[0]
            vids.append({
                "id": f"raretoons2:{slug}:{s}:{e}",
                "season": s,
                "episode": e,
                "title": r0.get("ep_title") or f"Episode {e}",
                "name": r0.get("ep_title") or f"Episode {e}",
            })
        meta["videos"] = vids
    return meta


def handle_stream(mtype, mid):
    """ONE MQ card per language, in-app HLS when resolvable in budget,
    otherwise the argon embed page (browser autoplay).  Foreign tt-ids
    (other catalogs) resolve through Cinemeta title matching."""
    parts = mid.split(":")
    s_filter = e_filter = None
    rec = None
    if mid.startswith("raretoons2:"):
        p2 = mid[len("raretoons2:"):].split(":")
        rec = SHOWS.get(p2[0])
        if len(p2) >= 3:
            try:
                s_filter, e_filter = int(p2[-2]), int(p2[-1])
            except ValueError:
                pass
    elif parts and parts[0].startswith("tt"):
        if len(parts) >= 3:
            try:
                s_filter, e_filter = int(parts[-2]), int(parts[-1])
            except ValueError:
                pass
        title = _cinemeta_name(mtype, parts[0])
        rec = _match_show(title, s_filter) if title else None
    else:
        return {"streams": []}
    if not rec:
        return {"streams": []}
    rows = _rows_for(rec, s_filter, e_filter)
    if not rows:
        # maybe a brand-new episode the static index never saw
        _absorb_live(rec, _live_rows(rec), s_filter)
        rows = _rows_for(rec, s_filter, e_filter)
    if not rows:
        return {"streams": []}
    base = _public_base_holder.get("base") or ""
    prefix = (f"S{s_filter:02d}E{e_filter:02d}" if s_filter is not None
              else f"E{(rows[0].get('episode') or 1):02d}")
    ep_title = rows[0].get("ep_title") or rec["name"]

    order = {"Hindi": 0, "Hindi Uncut": 1, "Tamil": 2, "Telugu": 3,
             "Bengali": 4, "Malayalam": 5, "Urdu": 6, "English": 7,
             "Hindi Sub": 8}

    def _picks(rs):
        p = {}
        for r in rs:
            lang = _lang_label(r.get("lang"))
            z = r.get("mq") or r.get("sb")
            if z:
                lst = p.setdefault(lang, [])
                if z not in lst:
                    lst.append(z)
        return {k: p[k] for k in sorted(p, key=lambda k: order.get(k, 50))}

    picks = _picks(rows)
    if not picks:
        return {"streams": []}
    resolved = _resolve_picks(picks)
    # freshness: a language failed to resolve (the site rotates movie
    # zippers) or the requested episode was missing -> re-parse the live
    # hub once and retry with fresh zippers.
    has_req = any((s_filter is None or (r.get("season") or 1) == s_filter)
                  and (e_filter is None or (r.get("episode") or 1) == e_filter)
                  for r in rows)
    if (not has_req or len(resolved) < len(picks)):
        try:
            live = _live_rows(rec)
        except Exception:
            live = []
        if live:
            _absorb_live(rec, live, s_filter)
            rows = _rows_for(rec, s_filter, e_filter)
            picks = _picks(rows)
            resolved = _resolve_picks(picks, done=resolved)
    if not resolved:
        return {"streams": []}

    dub_show = "dubbed" in (rec.get("site_name") or "").lower()
    streams = []
    for lang, zs in picks.items():
        hit = resolved.get(lang)
        if not hit:
            continue
        master, z = hit[0], hit[1]
        # PLAYABLE-ONLY policy (user request): a language that cannot be
        # resolved to our in-app HLS gets NO card at all.
        if not master:
            continue
        note = ("site has no dub for this episode yet"
                if dub_show and lang.lower() == "hindi sub" else "")
        streams.append(_phx_card(rec, lang, prefix, ep_title, note, z, base))
    return {"streams": streams}


_public_base_holder = {}

# ----------------------------------------------------- /mq HLS rewriter ----
_B64_RE = r"[A-Za-z0-9_\-]"


def _u64(text):
    return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


def _u64d(text):
    try:
        return base64.urlsafe_b64decode(
            text + "=" * (-len(text) % 4)).decode()
    except Exception:
        return ""


_URL_ATTR_RE = re.compile(r'URI="([^"]+)"')


def _rewrite_m3u8(text, base_url, proxy_prefix):
    """Rewrite every URI in an HLS playlist to same-origin /mq/u/<b64>.
    Handles absolute + relative URLs, variant lines, segment lines and
    URI="..." attributes (keys / maps / subtitles)."""
    out = []
    br = None              # pending (start, end-inclusive) from BYTERANGE
    last_end = 0
    for line in text.splitlines():
        ls = line.strip()
        if not ls:
            continue
        # Explode EXT-X-BYTERANGE into the segment URL (?rt=start-end):
        # players that ignore the tag would otherwise download the WHOLE
        # origin resource per segment ("full download" bug).  After this
        # every segment is a standalone resource with an exact body.
        if ls.startswith("#EXT-X-BYTERANGE"):
            mb = re.match(r"#EXT-X-BYTERANGE:(\d+)(?:@(\d+))?", ls)
            if mb:
                ln = int(mb.group(1))
                st = int(mb.group(2)) if mb.group(2) is not None else last_end
                br = (st, st + ln - 1)
                last_end = st + ln
            continue
        if ls.startswith("#EXT-X-KEY") or ls.startswith("#EXT-X-MAP") \
                or ls.startswith("#EXT-X-MEDIA") or ls.startswith("#EXT-X-I-FRAME"):
            def _attr(mm):
                u = urljoin(base_url, mm.group(1))
                ext = ".key" if "KEY" in ls[:12] else ".bin"
                return f'URI="{proxy_prefix}/u/{_u64(u)}{ext}"'
            out.append(_URL_ATTR_RE.sub(_attr, ls))
        elif ls.startswith("#"):
            out.append(ls)
        else:
            u = urljoin(base_url, ls)
            ext = ".m3u8" if u.lower().find(".m3u8") >= 0 else ".ts"
            seg = f"{proxy_prefix}/u/{_u64(u)}{ext}"
            if br:
                seg += f"?rt={br[0]}-{br[1]}"
                br = None
            out.append(seg)
    return "\n".join(out) + "\n"


def _serve_m3u8_master(handler, zipper):
    master, _ = _mq_resolve(zipper)
    if not master:
        handler._send({"error": "MQ stream not available - retry or use "
                                "the site-player card"}, 502)
        return
    try:
        r = cf_get(master, referer="https://argon.razorshell.space/")
        if r.status_code != 200:
            # signed URL may have expired - refresh once
            with _hls_lock:
                _hls_cache.pop(zipper, None)
            master, _ = _mq_resolve(zipper)
            if not master:
                handler._send({"error": "MQ expired"}, 502)
                return
            r = cf_get(master, referer="https://argon.razorshell.space/")
            if r.status_code != 200:
                handler._send({"error": f"master {r.status_code}"}, 502)
                return
        body = _rewrite_m3u8(r.text or "", master,
                             f"/mq/{_mq_token(zipper)}").encode()
        handler._send_raw(body, CONTENT_TYPES[".m3u8"])
    except Exception as exc:
        handler._send({"error": f"master: {exc}"}, 502)


def _serve_m3u8_child(handler, zipper, b64url, ext, rt=""):
    target = _u64d(b64url)
    if not target.startswith("http"):
        handler._send({"error": "bad child"}, 400)
        return
    proxy_prefix = f"/mq/{_mq_token(zipper)}"
    rng = handler.headers.get("Range")
    forced = None
    if rt:
        mr = re.match(r"(\d+)-(\d+)", rt)
        if mr:
            forced = f"bytes={mr.group(1)}-{mr.group(2)}"
            rng = forced          # the segment IS this byte slice
    try:
        # Variant/media playlists must be REWRITTEN too (their segment and
        # child-playlist URIs point at the CF-protected CDN); media files
        # stream straight through.
        if ext == ".m3u8":
            r = cf_get(target, referer="https://argon.razorshell.space/",
                       timeout=20)
            if r.status_code != 200:
                handler._send({"error": f"playlist {r.status_code}"}, 502)
                return
            body = _rewrite_m3u8(r.text or "", target, proxy_prefix).encode()
            handler._send_raw(body, CONTENT_TYPES[".m3u8"])
            return
        up = cf_stream(target, referer="https://argon.razorshell.space/",
                       rng=rng, timeout=30)
        code = up.status_code
        if code not in (200, 206, 416):
            handler._send({"error": f"child {code}"}, 502)
            return
        ctype = up.headers.get("Content-Type")
        if not ctype or "text/html" in ctype.lower():
            ctype = CONTENT_TYPES.get(ext, "application/octet-stream")
        total_s = up.headers.get("Content-Length") or ""
        total = int(total_s) if total_s.isdigit() else 0
        cr = up.headers.get("Content-Range")

        def _headers(cl, crng=None):
            handler.send_response(206 if crng else code)
            handler.send_header("Content-Type", ctype)
            if crng:
                handler.send_header("Content-Range", crng)
            # Content-Length is MANDATORY here: without it HTTP/1.1 clients
            # stall until the socket closes (that was the broken seeking).
            # On 206 the origin's Content-Length IS the body size.
            if cl:
                handler.send_header("Content-Length", str(cl))
            handler.send_header("Connection", "close")
            handler.send_header("Accept-Ranges", "bytes")
            handler.send_header("Access-Control-Allow-Origin", "*")
            handler.send_header("Access-Control-Expose-Headers",
                                "Content-Length, Content-Range, Accept-Ranges")
            handler.send_header("Cache-Control", "no-store")
            handler.end_headers()

        # normalizer: origin ignored Range (200 + full body) -> slice
        if code == 200 and rng and total > 0:
            m2 = re.match(r"\s*bytes=(\d*)-(\d*)", rng)
            if m2 and (m2.group(1) or m2.group(2)):
                start = int(m2.group(1)) if m2.group(1) else 0
                end = int(m2.group(2)) if m2.group(2) else total - 1
                if start < total:
                    end = min(end, total - 1)
                    need = end - start + 1
                    _headers(need, f"bytes {start}-{end}/{total}")
                    if not handler._is_head:
                        skip, left = start, need
                        first = True
                        for ch in up.iter_content(256 * 1024):
                            if not ch:
                                continue
                            if first:
                                # html guard: origin sent a page, not media
                                if ch.lstrip()[:4] in (b"<!DO", b"<htm"):
                                    return
                                first = False
                            if skip:
                                if len(ch) <= skip:
                                    skip -= len(ch)
                                    continue
                                ch, skip = ch[skip:], 0
                            if left <= 0:
                                break
                            if len(ch) > left:
                                ch = ch[:left]
                            handler.wfile.write(ch)
                            left -= len(ch)
                    return
        first = True
        if forced:
            # standalone exploded segment: plain 200 + exact Content-Length
            handler.send_response(200)
            handler.send_header("Content-Type", ctype)
            if total:
                handler.send_header("Content-Length", str(total))
            handler.send_header("Accept-Ranges", "bytes")
            handler.send_header("Access-Control-Allow-Origin", "*")
            handler.send_header("Access-Control-Expose-Headers",
                                "Content-Length, Accept-Ranges")
            handler.send_header("Cache-Control", "no-store")
            handler.send_header("Connection", "close")
            handler.end_headers()
            if not handler._is_head:
                first = True
                for ch in up.iter_content(256 * 1024):
                    if not ch:
                        continue
                    if first:
                        # html guard: origin handed a page, not media
                        if ch.lstrip()[:4] in (b"<!DO", b"<htm"):
                            return
                        first = False
                    handler.wfile.write(ch)
            return
        _headers(total if total else None, cr)
        if not handler._is_head:
            for ch in up.iter_content(256 * 1024):
                if not ch:
                    continue
                if first:
                    # html guard: origin sent a page, not media
                    if ch.lstrip()[:4] in (b"<!DO", b"<htm"):
                        return
                    first = False
                handler.wfile.write(ch)
    except (BrokenPipeError, ConnectionResetError):
        pass
    except Exception as exc:
        try:
            handler._send({"error": f"child: {exc}"}, 502)
        except Exception:
            pass


# ------------------------------------------------------------- http server -
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "RareToons2"

    def log_message(self, fmt, *args):
        sys.stderr.write("[rt2] " + (fmt % args) + "\n")

    def _send(self, obj, code=200, ctype="application/json"):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _send_raw(self, body, ctype, code=200):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self):
        self._is_head = True
        self.do_GET()

    def do_GET(self):
        self._is_head = False
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        qs = parse_qs(parsed.query)
        _public_base_holder["base"] = public_base_from(self)
        try:
            if path.rstrip("/") in ("", "/"):
                self._send({"addon": "RareToons 2.0", "version": VERSION,
                            "status": "ok",
                            "series": len(SERIES), "movies": len(MOVIES),
                            "rows": len(ROWS)})
                return
            if path == "/manifest.json":
                self._send(handle_manifest())
                return
            if path == "/logo.png":
                try:
                    with open(os.path.join(BASE, "logo.png"), "rb") as f:
                        self._send_raw(f.read(), "image/png")
                except Exception:
                    self._send({"error": "no logo"}, 404)
                return


            m = re.match(r"^/meta/(series|movie)/(raretoons2:.+)\.json$", path)
            if m:
                meta = handle_meta(m.group(1), m.group(2))
                self._send({"meta": meta} if meta else {"meta": {}},
                           200 if meta else 404)
                return

            m = re.match(r"^/stream/(series|movie)/"
                         r"(raretoons2:[^/]+|tt\d+(?::\d+)*)\.json$", path)
            if m:
                self._send(handle_stream(m.group(1), m.group(2)))
                return

            m = re.match(rf"^/mq/({_B64_RE}+)\.m3u8$", path)
            if m:
                zipper = _mq_decode_token(m.group(1))
                if not zipper.startswith("https://codedew.com/"):
                    self._send({"error": "bad token"}, 400)
                    return
                _serve_m3u8_master(self, zipper)
                return

            m = re.match(rf"^/mq/({_B64_RE}+)/u/({_B64_RE}+)"
                         rf"(\.[A-Za-z0-9]{{1,5}})?$", path)
            if m:
                zipper = _mq_decode_token(m.group(1))
                if not zipper.startswith("https://codedew.com/"):
                    self._send({"error": "bad token"}, 400)
                    return
                _serve_m3u8_child(self, zipper, m.group(2),
                                  (m.group(3) or ".ts").lower(),
                                  rt=(qs.get("rt", [""])[0] if qs else ""))
                return
            self._send({"error": "not found"}, 404)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as exc:
            try:
                self._send({"error": str(exc)}, 500)
            except Exception:
                pass


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else PORT
    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"[rt2] RareToons 2.0 v{VERSION} | series={len(SERIES)} "
          f"movies={len(MOVIES)} rows={len(ROWS)} posters={len(POSTERS)}")
    print(f"[rt2] listening on 0.0.0.0:{port}")
    srv.serve_forever()


if __name__ == "__main__":
    main()
