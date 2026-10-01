#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RareToons 3.0 — Stremio addon.  Streams-only, Phoenix-style cards.

CARD FORMAT (one card per language that actually plays in-app)

    name   ◫ MQ ◫
    title  ⧉ <title> ⌗ <Language>[ · sub]
           ⬡ Movie | ⬡ S01E05 · <episode title>
           ⊞ RareToons ◧ MQ 1080·720·360[ · note]

    Languages: Hindi, Tamil, Telugu, Bengali, Malayalam, Urdu, English,
    Japanese, Anime Times ... (whatever the show's hub carries).
    PLAYABLE-ONLY: a language we cannot resolve to our HLS gets no card.

RESOLUTION PIPELINE

    1. episodes_index.jsonl (crawler snapshot) gives first-guess zippers.
    2. The site ROTATES movie zippers ~hourly and adds dubs/episodes, so
       EVERY stream/meta request merges the live hub page first
       (_live_rows -> _absorb_live, TTL 15 min, negative 4 min).
    3. codedew zipper links (all non-Hindi languages + movies) sit behind
       a 3-step "Security Scan" wall: _zipper_walk follows the data-href
       chain with a cookie session.  Walks are throttled by a global
       semaphore(3) - codedew rate-limits parallel walks.
    4. The post-wall page embeds https://argon.razorshell.space/embed/<id>
       carrying _juicycodes("<blob>"): a symbol-map cipher (ported below)
       decoding to the JWPlayer config whose sources.file is the
       1080p/720p/360p HLS master (groovy.monster, JUICYCODES).
    5. /mq/... proxies rewrite the whole HLS tree same-origin.  The site's
       MQ variant playlists are #EXT-X-BYTERANGE slices of ONE resource -
       exploded to ?rt=start-end so no player ever downloads the whole
       file (the old "full download" bug).  Content is single-video +
       single-audio TS by site encoding.

IDS
    raretoons2:<slug>[:s:e]   legacy ids from our own metas
    tt<...>:s:e               foreign ids, title-matched via Cinemeta

Run:  python3 addon.py [port]     (binds 0.0.0.0)
"""
import base64
import json
import os
import re
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote, urljoin

import requests
from curl_cffi import requests as creq

# --------------------------------------------------------------- config ---
def _env_int(key, default):
    try:
        return int(os.environ.get(key) or default)
    except Exception:
        return default

PORT            = _env_int("PORT", 7700)
HOST_SUFFIX     = os.environ.get("HOST_SUFFIX", ".baby-beamup.club")
RESOLVE_TIMEOUT = _env_int("RESOLVE_TIMEOUT", 8)   # per upstream fetch, s
WALK_HOPS       = _env_int("WALK_HOPS", 5)         # wall hops incl. final
WALK_BUDGET     = _env_int("WALK_BUDGET", 10)      # per zipper, s
LIST_BUDGET     = _env_int("LIST_BUDGET", 15)      # per /stream resolve, s
HLS_TTL         = _env_int("HLS_TTL", 1800)        # zipper -> master URL
NEG_TTL         = _env_int("NEG_TTL", 60)          # failed resolve cache
LIVE_TTL        = _env_int("LIVE_TTL", 900)        # hub parse validity
LIVE_NEG        = _env_int("LIVE_NEG", 240)        # empty hub parse cache
CINE_TTL        = _env_int("CINE_TTL", 6 * 3600)   # Cinemeta title cache

VERSION  = "3.0.1"
ADDON_ID = "community.raretoons2"
ADDON_NAME = "RareToons"
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
SITE = "https://www.rareanimes.mov/"
ARGON = "https://argon.razorshell.space/"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

CONTENT_TYPES = {
    ".m3u8": "application/vnd.apple.mpegurl",
    ".ts":   "video/mp2t",
    ".mp4":  "video/mp4",
    ".vtt":  "text/vtt",
    ".key":  "application/octet-stream",
    ".png":  "image/png",
    ".bin":  "application/octet-stream",
}

# Language badge order for cards (top of list first).
LANG_ORDER = {"Hindi": 0, "Tamil": 1, "Telugu": 2, "Bengali": 3,
              "Malayalam": 4, "Urdu": 5, "English": 6, "Japanese": 7,
              "Hindi Sub": 8, "Anime Times": 9}

# ----------------------------------------------------------------- data ---
def _slug(text):
    return re.sub(r"[^a-z0-9]+", "_", (text or "").lower())[:80].strip("_") or "x"


def _load_jsonl(path):
    rows = []
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    except FileNotFoundError:
        pass
    return rows


ROWS = _load_jsonl(os.path.join(BASE_DIR, "episodes_index.jsonl"))

POSTERS = {}
try:
    with open(os.path.join(BASE_DIR, "posters.json"), encoding="utf-8") as fh:
        POSTERS = json.load(fh)
except Exception:
    pass


def _display(show):
    """Human hub name; Subbed/Dubbed variants must stay distinct."""
    s = re.split(r"\s+(?:[-–]\s+)?(?:Hindi|Tamil|Telugu|WatchMulti|Multi)\b",
                 show or "", maxsplit=1)[0].strip(" -–")
    s = re.sub(r"\s*(?:[-–]\s*)?Episodes?\.?\s*$", "", s, flags=re.I).strip(" -–")
    if re.search(r"subbed", show or "", re.I):
        s += " (Hindi Sub)"
    elif re.search(r"dubbed", show or "", re.I):
        s += " (Hindi Dub)"
    return s or show


SHOWS = {}


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
        try:
            s, e = int(r.get("season") or 0), int(r.get("episode") or 0)
        except (TypeError, ValueError):
            continue
        rec["eps"].setdefault((s, e), []).append(r)
    for rec in SHOWS.values():
        rec["movie"] = all((k[0] or 0) == 0 for k in rec["eps"]) if rec["eps"] else False
        rec["poster"] = rec["poster"] or POSTERS.get(rec["site_name"], "")


_build_shows()
SERIES = [r for r in SHOWS.values() if not r["movie"]]
MOVIES = [r for r in SHOWS.values() if r["movie"]]

# shared store lock: guards SHOWS[x]["eps"] mutation AND iteration
STORE_LOCK = threading.RLock()

# ------------------------------------------------------------------ http ---
def upstream_get(url, referer=None, timeout=RESOLVE_TIMEOUT):
    """Cloudflare-fronted hosts need the impersonating client."""
    headers = {"User-Agent": UA, "Accept": "*/*",
               "Accept-Language": "en-US,en;q=0.9"}
    if referer:
        headers["Referer"] = referer
    return creq.get(url, headers=headers, timeout=timeout,
                    allow_redirects=True, impersonate="chrome")


def upstream_stream(url, referer=None, rng=None, timeout=30):
    """Streaming variant for /mq child proxying."""
    headers = {"User-Agent": UA, "Accept": "*/*"}
    if referer:
        headers["Referer"] = referer
    if rng:
        headers["Range"] = rng
    return creq.get(url, headers=headers, timeout=timeout, stream=True,
                    allow_redirects=True, impersonate="chrome")


# ------------------------------------------------------- MQ-HLS resolver ---
_JUICY_SYMBOLS = ["`", "%", "-", "+", "*", "$", "!", "_", "^", "="]


def _juicy_decode(blob):
    """Port of the site's _juicycodes(): base64 -> symbol-map digits ->
    4-digit groups -> chr(group % 1000 - salt); salt = last 3 chars
    (each ord()-100).  Returns the JWPlayer config JS/JSON."""
    salt = int("".join(str(ord(c) - 100) for c in blob[-3:]))
    body = blob[:-3]
    # site JS: replace(/_/g,"+").replace(/-/g,"/") then a base64 loop on
    # the STANDARD alphabet that simply skips characters outside it.
    t = re.sub(r"[^A-Za-z0-9+/=]", "",
               body.replace("-", "+").replace("_", "/"))
    s = base64.b64decode(t + "=" * (-len(t) % 4)).decode("latin-1")
    digits = "".join(
        str(_JUICY_SYMBOLS.index(c)) if c in _JUICY_SYMBOLS else "0"
        for c in s)
    return "".join(chr(int(digits[i:i + 4]) % 1000 - salt)
                   for i in range(0, len(digits) // 4 * 4, 4))


_EMB_RE    = re.compile(r'<iframe[^>]+src="([^"]*argon\.razorshell\.space[^"]*)"')
_BLOB_RE   = re.compile(r'_juicycodes\(\s*((?:"[A-Za-z0-9+/=_-]{1,}"\s*\+\s*)+'
                        r'"[A-Za-z0-9+/=_-]{1,}")\s*\)')
_MASTER_RE = re.compile(r'"file":"(https:[^"]+\.m3u8)"')
_WALL_RE   = re.compile(r'data-href="([^"]+)"')

_WALK_SEM   = threading.Semaphore(3)   # codedew rate-limits parallel walks
_hls_cache  = {}                       # zipper -> (expires, master, embed)
_hls_lock   = threading.Lock()


def _zipper_walk(zipper, deadline):
    """Follow codedew's 3-step 'Security Scan' wall.  Each wall page has
    a goBtn data-href; the NEXT hop only resolves when the cookies set on
    the first response are resent - hence one cookie Session.  Returns
    (final_text, argon_embed_or_None)."""
    sess = creq.Session(impersonate="chrome")
    prev, url, text = SITE, zipper, ""
    for _ in range(WALK_HOPS):
        if deadline and time.monotonic() > deadline:
            return text, None
        with _WALK_SEM:                     # global throttle: ONE fetch per hop
            r = sess.get(url, headers={"User-Agent": UA, "Accept": "*/*",
                                       "Accept-Language": "en-US,en;q=0.9",
                                       "Referer": prev},
                         timeout=RESOLVE_TIMEOUT, allow_redirects=True)
        text = r.text or ""
        m = _EMB_RE.search(text)
        if m:
            return text, m.group(1)
        hop = _WALL_RE.search(text)
        if not hop:
            return text, None               # e.g. hubcloud download drive
        prev, url = url, hop.group(1).replace("&amp;", "&")
        if url.startswith("/"):
            url = "https://codedew.com" + url
    return text, None


def _mq_resolve(zipper):
    """zipper -> (master_m3u8_url, embed_url).  Wall -> argon iframe ->
    embed page -> juicy blob -> JWPlayer config.  Cached (HLS_TTL /
    NEG_TTL)."""
    now = time.time()
    with _hls_lock:
        hit = _hls_cache.get(zipper)
        if hit and hit[0] > now:
            return hit[1], hit[2]
    embed = master = None
    deadline = time.monotonic() + WALK_BUDGET
    try:
        _, embed = _zipper_walk(zipper, deadline)
        if embed and time.monotonic() < deadline:
            ep = upstream_get(embed, referer=zipper).text or ""
            b = _BLOB_RE.search(ep)
            if b:
                blob = "".join(re.findall(r'"([A-Za-z0-9+/=_-]*)"', b.group(1)))
                mm = _MASTER_RE.search(_juicy_decode(blob))
                if mm:
                    master = mm.group(1).replace("\\/", "/")
    except Exception:
        pass
    with _hls_lock:
        _hls_cache[zipper] = (now + (HLS_TTL if master else NEG_TTL),
                              master, embed)
    return master, embed


def _mq_token(zipper):
    return base64.urlsafe_b64encode(zipper.encode()).decode().rstrip("=")


def _mq_decode_token(token):
    try:
        return base64.urlsafe_b64decode(
            token + "=" * (-len(token) % 4)).decode()
    except Exception:
        return ""

# --------------------------------------------------------- live freshness --
_EP_POS_RE = re.compile(r"Episode\s*0*(\d{1,3})", re.I)
_LANG_ZIP_RE = re.compile(
    r"color:\s*#[0-9a-fA-F]+;?[^>]*>\s*([A-Za-z][A-Za-z ]{1,18})"
    r"\s*</span>[^<]{0,60}?<a[^>]+href=\"([^\"]*codedew\.com/zipper[^\"]*)\"",
    re.S)
_HUB_ZIP_RE = re.compile(r'href="(https://codedew\.com/zipper/\?url=[^"]+)"')
_LIVE_LANGS = ("Hindi", "Tamil", "Telugu", "English", "Japanese",
               "Bengali", "Malayalam", "Urdu")

_live_cache = {}                          # hub -> (expires, rows)
_live_lock = threading.Lock()


def _live_rows(rec):
    """Re-parse the show's hub page on the site - the freshness path:
    brand-new episodes AND rotated movie zippers appear without a
    redeploy.  Episode hubs expose <color span>Language</span><a zipper>;
    movie hubs expose 'Hindi - Download' / 'Tamil - [ WatchMultiQuality ]'
    button blocks (HubCloud/DLBeta/4k/GB rows are download-only: skip)."""
    hub = rec.get("hub")
    if not hub:
        return []
    now = time.time()
    with _live_lock:
        hit = _live_cache.get(hub)
        if hit and hit[0] > now:
            return hit[1]
    rows = []
    try:
        page = upstream_get(hub, referer=SITE).text or ""
        if rec.get("movie"):
            for mm in _HUB_ZIP_RE.finditer(page):
                ctx = re.sub(r"<[^>]+>", " ",
                             page[max(0, mm.start() - 300):mm.start()])
                ctx = re.sub(r"\s+", " ", ctx).strip()
                if not any(b in ctx for b in ("WatchMultiQuality", "WatchNow",
                                              "Download")):
                    continue
                for lang in _LIVE_LANGS:
                    if re.search(rf"\b{lang}\b", ctx, re.I):
                        rows.append({"show": rec["site_name"], "season": 0,
                                     "episode": 1, "ep_title": "",
                                     "lang": lang, "hub_url": hub,
                                     "mq": mm.group(1), "sb": ""})
                        break
        else:
            eps = [(mm.start(), int(mm.group(1)))
                   for mm in _EP_POS_RE.finditer(page)]
            for lm in _LANG_ZIP_RE.finditer(page):
                ep = 0
                for pos, n in eps:
                    if pos < lm.start():
                        ep = n
                    else:
                        break
                if ep:
                    rows.append({"show": rec["site_name"], "season": None,
                                 "episode": ep, "ep_title": "",
                                 "lang": lm.group(1).strip(), "hub_url": hub,
                                 "mq": lm.group(2), "sb": ""})
    except Exception:
        rows = []
    with _live_lock:
        _live_cache[hub] = (now + (LIVE_TTL if rows else LIVE_NEG), rows)
    return rows


def _absorb_live(rec, live_rows, season=None):
    """Merge live hub rows into rec['eps'] live-first: rotated zippers get
    replaced by fresh ones and unseen episodes become visible."""
    if not live_rows:
        return
    buckets = {}
    for r in live_rows:
        if rec["movie"]:
            key = (0, 1)
        else:
            s0 = season or r.get("season")
            if not s0:
                seasons = [k[0] for k in rec["eps"] if k[0]]
                s0 = min(seasons) if seasons else 1
            key = (s0, r.get("episode") or 1)
        r["season"] = key[0]
        buckets.setdefault(key, []).append(r)
    with STORE_LOCK:
        for key, rs in buckets.items():
            langs = {r.get("lang") for r in rs}
            old = rec["eps"].get(key, [])
            # fresh candidates first (document order = resolve tries them
            # in sequence), then untouched leftovers
            rec["eps"][key] = rs + [o for o in old
                                    if o.get("lang") not in langs]


def _resolve_picks(picks, done=None, budget=LIST_BUDGET):
    """{lang: [zippers]} -> {lang: (master, zipper)}.  One thread per
    language (wall-walks are I/O bound), up to 3 zippers per language,
    inside one shared time budget."""
    out = {}
    if not picks:
        return out
    deadline = time.monotonic() + budget
    lock = threading.Lock()

    def _run(lang, zippers):
        if done and lang in done:
            with lock:
                out[lang] = done[lang]
            return
        for z in zippers[:3]:
            if time.monotonic() > deadline:
                return
            master, _ = _mq_resolve(z)
            if master:
                with lock:
                    out[lang] = (master, z)
                return

    threads = [threading.Thread(target=_run, args=(l, zs), daemon=True)
               for l, zs in picks.items()]
    for t in threads:
        t.start()
    for t in threads:
        t.join(max(0.1, deadline - time.monotonic()) + 0.5)
    return out

# ------------------------------------------------------------------ meta ---
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
                         headers={"User-Agent": UA,
                                  "Accept": "application/json"})
        if r.status_code == 200:
            name = (r.json().get("meta") or {}).get("name")
    except Exception:
        name = None
    with _cine_lock:
        _cine_cache[key] = (now + (CINE_TTL if name else 90), name)
    return name


def _norm_title(text):
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def _base_title(rec):
    """Franchise key: no season, no (...)/language/dub tails - works on
    every hub-title shape ("Season 2 Hindi Episodes ...", "Season 1
    Hindi Dubbed ...", "Season 4 (Hashira Training Arc) ...")."""
    n = rec["name"]
    n = re.sub(r"\([^)]*\)", " ", n)
    n = re.sub(r"\s*[-–]?\s*(hindi|tamil|telugu|malayalam|bengali|multi|"
               r"dubbed|subbed|dub|sub|uncut|crunchyroll|jio\s*cinema)\b.*$",
               "", n, flags=re.I)
    n = re.sub(r"\bseason\s*\d+\b", " ", n, flags=re.I)
    n = re.sub(r"\bepisodes\b", " ", n, flags=re.I)
    return re.sub(r"\s+", " ", n).strip(" -–")


def _match_show(title, season):
    """Franchise match: hubs whose base title matches the Cinemeta title;
    pick the hub carrying the REQUESTED season.  No such hub -> None
    (no cards beats wrong-season episodes).  Exact-base hubs beat
    substring hubs ("Naruto" must not swallow "Naruto Shippuden");
    Subbed/Dubbed duplicates prefer Dubbed."""
    tn = _norm_title(title)
    if len(tn) < 4:
        return None
    cands, exact_base = [], []
    for rec in SHOWS.values():
        bn = _norm_title(_base_title(rec))
        if not bn or not (bn == tn or bn in tn or tn in bn):
            continue
        cands.append(rec)
        if bn == tn:
            exact_base.append(rec)
    if not cands:
        return None
    if exact_base:
        cands = exact_base
    if season is not None:
        with STORE_LOCK:
            exact = [r for r in cands if any(k[0] == season for k in r["eps"])]
        if exact:
            dubbed = [r for r in exact
                      if "subbed" not in (r.get("site_name") or "").lower()]
            return (dubbed or exact)[0]
        return None

    def _first_season(r):
        with STORE_LOCK:
            ss = sorted(k[0] for k in r["eps"] if k[0])
        return ss[0] if ss else 99

    cands.sort(key=lambda r: (r["movie"], _first_season(r)))
    dubbed = [r for r in cands
              if "subbed" not in (r.get("site_name") or "").lower()]
    return (dubbed or cands)[0]


def _rows_for(rec, season, episode):
    with STORE_LOCK:
        eps = rec["eps"]
        if rec["movie"]:
            return eps.get((0, 1)) or next(iter(eps.values()), [])
        if season is None:
            for key in sorted(eps):
                return eps[key]
            return []
        rows = (eps.get((season, episode)) or eps.get((0, episode))
                or eps.get((1, episode)) or [])
        if not rows:
            for (s0, e0), v in sorted(eps.items()):
                if e0 == episode:
                    rows = v
                    break
        return rows

# ----------------------------------------------------------------- cards ---
def _clip(text, limit):
    """Whitespace-normalised, word-boundary-clipped text."""
    text = re.sub(r"\s+", " ", text or "").strip()
    if len(text) <= limit:
        return text
    cut = text[:limit]
    sp = cut.rfind(" ")
    return cut[:sp] if sp > limit // 2 else cut


def _phx_card(rec, lang, prefix, ep_title, note, zipper, base):
    """Phoenix-style stream card:

        name   ◫ MQ ◫
        title  ⧉ <title> ⌗ <Language>[ · sub]
               ⬡ Movie | ⬡ S01E05 · <ep title>
               ⊞ RareToons ◧ MQ 1080·720·360[ · note]
    """
    sub = " · sub" if lang.lower().endswith("sub") else ""
    lang0 = re.sub(r"\s*sub\s*$", "", lang, flags=re.I).strip() or lang
    # hub names carry "(Hindi Dubbed) ..." tails; the badge already says it
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
        "url": f"{base}/mq/{_mq_token(zipper)}.m3u8",
        "behaviorHints": {
            "notWebReady": False,
            "bingeGroup": f"rt2|mq|{lang.lower()}",
            "filename": f"{prefix}.m3u8",
        },
    }


def _lang_label(raw):
    """Row language -> card badge.  Default/WMQ rows are the primary
    (Hindi) player; 'Hindi Dub' folds into plain Hindi; 'Hindi Sub'
    keeps its own badge."""
    lang = re.sub(r"\s*(?:uncut|censored)\s*$", "", raw or "",
                  flags=re.I).strip()
    if not lang or lang.lower() in ("default", "watchmultiquality"):
        return "Hindi"
    if lang.lower() == "animetimes":
        return "Anime Times"
    return re.sub(r"\s+dub$", "", lang, flags=re.I)

# ------------------------------------------------------------- handlers ----
_public_base_holder = {}


def handle_manifest(base):
    return {
        "id": ADDON_ID,
        "version": VERSION,
        "name": ADDON_NAME,
        "description": ("Anime & toons in Hindi, Tamil, Telugu, Bengali "
                        "and more - every language the site carries, "
                        "played in-app as multi-quality HLS.  Works with "
                        "any catalog's tt-ids."),
        "logo": (base + "/logo.png") if base
                else "https://5a16d5684c14-raretoons.baby-beamup.club/logo.png",
        "background": None,
        "types": ["movie", "series"],
        # streams-only (user request): no catalogs of our own; streams
        # resolve for foreign tt... ids via Cinemeta and for legacy
        # raretoons2: ids.
        "resources": ["stream", "meta"],
        "idPrefixes": ["raretoons2", "tt"],
        "catalogs": [],
        "behaviorHints": {"configurable": False},
    }


def handle_meta(mtype, mid):
    slug = mid.split(":", 1)[1] if ":" in mid else mid
    rec = SHOWS.get(slug)
    if not rec:
        return None
    try:
        _absorb_live(rec, _live_rows(rec))
    except Exception:
        pass
    meta = {
        "id": mid,
        "type": mtype,
        "name": rec["name"],
        "poster": rec["poster"] or None,
        "background": rec["poster"] or None,
        "description": (f"{rec['name']} - every language the site "
                        "carries, streamed in-app as multi-quality HLS."),
        "genres": ["Animation"],
        "videos": [],
    }
    if not rec["movie"]:
        vids = []
        with STORE_LOCK:
            for (s, e), rows in sorted(rec["eps"].items()):
                if not s:
                    continue
                r0 = rows[0]
                title = r0.get("ep_title") or f"Episode {e}"
                vids.append({"id": f"raretoons2:{slug}:{s}:{e}",
                             "season": s, "episode": e,
                             "title": title, "name": title})
        meta["videos"] = vids
    return meta


def handle_stream(mtype, mid):
    """One MQ card per resolvable language.  Foreign tt-ids title-match
    through Cinemeta.  Every request merges the live hub first (TTL
    cached): the site rotates movie zippers and adds dubs/episodes
    continuously, so the crawler snapshot goes stale within hours."""
    season = episode = None
    rec = None
    if mid.startswith("raretoons2:"):
        parts = mid[len("raretoons2:"):].split(":")
        rec = SHOWS.get(parts[0])
        if len(parts) >= 3:
            try:
                season, episode = int(parts[-2]), int(parts[-1])
            except ValueError:
                pass
    elif re.match(r"^tt\d+$", mid.split(":")[0] or ""):
        parts = mid.split(":")
        try:
            if len(parts) >= 3:
                season, episode = int(parts[-2]), int(parts[-1])
        except ValueError:
            pass
        title = _cinemeta_name(mtype, parts[0])
        rec = _match_show(title, season) if title else None
    else:
        return {"streams": []}
    if not rec:
        return {"streams": []}

    try:
        _absorb_live(rec, _live_rows(rec), season)
    except Exception:
        pass
    rows = _rows_for(rec, season, episode)
    if not rows:
        return {"streams": []}

    base = _public_base_holder.get("base") or ""
    prefix = (f"S{season:02d}E{episode:02d}" if season is not None
              else f"E{(rows[0].get('episode') or 1):02d}")
    ep_title = rows[0].get("ep_title") or rec["name"]

    def _picks(rs):
        p = {}
        for r in rs:
            lang = _lang_label(r.get("lang"))
            z = r.get("mq") or r.get("sb")
            if z:
                lst = p.setdefault(lang, [])
                if z not in lst:
                    lst.append(z)
        return {k: p[k] for k in sorted(p, key=lambda k: LANG_ORDER.get(k, 50))}

    picks = _picks(rows)
    if not picks:
        return {"streams": []}
    resolved = _resolve_picks(picks)

    # freshness retry: some language failed (rotated zippers) or the
    # requested episode is not in the merged view -> force one more
    # live parse and retry with fresh zippers.
    has_req = any((season is None or (r.get("season") or 1) == season)
                  and (episode is None or (r.get("episode") or 1) == episode)
                  for r in rows)
    if not has_req or len(resolved) < len(picks):
        with _live_lock:
            _live_cache.pop(rec.get("hub"), None)
        try:
            live = _live_rows(rec)
        except Exception:
            live = []
        if live:
            _absorb_live(rec, live, season)
            rows = _rows_for(rec, season, episode)
            picks = _picks(rows)
            resolved = _resolve_picks(picks, done=resolved)

    dub_show = "dubbed" in (rec.get("site_name") or "").lower()
    has_dub = any(_lang_label(lang).lower() == "hindi" for lang in resolved)
    streams = []
    for lang in picks:                     # LANG_ORDER iteration order
        hit = resolved.get(lang)
        if not hit:
            continue                       # PLAYABLE-ONLY: no card at all
        master, z = hit
        note = ""
        if dub_show and lang.lower() == "hindi sub" and not has_dub:
            note = "site has no dub for this episode yet"
        streams.append(_phx_card(rec, lang, prefix, ep_title, note, z, base))
    return {"streams": streams}

# ---------------------------------------------------- /mq HLS rewriter -----
_B64_RE = r"[A-Za-z0-9_\-]"
_URL_ATTR_RE = re.compile(r'URI="([^"]+)"')


def _u64(text):
    return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


def _u64d(text):
    try:
        return base64.urlsafe_b64decode(
            text + "=" * (-len(text) % 4)).decode()
    except Exception:
        return ""


def _rewrite_m3u8(text, base_url, proxy_prefix):
    """Rewrite every URI in an HLS playlist to same-origin /mq/u/<b64>:
    absolute + relative URLs, variant lines, segment lines, URI="..."
    attributes.  #EXT-X-BYTERANGE is EXPLODED into the segment URL
    (?rt=start-end) so a player that ignores the tag never downloads the
    whole origin resource (the "full download" bug)."""
    out, br, last_end = [], None, 0
    for line in text.splitlines():
        ls = line.strip()
        if not ls:
            continue
        if ls.startswith("#EXT-X-BYTERANGE"):
            mb = re.match(r"#EXT-X-BYTERANGE:(\d+)(?:@(\d+))?", ls)
            if mb:
                ln = int(mb.group(1))
                st = int(mb.group(2)) if mb.group(2) is not None else last_end
                br = (st, st + ln - 1)
                last_end = st + ln
            continue
        if ls.startswith(("#EXT-X-KEY", "#EXT-X-MAP", "#EXT-X-MEDIA",
                          "#EXT-X-I-FRAME")):
            def _attr(mm):
                u = urljoin(base_url, mm.group(1))
                ext = ".key" if "KEY" in ls[:12] else ".bin"
                return f'URI="{proxy_prefix}/u/{_u64(u)}{ext}"'
            out.append(_URL_ATTR_RE.sub(_attr, ls))
        elif ls.startswith("#"):
            out.append(ls)
        else:
            u = urljoin(base_url, ls)
            ext = ".m3u8" if ".m3u8" in u.lower() else ".ts"
            seg = f"{proxy_prefix}/u/{_u64(u)}{ext}"
            if br:
                seg += f"?rt={br[0]}-{br[1]}"
                br = None
            out.append(seg)
    return "\n".join(out) + "\n"


_HTML_HEADS = (b"<!DO", b"<htm")


def _pump(handler, response, length=None, skip=0):
    """Stream the upstream body to the client, guarding against the
    origin slipping in an HTML page instead of media."""
    first = True
    written = 0
    for chunk in response.iter_content(256 * 1024):
        if not chunk:
            continue
        if first:
            if chunk.lstrip()[:4] in _HTML_HEADS:
                return False
            first = False
        if skip:
            if len(chunk) <= skip:
                skip -= len(chunk)
                continue
            chunk, skip = chunk[skip:], 0
        if length is not None and written + len(chunk) > length:
            chunk = chunk[:length - written]
        handler.wfile.write(chunk)
        written += len(chunk)
        if length is not None and written >= length:
            break
    return True


def _serve_m3u8_master(handler, zipper):
    master, _ = _mq_resolve(zipper)
    if not master:
        handler._send({"error": "MQ stream unavailable - try again"}, 502)
        return
    try:
        # the site's video host (groovy) sometimes hangs at connect
        # level during load spikes - one bounded retry rides out blips
        r = None
        for attempt in (1, 2):
            try:
                r = upstream_get(master, referer=ARGON,
                                 timeout=RESOLVE_TIMEOUT)
                break
            except Exception:
                if attempt == 2:
                    raise
        if r is None or r.status_code != 200:
            # signed URL may have expired - refresh the resolve once
            with _hls_lock:
                _hls_cache.pop(zipper, None)
            master, _ = _mq_resolve(zipper)
            if not master:
                handler._send({"error": "MQ expired"}, 502)
                return
            r = upstream_get(master, referer=ARGON)
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
            rng = forced                # the segment IS this byte slice
    try:
        # Variant/media playlists must be REWRITTEN too; media streams.
        if ext == ".m3u8":
            r = upstream_get(target, referer=ARGON, timeout=20)
            if r.status_code != 200:
                handler._send({"error": f"playlist {r.status_code}"}, 502)
                return
            body = _rewrite_m3u8(r.text or "", target, proxy_prefix).encode()
            handler._send_raw(body, CONTENT_TYPES[".m3u8"])
            return

        up = upstream_stream(target, referer=ARGON, rng=rng, timeout=30)
        code = up.status_code
        if code not in (200, 206, 416):
            handler._send({"error": f"child {code}"}, 502)
            return
        ctype = up.headers.get("Content-Type")
        if not ctype or "text/html" in ctype.lower():
            ctype = CONTENT_TYPES.get(ext, "application/octet-stream")
        total_s = up.headers.get("Content-Length") or ""
        total = int(total_s) if total_s.isdigit() else 0
        crange = up.headers.get("Content-Range")

        def _headers(cl, crng=None):
            handler.send_response(206 if crng else code)
            handler.send_header("Content-Type", ctype)
            if crng:
                handler.send_header("Content-Range", crng)
            # Content-Length is MANDATORY: without it HTTP/1.1 clients
            # stall until socket close (that was the broken seeking).
            if cl:
                handler.send_header("Content-Length", str(cl))
            handler.send_header("Accept-Ranges", "bytes")
            handler.send_header("Access-Control-Allow-Origin", "*")
            handler.send_header("Access-Control-Expose-Headers",
                                "Content-Length, Content-Range, "
                                "Accept-Ranges")
            handler.send_header("Cache-Control", "no-store")
            handler.send_header("Connection", "close")
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
                        _pump(handler, up, length=need, skip=start)
                    return

        if forced:
            # exploded standalone segment: plain 200 + exact body
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
                _pump(handler, up)
            return

        _headers(total or None, crange)
        if not handler._is_head:
            _pump(handler, up)
    except (BrokenPipeError, ConnectionResetError):
        pass
    except Exception as exc:
        try:
            handler._send({"error": f"child: {exc}"}, 502)
        except Exception:
            pass

# ---------------------------------------------------------- http server ----
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "RareToons2"

    def log_message(self, fmt, *args):
        sys.stderr.write("[rt2] " + (fmt % args) + "\n")

    # ------------------------------------------------------ helpers ----
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

    # ------------------------------------------------------- routes ----
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Methods", "GET, HEAD, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "range, origin, "
                          "content-type, accept")
        self.send_header("Access-Control-Max-Age", "86400")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_HEAD(self):
        self._is_head = True
        self._route()

    def do_GET(self):
        self._is_head = False
        self._route()

    def _route(self):
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        qs = parse_qs(parsed.query)
        _public_base_holder["base"] = public_base_from(self)
        try:
            if path.rstrip("/") in ("", "/"):
                self._send({"addon": ADDON_NAME, "version": VERSION,
                            "status": "ok", "series": len(SERIES),
                            "movies": len(MOVIES), "rows": len(ROWS)})
                return
            if path == "/manifest.json":
                self._send(handle_manifest(_public_base_holder["base"]))
                return
            if path == "/logo.png":
                try:
                    with open(os.path.join(BASE_DIR, "logo.png"), "rb") as fh:
                        self._send_raw(fh.read(), "image/png")
                except Exception:
                    self._send({"error": "no logo"}, 404)
                return

            m = re.match(r"^/meta/(series|movie)/(raretoons2:.+)\.json$",
                         path)
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


def public_base_from(handler):
    host = handler.headers.get("Host") or ""
    if "." not in host and HOST_SUFFIX and not host.endswith(HOST_SUFFIX):
        host = host + HOST_SUFFIX
    proto = handler.headers.get("X-Forwarded-Proto") or "http"
    return f"{proto}://{host}"


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else PORT
    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"[rt2] {ADDON_NAME} v{VERSION} | series={len(SERIES)} "
          f"movies={len(MOVIES)} rows={len(ROWS)} posters={len(POSTERS)}")
    print(f"[rt2] listening on 0.0.0.0:{port}")
    srv.serve_forever()


if __name__ == "__main__":
    main()
