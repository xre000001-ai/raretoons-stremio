#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RareToons 2.0 — Stremio addon, rebuilt from scratch on 2026-09-30.

The previous addon accumulated months of workarounds (zipper 302 chains,
POST APIs, lazy placeholder cards, election logic, TMDB enrichment).
The site changed under it repeatedly and playback broke.  This file is a
clean-room rewrite against the site AS IT WORKS TODAY (every step below
was verified live on 2026-09-30 before being coded).

HOW THE SITE WORKS TODAY (verified 2026-09-30)
----------------------------------------------
* Show pages (www.rareanimes.mov/...) list per-episode links shaped
  https://codedew.com/zipper/?url=<percent-encoded-file-id>  ("zipper").
* A zipper page today embeds a player iframe (argon.razorshell.space)
  and auto-advances episodes.  That is the SITE's own player and plays
  in any browser — this is the playback path every user confirmed works.
* Independently, https://codedew.com/streambeta/?url=<same-file-id>
  returns a server-rendered page whose HTML embeds
      let playerSources = [ {"url": "<signed worker URL>", "name": "V1"}, ... ]
  with V1..V4 entries: workers.dev signed direct-file URLs, a pixeldrain
  mirror, and a download portal (fused out below).  This is what we use
  for IN-APP playback.

DESIGN (small on purpose)
-------------------------
1. Resolution happens AT REQUEST TIME with a hard budget; no placeholder
   or "lazy" cards — every card is either a real proxied stream or the
   site's browser player.  A failed resolve still leaves the browser
   card, so the list is never empty.
2. In-app playback goes THROUGH this addon (/s/ route: same-origin,
   CORS-open, correct Content-Type).  Direct worker URLs 302s failed for
   many ISPs/players; the addon-domain path is the one that plays.
3. /s/ carries a Range normalizer: some origins ignore Range and return
   the full body with 200 — we slice server-side and emit a proper 206.
4. No TMDB, no catalogs zoo, no elections, no background jobs.  Site
   data only: episodes_index.jsonl (rows) + posters.json (og:images).
5. beamup-ready: buildpack (Procfile), PORT env, public base rebuilt
   from the bare Host header the beamup router sends.

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
from urllib.parse import urlparse, parse_qs, unquote, quote

import requests
from curl_cffi import requests as creq

# ---------------------------------------------------------------- config --
def _env_int(k, d):
    try: return int(os.environ.get(k) or d)
    except Exception: return d

PORT = _env_int("PORT", 7700)
HOST_SUFFIX = os.environ.get("HOST_SUFFIX", ".baby-beamup.club")
RESOLVE_TIMEOUT = _env_int("RESOLVE_TIMEOUT", 9)     # per-zipper seconds
LIST_BUDGET = _env_int("LIST_BUDGET", 7)             # whole /stream request
PICK_TTL = _env_int("PICK_TTL", 600)                 # /s/ pinned source TTL
SRC_TTL = _env_int("SRC_TTL", 600)                   # zipper→sources cache
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
MAX_SERVERS = 3                                       # V1..V3 in-app cards
VERSION = "2.1.0"
ADDON_ID = "community.raretoons2"

BASE = os.path.dirname(os.path.abspath(__file__))

CONTENT_TYPES = {
    ".mkv": "video/x-matroska", ".mp4": "video/mp4", ".webm": "video/webm",
    ".mov": "video/quicktime", ".avi": "video/x-msvideo", ".m4v": "video/x-m4v",
    ".ts": "video/mp2t", ".bin": "application/octet-stream",
}

# Hosts that are download portals / players, not direct files:
_DROP_HOST = re.compile(
    r"(fuckingfast\.net|mega\.nz|mediafire|drive\.google|1fichier|"
    r"colonel-fans|argon\.razorshell)", re.I)

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

# slug -> show record
SHOWS = {}


def _display(show):
    """Trim the site's SEO tail off a show name for display."""
    s = re.split(r"\s+(?:[-–]\s+)?(?:Hindi|Tamil|Telugu|WatchMulti|Multi)\b",
                 show or "", maxsplit=1)[0].strip(" -–")
    s = re.sub(r"\s*(?:[-–]\s*)?Episodes?\.?\s*$", "", s, flags=re.I).strip(" -–")
    return s or show


def _is_movie(show):
    return bool(re.search(r"\bmovie\b|\(\d{4}\)", show, re.I)) or \
        all((r.get("season") or 0) == 0 for r in _rows_of_show(show))


def _rows_of_show(show):
    return [r for r in ROWS if r.get("show") == show]


def _build_shows():
    for r in ROWS:
        show = r.get("show") or ""
        slug = _slug(show)
        rec = SHOWS.get(slug)
        if rec is None:
            rec = {
                "slug": slug, "name": _display(show), "site_name": show,
                "hub": r.get("hub_url") or "", "poster": POSTERS.get(show, ""),
                "movie": None, "eps": {},
            }
            SHOWS[slug] = rec
        s = int(r.get("season") or 0)
        e = int(r.get("episode") or 0)
        key = (s, e)
        bucket = rec["eps"].get(key)
        if bucket is None:
            rec["eps"][key] = bucket = []
        bucket.append(r)
    for rec in SHOWS.values():
        rec["movie"] = all((k[0] or 0) == 0 for k in rec["eps"]) if rec["eps"] else False
        rec["poster"] = rec["poster"] or POSTERS.get(rec["site_name"], "")


_build_shows()
SERIES = [r for r in SHOWS.values() if not r["movie"]]
MOVIES = [r for r in SHOWS.values() if r["movie"]]

# ------------------------------------------------------------------ http --
_cf_lock = threading.Lock()


def cf_get(url, referer=None, timeout=RESOLVE_TIMEOUT):
    """codedew/argon pages sit behind Cloudflare - use the impersonating
    client (verified working against the site)."""
    headers = {"User-Agent": UA, "Accept": "*/*",
               "Accept-Language": "en-US,en;q=0.9"}
    if referer:
        headers["Referer"] = referer
    with _cf_lock:
        return creq.get(url, headers=headers, timeout=timeout,
                        allow_redirects=True, impersonate="chrome")


def plain_get(url, rng=None, timeout=20):
    headers = {"User-Agent": UA, "Accept": "*/*"}
    if rng:
        headers["Range"] = rng
    return requests.get(url, headers=headers, timeout=timeout, stream=True,
                        allow_redirects=True)


# ------------------------------------------------------------- resolution --
# zipper -> (expiry, [ {url, name, filename} ... ])
_src_cache = {}
_src_lock = threading.Lock()
_INFLIGHT = {}


def _parse_player_sources(html):
    """playerSources JSON out of a streambeta page; falls back to a plain
    scan for signed worker URLs."""
    out = []
    m = re.search(r"(?:let|var|const)\s+playerSources\s*=\s*(\[.*?\])\s*;",
                  html or "", re.S)
    if m:
        try:
            for it in json.loads(m.group(1)):
                u = it.get("url") or it.get("stream_url") or ""
                if not u:
                    continue
                out.append({"url": u, "name": (it.get("name") or "").upper(),
                            "filename": it.get("filename") or ""})
        except Exception:
            pass
    if not out:
        T = r"[A-Za-z0-9_\-.~+/=&?%]+"
        for mm in re.finditer(r"https?://[a-z0-9.-]*workers\.dev/" + T, html or ""):
            out.append({"url": mm.group(0).rstrip(" ,;\"')"), "name": "", "filename": ""})
    # drop portals / junk, keep order
    keep = []
    for s in out:
        u = s["url"]
        if _DROP_HOST.search(u):
            continue
        if u.endswith(".m3u8") or ".m3u8?" in u:
            continue
        keep.append(s)
    seen, dedup = set(), []
    for s in keep:
        if s["url"] not in seen:
            seen.add(s["url"])
            dedup.append(s)
    return dedup


def resolve_zipper(zipper, timeout=RESOLVE_TIMEOUT):
    """codedew zipper -> [{"url","name","filename"}] (cached, TTL SRC_TTL)."""
    if not zipper:
        return []
    now = time.time()
    with _src_lock:
        hit = _src_cache.get(zipper)
        if hit and hit[0] > now:
            return hit[1]
    m = re.search(r"[?&]url=([^&\"']+)", zipper)
    if not m:
        return []
    page_url = "https://codedew.com/streambeta/?url=" + m.group(1)
    try:
        r = cf_get(page_url, referer=zipper, timeout=timeout)
        sources = _parse_player_sources(r.text) if r.status_code == 200 else []
    except Exception:
        sources = []
    with _src_lock:
        _src_cache[zipper] = (now + SRC_TTL, sources)
    return sources


def resolve_many(zippers):
    """Resolve several zippers in parallel, respecting the request budget."""
    out = {z: [] for z in zippers}
    threads = []

    def _run(z):
        out[z] = resolve_zipper(z)

    for z in zippers:
        t = threading.Thread(target=_run, args=(z,), daemon=True)
        t.start()
        threads.append(t)
    deadline = time.time() + LIST_BUDGET
    for t in threads:
        t.join(max(0.2, deadline - time.time()))
    return out


# ----------------------------------------------------------------- urls ----
def _token(zipper, index=0):
    return base64.urlsafe_b64encode(
        f"{zipper}\x00{index}".encode()).decode().rstrip("=")


def _decode_token(tok):
    try:
        pad = "=" * (-len(tok) % 4)
        raw = base64.urlsafe_b64decode((tok + pad).encode()).decode()
        z, _, i = raw.partition("\x00")
        return z, (int(i) if i.isdigit() else 0)
    except Exception:
        return "", 0


def _proxy_url(base, zipper, index=0, ext=".mkv"):
    return f"{base}/s/{_token(zipper, index)}{ext}"


def public_base_from(handler):
    host = handler.headers.get("Host") or ""
    if "." not in host and HOST_SUFFIX and not host.endswith(HOST_SUFFIX):
        host = host + HOST_SUFFIX
    proto = handler.headers.get("X-Forwarded-Proto") or "http"
    return f"{proto}://{host}"


# ---------------------------------------------------------------- cards ----
def _ext_of(url):
    m = re.search(r"\.(mkv|mp4|webm|mov|avi|m4v)(?:[?#]|$)", url, re.I)
    return ("." + m.group(1).lower()) if m else ".mkv"


def _lang_label(raw):
    lang = re.sub(r"\s*(uncut|censored)\s*$", "", raw or "", flags=re.I).strip()
    if not lang:
        lang = "Hindi"
    return "Multi" if lang.lower().startswith("watch") else lang


def _cards_for_zipper(base, prefix, ep_title, lang, zipper, sources):
    """Build the card list for one language zipper:
    in-app proxy cards (real resolved servers) + the site browser player."""
    cards, seen = [], set()
    lang_l = _lang_label(lang)
    listed = sources[:MAX_SERVERS]
    for i, s in enumerate(listed):
        ext = _ext_of(s["url"])
        url = _proxy_url(base, zipper, i, ext)
        if url in seen:
            continue
        seen.add(url)
        srv = s.get("name") or f"V{i + 1}"
        fname = s.get("filename") or f"{prefix}{ext}"
        cards.append({
            "name": f"RT2 • {lang_l}",
            "title": f"{prefix} • {ep_title} — {srv} (in-app)",
            "description": f"{prefix}\n◈ {lang_l}\n◈ Server {srv}\n◈ plays inside the app",
            "url": url,
            "behaviorHints": {
                "notWebReady": False,
                "bingeGroup": f"rt2|inapp|{lang_l.lower()}",
                "filename": fname,
            },
        })
    if zipper not in seen:
        cards.append({
            "name": f"RT2 • {lang_l}",
            "title": f"{prefix} • {ep_title} — Site Player (browser)",
            "description": f"{prefix}\n◈ {lang_l}\n◈ opens the site's own player in a browser",
            "url": zipper,
            "externalUrl": zipper,
            "behaviorHints": {"notWebReady": True},
        })
    return cards


def _browser_only_card(prefix, ep_title, lang, zipper):
    return [{
        "name": f"RT2 • {_lang_label(lang)}",
        "title": f"{prefix} • {ep_title} — Site Player (browser)",
        "description": f"{prefix}\n◈ {_lang_label(lang)}\n◈ opens the site's own player in a browser",
        "url": zipper,
        "externalUrl": zipper,
        "behaviorHints": {"notWebReady": True},
    }]


# -------------------------------------------------------------- handlers ---
def _mq_card(prefix, ep_title, lang, zipper):
    """The one card per language: the site's own MultiQuality player.
    This is the path that plays everywhere (in Stremio's player), so the
    V1/V2/V3 in-app mirror cards were dropped in v2.1.0 on user request."""
    lang_l = _lang_label(lang)
    return {
        "name": f"RT2 • {lang_l} • MQ",
        "title": f"{prefix} • {ep_title} — MultiQuality (site player)",
        "description": f"{prefix}\n◈ {lang_l}\n◈ MQ player — plays in app",
        "url": zipper,
        "externalUrl": zipper,
        "behaviorHints": {"notWebReady": True},
    }


# -------------------------------------------------- other-catalog streams --
# Stremio asks us for streams with foreign ids too (tt... from Cinemeta's
# catalogs).  Resolve the title via the free public Cinemeta API and fuzzy-
# match it against the site index, so streams appear on OTHER catalogs too
# (v2.1.0, user request).  No API key needed.
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
    n = rec["name"]
    n = re.sub(r"\s*[-–]?\s*(hindi|tamil|telugu|multi)\b.*$", "", n, flags=re.I)
    n = re.sub(r"\s*season\s*\d+\s*$", "", n, flags=re.I)
    return n.strip(" -–")


def _match_show(title, season):
    tn = _norm_title(title)
    if len(tn) < 4:
        return None
    best, best_score = None, -1
    for rec in SHOWS.values():
        bn = _norm_title(_base_title(rec))
        if not bn:
            continue
        if len(min(bn, tn, key=len)) < 4 and bn != tn:
            continue
        if bn in tn or tn in bn:
            has = season is None or any(k[0] == season for k in rec["eps"])
            score = (2 if has else 0) + (3 if bn == tn else 0)
            if score > best_score:
                best, best_score = rec, score
    return best


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


def handle_manifest():
    base = _public_base_holder.get("base") or ""
    return {
        "id": ADDON_ID,
        "version": VERSION,
        "name": "RareToons 2.0",
        "description": ("Anime & toons in Hindi / Tamil / Telugu — rebuilt "
                        "from scratch. In-app playback + site player fallback."),
        "logo": (base + "/logo.png") if base else "https://5a16d5684c14-raretoons.baby-beamup.club/logo.png",
        "background": None,
        "types": ["movie", "series"],
        "resources": ["catalog", "meta", "stream"],
        "idPrefixes": ["raretoons2", "tt"],
        "catalogs": [
            {"type": "series", "id": "rt2_series", "name": "RareToons Series",
             "extra": [{"name": "search", "isRequired": False},
                       {"name": "skip", "isRequired": False}]},
            {"type": "movie", "id": "rt2_movies", "name": "RareToons Movies",
             "extra": [{"name": "search", "isRequired": False},
                       {"name": "skip", "isRequired": False}]},
        ],
        "behaviorHints": {"configurable": False},
    }


def handle_catalog(ctype, cid, search="", skip=""):
    pool = SERIES if cid.endswith("series") else MOVIES
    term = (search or "").strip().lower()
    if term:
        pool = [s for s in pool if term in s["site_name"].lower()
                or term in s["name"].lower()]
    try:
        off = max(0, int(skip or 0))
    except Exception:
        off = 0
    metas = []
    for s in pool[off:off + 100]:
        metas.append({
            "id": f"raretoons2:{s['slug']}",
            "type": "movie" if s["movie"] else "series",
            "name": s["name"],
            "poster": s["poster"] or None,
            "description": (f"{s['name']} — Hindi / Tamil / Telugu dubs via "
                            "RareToons (site mirror)."),
        })
    metas = [m for m in metas if m["poster"]]
    return {"metas": metas}


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
    """Streams for our own ids (raretoons2:...) AND foreign ids
    (tt... from other catalogs' metas, resolved via Cinemeta).
    v2.1.0: ONE card per language - the site's MQ player - exactly the
    path that plays; V1/V2/V3 mirror cards were dropped."""
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
        return {"streams": []}
    prefix = (f"S{s_filter:02d}E{e_filter:02d}" if s_filter is not None
              else f"E{(rows[0].get('episode') or 1):02d}")
    ep_title = rows[0].get("ep_title") or rec["name"]
    streams, seen, langs = [], set(), set()
    for r in rows:
        lang = _lang_label(r.get("lang"))
        if lang in langs:
            continue
        z = r.get("mq") or r.get("sb")
        if not z:
            continue
        langs.add(lang)
        c = _mq_card(prefix, ep_title, lang, z)
        if c["url"] not in seen:
            seen.add(c["url"])
            streams.append(c)
    return {"streams": streams}


_public_base_holder = {}

# ------------------------------------------------------- /s/ byte proxy ----
_pick_cache = {}
_pick_lock = threading.Lock()



def _stream_proxy(handler, zipper, index, ext):
    """Byte-range proxy with validation + failover + range normalizer.
    Media bytes flow through the addon - the path that plays everywhere."""
    sources = resolve_zipper(zipper)
    if not sources:
        handler._send({"error": "stream unavailable - open the "
                                "Site Player card"}, 502)
        return
    order = []
    if 0 <= index < len(sources):
        order.append(index)
    order += [i for i in range(len(sources)) if i != index]
    # candidate loop: peek the first chunk; an HTML body means the origin
    # handed us a player page instead of a file -> fail over to next.
    for i in order:
        cand = sources[i]["url"]
        up = None
        try:
            rng0 = handler.headers.get("Range")
            up = plain_get(cand, rng=rng0, timeout=30)
            if up.status_code not in (200, 206, 416):
                up.close()
                continue
            peek = b""
            for ch in up.iter_content(64 * 1024):
                peek = ch[:16]
                break
            if peek.lstrip()[:4] in (b"<!DO", b"<htm", b"<?xm", b"<html"):
                up.close()
                handler.log_message("html-pass %s -> next", cand[:60])
                continue
            _relay(handler, up, peek, ext, cand, rng0)
            return
        except (BrokenPipeError, ConnectionResetError):
            return
        except Exception:
            if up:
                up.close()
            continue
    handler._send({"error": "all servers refused"}, 502)


def _relay(handler, up, first_chunk, ext, target, rng):
    """Stream an already-opened upstream (first chunk peeked) with the
    Range normalizer for 200-full answers."""
    code = up.status_code
    ctype = up.headers.get("Content-Type")
    if not ctype or "text" in ctype.lower() or "octet-stream" in ctype.lower():
        ctype = CONTENT_TYPES.get(ext, ctype or "video/mp4")
    total_s = up.headers.get("Content-Length") or ""
    total = int(total_s) if total_s.isdigit() else 0

    def _headers(cl, cr=None):
        handler.send_response(206 if cr else code)
        handler.send_header("Content-Type", ctype)
        if cr:
            handler.send_header("Content-Range", cr)
        if cl:
            handler.send_header("Content-Length", str(cl))
        handler.send_header("Accept-Ranges", "bytes")
        handler.send_header("Access-Control-Allow-Origin", "*")
        handler.send_header("Access-Control-Expose-Headers",
                            "Content-Length, Content-Range, Accept-Ranges")
        handler.send_header("Cache-Control", "no-store")
        handler.send_header("Connection", "close")
        handler.end_headers()

    def _chunks():
        if first_chunk:
            yield first_chunk
        for ch in up.iter_content(256 * 1024):
            if ch:
                yield ch

    try:
        if code == 200 and rng:
            m2 = re.match(r"\s*bytes=(\d*)-(\d*)", rng or "")
            if m2 and (m2.group(1) or m2.group(2)) and total > 0:
                start = int(m2.group(1)) if m2.group(1) else 0
                end = int(m2.group(2)) if m2.group(2) else total - 1
                if start < total:
                    end = min(end, total - 1)
                    need = end - start + 1
                    _headers(need, f"bytes {start}-{end}/{total}")
                    if not handler._is_head:
                        skip, left = start, need
                        for ch in _chunks():
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
                    handler.log_message("norm %s %d-%d", target[:50], start, end)
                    return
        _headers(total if code == 200 else None,
                 up.headers.get("Content-Range"))
        if not handler._is_head:
            for ch in _chunks():
                handler.wfile.write(ch)
    except (BrokenPipeError, ConnectionResetError):
        pass


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
        path = unquote(parsed.path).rstrip("/") or "/"
        qs = parse_qs(parsed.query)
        _public_base_holder["base"] = public_base_from(self)
        try:
            if path in ("", "/"):
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

            m = re.match(r"^/catalog/(series|movie)/rt2_(series|movies)"
                         r"(?:/([^/]+))?\.json$", path)
            if m:
                extra = m.group(3) or ""
                search = (qs.get("search", [""])[0] if qs else "") or (
                    extra if "=" not in extra else "")
                skip = qs.get("skip", [""])[0] if qs else ""
                self._send(handle_catalog(m.group(1), m.group(2), search, skip))
                return

            m = re.match(r"^/meta/(series|movie)/(raretoons2:.+)\.json$", path)
            if m:
                meta = handle_meta(m.group(1), m.group(2))
                self._send({"meta": meta} if meta else {"meta": {}},
                           200 if meta else 404)
                return

            m = re.match(r"^/stream/(series|movie)/(raretoons2:[^/]+|tt\d+(?::\d+)*)\.json$",
                         path)
            if m:
                self._send(handle_stream(m.group(1), m.group(2)))
                return

            m = re.match(r"^/s/([A-Za-z0-9_\-]+)(\.[A-Za-z0-9]{1,5})?$", path)
            if m:
                zipper, idx = _decode_token(m.group(1))
                if not zipper.startswith("https://codedew.com/"):
                    self._send({"error": "bad token"}, 400)
                    return
                t0 = time.time()
                _stream_proxy(self, zipper, idx, (m.group(2) or ".mkv").lower())
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
