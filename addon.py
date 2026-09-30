#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RareToons 2.2 — Stremio addon (MQ-HLS in-app playback).

v2.2.0 (2026-09-30): the ONLY cards we emit are the site's MultiQuality
(MQ) player, served IN-APP as HLS.  Verified end-to-end against the site:

  1. an episode's `mq` zipper (codedew.com/zipper/?url=<fid>) page embeds
     the player iframe  https://argon.razorshell.space/embed/<id>
  2. the embed page carries  _juicycodes("<base64 blob>")  — a simple
     symbol-map cipher (ported to Python below) that decodes to the
     JWPlayer `var config = {...}` JSON, whose sources.file is the
     1080p/720p/360p HLS master (groovy.monster, #POWERED-BY JUICYCODES)
  3. master + variant playlists + MPEG-TS segments all answer 200 to the
     curl_cffi/impersonate client (plain clients get a CF 403).

The addon rewrites the whole HLS tree to same-origin /mq/... URLs, so
Stremio (desktop / Android / web) plays it like any other HLS stream.

Cards: ONE per language ("RT2 • Hindi • MQ", ...).  Fallback when the
chain cannot be resolved in time: the argon embed page itself, which
autoplays in any browser.  Streams also resolve for foreign ids
(tt... from other catalogs' metas) via the free Cinemeta API.

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
VERSION = "2.3.0"
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


def _mq_resolve(zipper):
    """zipper -> (master_m3u8_url, embed_url) or (None, embed_url).
    zipper page -> argon iframe -> embed page -> juicy blob -> config."""
    now = time.time()
    with _hls_lock:
        hit = _hls_cache.get(zipper)
        if hit and hit[0] > now:
            return hit[1], hit[2]
    embed = None
    master = None
    try:
        zp = cf_get(zipper, referer=SEG_SITE).text or ""
        m = _EMB_RE.search(zp)
        if m:
            embed = m.group(1)
            ep = cf_get(embed, referer=zipper).text or ""
            b = _BLOB_RE.search(ep)
            if b:
                blob = "".join(re.findall(r'"([A-Za-z0-9+/=_-]*)"', b.group(1)))
                dec = _juicy_decode(blob)
                mm = _MASTER_RE.search(dec)
                if mm:
                    master = mm.group(1).replace("\\/", "/")
    except Exception:
        pass
    ttl = HLS_TTL if master else NEG_TTL
    with _hls_lock:
        _hls_cache[zipper] = (now + ttl, master, embed)
    return master, embed


def _mq_resolve_many(zippers):
    out = {z: (None, None) for z in zippers}
    threads = []

    def _run(z):
        out[z] = _mq_resolve(z)

    for z in zippers:
        t = threading.Thread(target=_run, args=(z,), daemon=True)
        t.start()
        threads.append(t)
    deadline = time.time() + LIST_BUDGET
    for t in threads:
        t.join(max(0.1, deadline - time.time()))
    return out


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
def _lang_label(raw):
    lang = re.sub(r"\s*(uncut|censored)\s*$", "", raw or "", flags=re.I).strip()
    return lang or "Hindi"



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
        return {"streams": []}
    base = _public_base_holder.get("base") or ""
    prefix = (f"S{s_filter:02d}E{e_filter:02d}" if s_filter is not None
              else f"E{(rows[0].get('episode') or 1):02d}")
    ep_title = rows[0].get("ep_title") or rec["name"]

    picks = {}
    for r in rows:
        lang = _lang_label(r.get("lang"))
        z = r.get("mq") or r.get("sb")
        if z and lang not in picks:
            picks[lang] = z
    if not picks:
        return {"streams": []}
    resolved = _mq_resolve_many(list(picks.values()))

    streams, seen = [], set()
    for lang, z in picks.items():
        master, embed = resolved.get(z, (None, None))
        if master:
            url = f"{base}/mq/{_mq_token(z)}.m3u8"
            streams.append({
                "name": f"RT2 • {lang} • MQ",
                "title": f"{prefix} • {ep_title} — MultiQuality (HLS, in-app)",
                "description": (f"{prefix}\n◈ {lang}\n◈ MQ 1080p/720p/360p "
                                "\n◈ plays inside the app"),
                "url": url,
                "behaviorHints": {
                    "notWebReady": False,
                    "bingeGroup": f"rt2|mq|{lang.lower()}",
                    "filename": f"{prefix}.m3u8",
                },
            })
        else:
            # no HLS in budget: the show hub is the site's own working UI
            # (some language links sit behind its 3-step link-scan wall,
            # so a bare zipper would land on the ad interstitial)
            target = embed or rec.get("hub") or z
            streams.append({
                "name": f"RT2 • {lang} • MQ",
                "title": f"{prefix} • {ep_title} — MQ (opens website)",
                "description": (f"{prefix}\n◈ {lang}\n◈ opens the show page "
                                "on the site (pick the episode there)"),
                "url": target,
                "externalUrl": target,
                "behaviorHints": {"notWebReady": True},
            })
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
    for line in text.splitlines():
        ls = line.strip()
        if not ls:
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
            out.append(f"{proxy_prefix}/u/{_u64(u)}{ext}")
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


def _serve_m3u8_child(handler, zipper, b64url, ext):
    target = _u64d(b64url)
    if not target.startswith("http"):
        handler._send({"error": "bad child"}, 400)
        return
    proxy_prefix = f"/mq/{_mq_token(zipper)}"
    rng = handler.headers.get("Range")
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
                                  (m.group(3) or ".ts").lower())
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
