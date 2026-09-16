#!/usr/bin/env python3
"""
RareToons Stremio addon with TMDB support (v1.8)
================================================
Indexes streams from rareanimes.com (+ store.animetoonhindi.com).

v1.8 changes (from v1.7):
  - MultiQuality streams are listed AGAIN, but as real in-app HLS:
    `/hls/{token}.m3u8` (the full chain - master -> variants -> segments
    / AES keys - is proxied + rewritten through the addon, CORS-open, with
    the player-page Referer, so Stremio Web/Chromecast/Android/TV can play
    the adaptive multi-quality stream instead of opening a browser tab).
  - Rewritten HLS child URIs keep their container extension
    (`/hls/{token}/u/<b64>.ts` / `.m3u8` / `.bin`), so ExoPlayer/VLC/hls.js
    can tell a TS segment from a playlist; segment Content-Type is now
    inferred from the extension (`.ts` -> `video/mp2t`).
  - `/hls/{token}.m3u8` 302s (like `/watch`) when the MultiQuality zipper has
    no HLS master, instead of serving a non-HLS body as a playlist.

v1.7 changes (from v1.6):
  - Only Server v1, v2, v3 are listed (capped at MAX_SERVERS=3).
  - MultiQuality (HLS/m3u8) streams are no longer listed.
  - "Proxy (Web/Cast)" renamed to "Web+Cast".
  - Only direct file streams (.mkv/.mp4) are listed.

Fundamentals
------------
An episode row carries a codedew.com/zipper URL (StreamBeta `sb`).
Resolving one means: GET the zipper (302 -> codedew.com/streambeta/?url=<fileId>),
POST {"fileId": ...} and read the v1..v3 player-source arrays; every entry
is a signed, expiring URL to the actual .mkv/.mp4 file.

Three rules follow from that:

1. ONLY SERVERS v1, v2, v3 ARE LISTED. The payload mixes streaming sources
   with download mirrors (mega/mediafire/zip, `download_url` fields) -
   downloads are filtered out, HLS (.m3u8) sources are skipped. Dead servers
   are not hidden; they fail over at playback.

2. LISTING MUST BE FAST. Listing is latency-bounded (LIST_BUDGET, shared
   by the whole request), never probes, is served from a
   stale-while-revalidate cache, dedupes in-flight resolves, prefetches
   the next episodes, and remembers each zipper's server shape across
   restarts (stream_hints.json).

3. PLAYBACK MUST WORK EVERYWHERE, WITH ZERO MEDIA BYTES THROUGH THE ADDON.
   /watch/{token}[.ext] re-resolves, probes and fails over, then 302s the
   player to a live CDN URL - the player talks to the CDN directly. The
   byte-range /proxy route and the in-app /hls chain (MultiQuality) still
   exist but are OFF by default (EXPOSE_PROXY_STREAMS / MQ_INAPP_HLS):
   the juicy.codes CDN IP-locks signed URLs to the resolver, so serving
   that chain always meant proxying every segment through this server.

TMDB integration (like Stremio addons do): catalog + meta + poster fetch.
Key from env TMDB_API_KEY, URL prefix /{key}/manifest.json or ?tmdbApiKey=.

Protocol: standard Stremio addon (manifest.json + /catalog + /meta + /stream)
Run:  python3 addon.py [port]     (binds 0.0.0.0)
"""
import base64
import concurrent.futures as cf
import difflib
import json
import os
import random
import re
import sys
import threading
import time
import unicodedata
from collections import defaultdict, Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote, quote, urljoin

import requests

# Optional Chrome-TLS impersonation. The MultiQuality player's CDN
# (juicy.codes platform) validates the client's TLS fingerprint: a plain
# `requests`/urllib client receives 403 "Invalid signature" even with a
# perfectly valid signed URL, while a Chrome-fingerprinted client (curl_cffi
# impersonate="chrome") gets 200s for the same URL. Without curl_cffi the
# addon degrades to the existing behaviour (browser-player fallback).
try:
    from curl_cffi import requests as _curl_requests
except Exception:                                 # not installed / unsupported
    _curl_requests = None

ROOT = Path(__file__).parent
HUB_INDEX = ROOT / "episodes_index.jsonl"
STREAMS = ROOT / "streams.json"
TMDB_CACHE_FILE = ROOT / "tmdb_cache.json"
PORT = int(os.environ.get("PORT") or (sys.argv[1] if len(sys.argv) > 1 else 8080))

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")


def _env_int(name, default):
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default


def _env_float(name, default):
    try:
        return float(os.environ.get(name, "") or default)
    except ValueError:
        return default


def _env_flag(name, default=False):
    v = (os.environ.get(name) or "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "on")


SB_TTL = _env_int("SB_TTL", 4 * 3600)       # cache a resolved server list
FAIL_TTL = _env_int("FAIL_TTL", 120)        # cache a failed resolution briefly (never 6h)
STALE_TTL = _env_int("STALE_TTL", 24 * 3600)   # serve-stale-while-revalidate window
PICK_TTL = _env_int("PICK_TTL", 15 * 60)    # remember the URL a /watch token redirected to
HINT_TTL = _env_int("HINT_TTL", 14 * 24 * 3600)  # remember a zipper's server shape
RESOLVE_TIMEOUT = _env_int("RESOLVE_TIMEOUT", 12)
PROBE_TIMEOUT = _env_int("PROBE_TIMEOUT", 8)
RESOLVE_CONCURRENCY = _env_int("RESOLVE_CONCURRENCY", 3)  # small bursts (Cloudflare punishes bursts)
UPSTREAM_DOWN_WINDOW = _env_int("UPSTREAM_DOWN_WINDOW", 60)  # circuit breaker window

# ---- latency budget -------------------------------------------------------
# /stream must answer FAST. Nothing in the listing path is allowed to block
# on upstream longer than LIST_BUDGET seconds *in total* (shared deadline for
# all rows of one request). Anything not resolved by then is still listed -
# its /watch/{token} URL resolves + fails over at PLAYBACK time.
LIST_BUDGET = _env_float("LIST_BUDGET", 2.0)
# Live-probing every source costs one HTTP round trip per server; at listing
# time that is the single biggest source of "it takes too long to fetch
# streams". Probing now happens at playback (/watch) where it costs nothing
# perceivable, with automatic failover to another server.
VERIFY_ON_LIST = _env_flag("VERIFY_ON_LIST", False)
PLAYBACK_VERIFY = _env_flag("PLAYBACK_VERIFY", True)
# When a zipper has never been resolved we still advertise its servers
# immediately (resolved lazily at playback).
DEFAULT_SERVER_SLOTS = _env_int("DEFAULT_SERVER_SLOTS", 4)
PREFETCH_WORKERS = _env_int("PREFETCH_WORKERS", 6)
# Hard cap for playback-time resolution: players give up on a redirect that
# never arrives, so never hang a /watch request forever.
PLAYBACK_BUDGET = _env_float("PLAYBACK_BUDGET", 25.0)
PREFETCH_ENABLED = _env_flag("PREFETCH_ENABLED", True)
# Expose an extra CORS-friendly proxied entry (Stremio Web / Chromecast /
# strict Android players that dislike cross-origin redirects).
EXPOSE_PROXY_STREAMS = _env_flag("EXPOSE_PROXY_STREAMS", False)
# In-app MultiQuality HLS chain: the juicy.codes CDN IP-locks its signed
# segment URLs to the resolver, so /hls can only serve the chain THROUGH
# this server = real bandwidth on every MQ playback. Default OFF
# (zero-media-bytes); set MQ_INAPP_HLS=1 to restore the old behaviour.
MQ_INAPP_HLS = _env_flag("MQ_INAPP_HLS", False)
HINTS_FILE = ROOT / "stream_hints.json"

# --------------------------------------------------------------------------
# TMDB config - like Stremio addons do
# --------------------------------------------------------------------------
DEFAULT_TMDB_KEY = "1af06616dcbb28ff03088d87d63211f5"
TMDB_API_KEY = os.environ.get("TMDB_API_KEY") or os.environ.get("TMDB_KEY") or DEFAULT_TMDB_KEY
TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500"
TMDB_BG_BASE = "https://image.tmdb.org/t/p/w1280"
TMDB_CACHE_TTL = 7 * 24 * 3600
TMDB_SEARCH_TTL = 3 * 24 * 3600

_tl = threading.local()
_tmdb_cache = {}
_tmdb_lock = threading.Lock()
_tmdb_cache_dirty = False

def _load_tmdb_cache():
    global _tmdb_cache
    if TMDB_CACHE_FILE.exists():
        try:
            data = json.loads(TMDB_CACHE_FILE.read_text())
            _tmdb_cache = data
            now = time.time()
            expired = [k for k, (exp, _) in _tmdb_cache.items() if exp < now]
            for k in expired:
                _tmdb_cache.pop(k, None)
            print(f"[tmdb] cache loaded {len(_tmdb_cache)} entries, purged {len(expired)}", flush=True)
        except Exception as e:
            print(f"[tmdb] cache load failed: {e}", flush=True)
            _tmdb_cache = {}

def _save_tmdb_cache():
    global _tmdb_cache_dirty
    try:
        with _tmdb_lock:
            TMDB_CACHE_FILE.write_text(json.dumps(_tmdb_cache))
            _tmdb_cache_dirty = False
    except Exception as e:
        print(f"[tmdb] cache save failed: {e}", flush=True)

def _tmdb_cache_get(key):
    with _tmdb_lock:
        ent = _tmdb_cache.get(key)
        if ent and ent[0] > time.time():
            return ent[1]
        elif ent:
            _tmdb_cache.pop(key, None)
    return None

def _tmdb_cache_set(key, value, ttl=TMDB_CACHE_TTL):
    global _tmdb_cache_dirty
    with _tmdb_lock:
        _tmdb_cache[key] = (time.time() + ttl, value)
        _tmdb_cache_dirty = True
        if len(_tmdb_cache) % 20 == 0:
            threading.Thread(target=_save_tmdb_cache, daemon=True).start()

def _get_effective_tmdb_key():
    return getattr(_tl, 'tmdb_key', None) or TMDB_API_KEY

def tmdb_request(path, params=None, cache_key=None, ttl=TMDB_CACHE_TTL, timeout=2.5):
    """GET TMDB with caching. Fast timeout for offline env."""
    if cache_key:
        cached = _tmdb_cache_get(cache_key)
        if cached is not None:
            return cached
    api_key = _get_effective_tmdb_key()
    if not api_key:
        return None
    try:
        q = dict(params or {})
        q['api_key'] = api_key
        for attempt in range(2):
            try:
                r = requests.get(f"{TMDB_BASE}{path}", params=q, timeout=timeout,
                                 headers={"User-Agent": UA, "Accept": "application/json"})
                if r.status_code == 200:
                    data = r.json()
                    if cache_key:
                        _tmdb_cache_set(cache_key, data, ttl)
                    return data
                elif r.status_code in (429, 503):
                    time.sleep(0.3 + attempt * 0.3)
                    continue
                else:
                    return None
            except requests.RequestException:
                time.sleep(0.15)
        return None
    except Exception:
        return None

def tmdb_search_tv(query):
    if not query:
        return None
    ck = f"search:tv:{query.lower().strip()}"
    return tmdb_request("/search/tv", {"query": query, "include_adult": False, "language": "en-US"},
                        cache_key=ck, ttl=TMDB_SEARCH_TTL, timeout=3)

def tmdb_search_movie(query):
    if not query:
        return None
    ck = f"search:movie:{query.lower().strip()}"
    return tmdb_request("/search/movie", {"query": query, "include_adult": False, "language": "en-US"},
                        cache_key=ck, ttl=TMDB_SEARCH_TTL, timeout=3)

def tmdb_search_multi(query):
    if not query:
        return None
    ck = f"search:multi:{query.lower().strip()}"
    return tmdb_request("/search/multi", {"query": query, "include_adult": False, "language": "en-US"},
                        cache_key=ck, ttl=TMDB_SEARCH_TTL, timeout=3)

def tmdb_find_by_imdb(imdb_id):
    ck = f"find:{imdb_id}"
    return tmdb_request(f"/find/{imdb_id}", {"external_source": "imdb_id"},
                        cache_key=ck, ttl=TMDB_CACHE_TTL, timeout=3)

def tmdb_tv_details(tmdb_id):
    ck = f"tv:{tmdb_id}"
    return tmdb_request(f"/tv/{tmdb_id}", {"language": "en-US"}, cache_key=ck, timeout=3)

def tmdb_movie_details(tmdb_id):
    ck = f"movie:{tmdb_id}"
    return tmdb_request(f"/movie/{tmdb_id}", {"language": "en-US"}, cache_key=ck, timeout=3)

def tmdb_tv_external_ids(tmdb_id):
    ck = f"tv:ext:{tmdb_id}"
    return tmdb_request(f"/tv/{tmdb_id}/external_ids", {}, cache_key=ck, timeout=3)

def tmdb_movie_external_ids(tmdb_id):
    ck = f"movie:ext:{tmdb_id}"
    return tmdb_request(f"/movie/{tmdb_id}/external_ids", {}, cache_key=ck, timeout=3)

def get_imdb_from_tmdb(type_hint, tmdb_id):
    try:
        if type_hint == "movie":
            ext = tmdb_movie_external_ids(tmdb_id)
        else:
            ext = tmdb_tv_external_ids(tmdb_id)
        if ext:
            return ext.get("imdb_id")
    except Exception:
        pass
    return None

def get_tmdb_from_imdb(imdb_id):
    data = tmdb_find_by_imdb(imdb_id)
    if not data:
        return None
    if data.get("movie_results"):
        mr = data["movie_results"][0]
        return ("movie", mr["id"], mr)
    if data.get("tv_results"):
        tr = data["tv_results"][0]
        return ("series", tr["id"], tr)
    return None

def enrich_show_from_tmdb(display_name, type_hint, api_key_override=None, use_network=True):
    """Search TMDB for show, return enriched dict or None. Uses cache. If use_network=False, only cache."""
    if api_key_override:
        _tl.tmdb_key = api_key_override
    try:
        # check enrich cache first
        # clean query for cache key
        q_clean = re.sub(r"(?i)\b(hindi|tamil|telugu|dubbed|episodes|season \d+|download|hd)\b", " ", display_name)
        q_clean = re.sub(r"\s+", " ", q_clean).strip()
        enrich_ck = f"enrich:{type_hint}:{show_key(display_name)}"
        cached_enrich = _tmdb_cache_get(enrich_ck)
        if cached_enrich:
            return cached_enrich

        if not use_network:
            return None

        q = q_clean
        search_res = None
        if type_hint == "movie":
            search_res = tmdb_search_movie(q)
            if not search_res or not search_res.get("results"):
                short_q = " ".join(q.split()[:4])
                if short_q != q:
                    search_res = tmdb_search_movie(short_q)
        else:
            search_res = tmdb_search_tv(q)
            if not search_res or not search_res.get("results"):
                short_q = " ".join(q.split()[:4])
                if short_q != q:
                    search_res = tmdb_search_tv(short_q)
            if not search_res or not search_res.get("results"):
                search_res = tmdb_search_multi(q)

        if not search_res or not search_res.get("results"):
            return None
        results = search_res["results"]
        best = None
        for r in results[:5]:
            mt = r.get("media_type")
            if type_hint == "movie" and mt and mt != "movie":
                continue
            if type_hint == "series" and mt and mt not in ("tv", None):
                continue
            best = r
            break
        if not best:
            best = results[0]

        tmdb_id = best["id"]
        if type_hint == "movie" or best.get("media_type") == "movie" or "title" in best:
            details = tmdb_movie_details(tmdb_id) or best
            imdb_id = get_imdb_from_tmdb("movie", tmdb_id)
            name = details.get("title") or best.get("title") or display_name
            poster = details.get("poster_path") or best.get("poster_path")
            backdrop = details.get("backdrop_path") or best.get("backdrop_path")
            overview = details.get("overview") or best.get("overview")
            year = (details.get("release_date") or best.get("release_date") or "")[:4]
            genres = [g["name"] for g in details.get("genres", [])] if details.get("genres") else []
            rating = details.get("vote_average")
        else:
            details = tmdb_tv_details(tmdb_id) or best
            imdb_id = get_imdb_from_tmdb("series", tmdb_id)
            name = details.get("name") or best.get("name") or display_name
            poster = details.get("poster_path") or best.get("poster_path")
            backdrop = details.get("backdrop_path") or best.get("backdrop_path")
            overview = details.get("overview") or best.get("overview")
            year = (details.get("first_air_date") or best.get("first_air_date") or "")[:4]
            genres = [g["name"] for g in details.get("genres", [])] if details.get("genres") else []
            rating = details.get("vote_average")

        enriched = {
            "tmdb_id": tmdb_id,
            "imdb_id": imdb_id,
            "name": name,
            "poster_path": poster,
            "backdrop_path": backdrop,
            "poster": f"{TMDB_IMAGE_BASE}{poster}" if poster else None,
            "background": f"{TMDB_BG_BASE}{backdrop}" if backdrop else None,
            "overview": overview,
            "year": year,
            "genres": genres,
            "rating": rating,
            "raw": best,
            "details": details,
        }
        _tmdb_cache_set(enrich_ck, enriched, ttl=TMDB_CACHE_TTL)
        return enriched
    finally:
        if api_key_override:
            try:
                delattr(_tl, 'tmdb_key')
            except AttributeError:
                pass

# --------------------------------------------------------------------------
# name normalization / matching
# --------------------------------------------------------------------------
STOPWORDS = {
    "hindi", "tamil", "telugu", "bengali", "malayalam", "english", "dub",
    "dubbed", "dubbedepisodes", "episodes", "episode", "season", "seasons",
    "download", "watch", "hd", "fhd", "uhd", "480p", "720p", "1080p", "360p",
    "4k", "in", "the", "a", "an", "of", "and", "all", "complete", "free",
    "quality", "multi", "quality", "original", "fan", "censored", "uncut",
    "sony", "yay", "yay!", "tvrip", "rip", "x264", "h264", "ep", "eps",
    "raretoons", "raitoons", "toons", "toon", "india", "series", "new",
    "latest", "2002", "2007", "2009", "2017",
}

def strip_accents(s):
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if not unicodedata.combining(c))

def norm_text(s):
    s = strip_accents(s or "").lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return s.strip()

def base_tokens(title):
    s = norm_text(title)
    s = re.sub(r"\bseason\s*\d{1,3}\b", " ", s)
    s = re.sub(r"\bs\d{1,2}\b", " ", s)
    s = re.sub(r"\bmovie\s*\d{0,2}\b", " ", s)
    s = re.sub(r"\b(?:s|ep)\s*\d{1,3}\b", " ", s)
    toks = []
    for t in s.split():
        if t in STOPWORDS or t.isdigit():
            continue
        toks.append(t)
    return toks

def show_key(title):
    return " ".join(base_tokens(title))

def clean_display_title(title):
    t = title or ""
    t = re.sub(r"\s*[-–—]\s*Hindi Dubbed.*$", "", t, flags=re.I)
    t = re.sub(r"\s*Hindi Dubbed.*$", "", t, flags=re.I)
    t = re.sub(r"\s*\(?\d{3,4}p.*?\)?\s*$", "", t, flags=re.I)
    t = re.sub(r"\s*Episodes Download.*$", "", t, flags=re.I)
    t = re.sub(r"\s*Download HD.*$", "", t, flags=re.I)
    t = re.sub(r"\s*–\s*Rare.*$", "", t, flags=re.I)
    t = re.sub(r"\s+", " ", t).strip()
    return t[:120]

def clean_lang(l):
    l = (l or "").strip()
    if (not l or len(l) > 25 or l[0] in "📰🍂🎞🌐🔊🎬"
            or l.lower().startswith(("watch", "stream", "default"))):
        return ""
    return re.sub(r"\s*(uncut|censored)\s*$", "", l, flags=re.I).strip()

def match_score(query, base):
    if not query or not base:
        return 0.0
    if query == base:
        return 1.0
    qt, bt = set(query.split()), set(base.split())
    inter = qt & bt
    if qt and qt <= bt and len(qt) >= 1:
        return 0.9
    if bt and bt <= qt:
        return 0.85 if len(bt) >= 1 else 0.0
    r = difflib.SequenceMatcher(None, query, base).ratio()
    return max(r * 0.9, (len(inter) / max(len(qt), 1)) * 0.5)

# --------------------------------------------------------------------------
# index
# --------------------------------------------------------------------------
S_E_RE = re.compile(r"\bS(\d{1,2})E(\d{1,3})\b", re.I)


def _variant_add(row, src):
    """Remember one distinct (sb, mq, ep_title) source-row on `row`.

    Some hub pages carry TWO numbering schemes for the same (episode, lang)
    — e.g. an old block whose "Episode 4" video is actually season 2, plus a
    re-uploaded block with the correct season/episode files. The first row
    is not necessarily the right one, so we keep every distinct variant and
    elect at resolve time (see resolve_one / _sb_episode_verdict).
    """
    key = (src.get("sb"), src.get("mq"))
    if not key[0] and not key[1]:
        return
    variants = row.setdefault("variants", [])
    for v in variants:
        if (v.get("sb"), v.get("mq")) == key:
            if not v.get("ep_title") and src.get("ep_title"):
                v["ep_title"] = src["ep_title"]
            return
    variants.append({"sb": key[0], "mq": key[1],
                     "ep_title": src.get("ep_title") or ""})


def _merge_rows(rows):
    """Deduplicate index rows per (episode, lang), merging URL fields.

    Keeps the first non-None sb/mq/hub_url/store (today's behaviour) AND a
    `variants` list of every distinct (sb, mq, ep_title) source-row so the
    resolver can elect the one whose content matches the episode.
    """
    seen = {}
    for r in rows:
        k = (r["episode"], (r["lang"] or "").strip().lower())
        if k in seen:
            t = seen[k]
            for f in ("sb", "mq", "hub_url", "store"):
                if not t.get(f) and r.get(f):
                    t[f] = r[f]
            if not t.get("ep_title") and r.get("ep_title"):
                t["ep_title"] = r["ep_title"]
            _variant_add(t, r)
        else:
            seen[k] = dict(r)
            _variant_add(seen[k], seen[k])
    return list(seen.values())


def load_index():
    series = defaultdict(list)
    movies = defaultdict(list)
    shows = set()
    show_titles = defaultdict(list)

    if HUB_INDEX.exists():
        for line in HUB_INDEX.read_text().splitlines():
            if not line.strip():
                continue
            r = json.loads(line)
            key = show_key(r["show"])
            if not key:
                continue
            show_titles[key].append(r["show"])
            row = {
                "season": r.get("season"),
                "episode": r.get("episode"),
                "ep_title": r.get("ep_title", ""),
                "lang": clean_lang(r.get("lang")),
                "sb": r.get("sb"),
                "mq": r.get("mq"),
                "hub_url": r.get("hub_url"),
            }
            if not row["sb"] and not row["mq"]:
                continue   # download-only row (ZIP/DL) - streaming files only
            if "movie" in r["show"].lower() and (r.get("season") in (None, 1)) \
                    and r.get("episode") in (None, 1):
                movies[key].append(row)
            elif r.get("season") is not None:
                series[(key, r["season"])].append(row)
            else:
                series[(key, 0)].append(row)
            shows.add(key)

    if STREAMS.exists():
        d = json.loads(STREAMS.read_text())
        for e in d["episode_streams"]:
            page = e.get("page", "")
            pl = page.lower()
            if "mega" in pl or "mediafire" in pl or "filepress" in pl:
                continue
            m = S_E_RE.search(e.get("episode", "")) or S_E_RE.search(page)
            key = show_key(page)
            if not key:
                continue
            show_titles[key].append(page)
            stream_url = e.get("stream")
            if not stream_url or "codedew.com/zipper" not in stream_url.lower():
                continue   # streaming files only - no downloads
            # Store streams are codedew.com zipper URLs — they could be either
            # StreamBeta or MultiQuality. We store them in mq field and the
            # resolve_one function will try to resolve them as StreamBeta if
            # no sb URL exists.
            row = {"season": int(m.group(1)) if m else None,
                   "episode": int(m.group(2)) if m else None,
                   "ep_title": e.get("episode", ""),
                   "lang": "",
                   "sb": None,
                   "mq": stream_url,
                   "hub_url": None,
                   "store": e.get("source")}
            if "movie" in pl:
                movies[key].append(row)
            elif row["season"] is not None:
                series[(key, row["season"])].append(row)
            else:
                series[(key, 0)].append(row)
            shows.add(key)

    series = {k: _merge_rows(v) for k, v in series.items()}
    movies = {k: _merge_rows(v) for k, v in movies.items()}
    fallback = defaultdict(list)
    for (key, season), rows in series.items():
        if season == 0:
            fallback[key].extend(rows)

    display = {}
    for key, titles in show_titles.items():
        cleaned = [clean_display_title(t) for t in titles if clean_display_title(t)]
        if not cleaned:
            display[key] = key.title()
        else:
            cnt = Counter(cleaned)
            display[key] = cnt.most_common(1)[0][0]

    ep_count = Counter()
    for (key, _), rows in series.items():
        ep_count[key] += len(rows)
    for key, rows in movies.items():
        ep_count[key] += len(rows)

    return series, movies, fallback, shows, display, ep_count

# --------------------------------------------------------------------------
# on-demand resolvers (rareanimes)
# --------------------------------------------------------------------------
# ---- session pool (requests.Session is not thread-safe; pool keeps TLS
# ---- state warm and lets us cap concurrent connections to the player host)
_SESSIONS = []
_SESSIONS_LOCK = threading.Lock()

# circuit breaker: after connection-level failures, stop hammering upstream
# (hammering is exactly what makes Cloudflare start 403-ing/burst-blocking us)
_upstream_down_until = 0.0


def _upstream_down():
    return time.monotonic() < _upstream_down_until


def _mark_upstream_down():
    global _upstream_down_until
    _upstream_down_until = time.monotonic() + UPSTREAM_DOWN_WINDOW


def _make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


def _checkout_session():
    with _SESSIONS_LOCK:
        if _SESSIONS:
            return _SESSIONS.pop()
    return _make_session()


def _checkin_session(s):
    with _SESSIONS_LOCK:
        if s is not None and len(_SESSIONS) < 4:
            _SESSIONS.append(s)


# Chrome-impersonated sessions (curl_cffi). Kept thread-local: one curl
# session per worker thread, reused across requests. Used for the
# MultiQuality HLS chain, whose CDN (juicy.codes) rejects the TLS
# fingerprint of plain `requests` clients with 403 "Invalid signature".
_TL_CURL = threading.local()


def _impersonated_session():
    """A curl_cffi Session with a Chrome TLS fingerprint, or None when
    curl_cffi is not installed (the caller then falls back to plain
    `requests` and the MultiQuality player degrades to the browser)."""
    if _curl_requests is None:
        return None
    s = getattr(_TL_CURL, "session", None)
    if s is None:
        s = _curl_requests.Session(impersonate="chrome")
        _TL_CURL.session = s
    return s


def _impersonated_get(url, referer=None, timeout=None, range_header=None,
                      stream=False):
    """GET via the Chrome-impersonated session. Returns a requests-like
    response or None (not installed / request failed)."""
    s = _impersonated_session()
    if s is None:
        return None
    headers = {"User-Agent": UA, "Accept": "*/*"}
    if referer:
        headers["Referer"] = referer
    if range_header:
        headers["Range"] = range_header
    try:
        return s.get(url, headers=headers, stream=stream,
                     timeout=timeout or RESOLVE_TIMEOUT, allow_redirects=True)
    except Exception:
        return None


def _retry_sleep(attempt):
    time.sleep(0.8 + attempt + random.random())


def _http(url, follow=True, referer="https://www.rareanimes.com/", retries=3):
    """GET with retry+backoff. 403/429/5xx are retried (Cloudflare burst
    blocks are usually transient); connection-level failures trip the
    circuit breaker so we stop hammering a blocking edge."""
    s = _checkout_session()
    try:
        headers = {"Referer": referer}
        last_exc = None
        for attempt in range(retries):
            try:
                r = s.get(url, allow_redirects=follow, timeout=RESOLVE_TIMEOUT, headers=headers)
                if r.status_code in (403, 429, 500, 502, 503, 504, 520, 521, 522, 524) \
                        and attempt < retries - 1:
                    _retry_sleep(attempt)
                    continue
                return r
            except (requests.ConnectionError, requests.Timeout) as e:
                last_exc = e
                if attempt < retries - 1:
                    _retry_sleep(attempt)
        _mark_upstream_down()
        raise last_exc or requests.ConnectionError("request failed")
    finally:
        _checkin_session(s)


def _http_post(url, payload, referer="https://codedew.com/", retries=2):
    s = _checkout_session()
    try:
        headers = {
            "Referer": referer,
            "Origin": "https://codedew.com",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        last_exc = None
        for attempt in range(retries):
            try:
                r = s.post(url, json=payload, headers=headers, timeout=RESOLVE_TIMEOUT)
                if r.status_code in (403, 429, 500, 502, 503, 504, 520, 521, 522, 524) \
                        and attempt < retries - 1:
                    _retry_sleep(attempt)
                    continue
                return r
            except (requests.ConnectionError, requests.Timeout) as e:
                last_exc = e
                if attempt < retries - 1:
                    _retry_sleep(attempt)
        _mark_upstream_down()
        raise last_exc or requests.ConnectionError("request failed")
    finally:
        _checkin_session(s)


def _probe_media(url):
    """Check a media URL the way a player will fetch it (browser UA, NO
    Referer - the same conditions Stremio's player fetches under). Returns
    True only if the URL serves video right now (200/206). A 403 here means
    the signed URL is dead/blocked, so that server must NOT be advertised.

    HLS playlists sometimes sit behind a Referer check (the browser player
    sends the player page as Referer), so an HLS URL is retried with the
    usual player-page referers before being declared dead.

    Some HLS CDNs (the juicy.codes MultiQuality player) validate the TLS
    fingerprint, so an HLS URL is ALSO probed through the Chrome-
    impersonated session first - a plain `requests` probe gets a synthetic
    403 there even when the stream plays fine."""
    if _is_hls_url(url):
        with _hls_referers_lock:
            ref = _hls_referers.get(url) or "https://argon.razorshell.space/"
        r = _impersonated_get(url, referer=ref, timeout=PROBE_TIMEOUT,
                              range_header="bytes=0-0")
        if r is not None:
            try:
                if r.status_code in (200, 206):
                    r.close()
                    return True
                r.close()
            except Exception:
                pass
    s = _checkout_session()
    try:
        referers = []
        if _is_hls_url(url):
            with _hls_referers_lock:
                ref = _hls_referers.get(url)
            if ref:
                referers.append(ref)
            referers += [None, "https://argon.razorshell.space/",
                         "https://www.rareanimes.com/",
                         "https://codedew.com/"]
        else:
            referers.append(None)
        for ref in referers:
            try:
                headers = {"User-Agent": UA, "Accept": "*/*", "Range": "bytes=0-0"}
                if ref:
                    headers["Referer"] = ref
                r = s.get(url, headers=headers, timeout=PROBE_TIMEOUT, stream=True)
                if r.status_code in (200, 206):
                    try:
                        r.raw.read(128)
                    except Exception:
                        pass
                    r.close()
                    return True
                r.close()
            except Exception:
                continue
        return False
    finally:
        _checkin_session(s)


# StreamBeta payloads mix STREAMING sources (v1..vN worker/R2 signed URLs)
# with DOWNLOAD mirrors (mega/mediafire/zip packs / "download_url" fields).
# Only streaming files may reach Stremio - downloads 403 or simply won't
# play in the player. ("pull streaming files, not downloads")
DOWNLOAD_HOSTS = (
    "mega.nz", "mediafire.com", "gofile.com", "1fichier.com", "dropbox.com",
    "drive.google.com", "gdrview", "gdriv.", "hugeshare", "turbobit",
    "krakenfiles", "uptobox", "dfiles", "filefactory", "pixeldrain", "dl.",
)
DOWNLOAD_EXT = (".zip", ".rar", ".7z", ".iso", ".txt", ".html", ".php", ".srt")


def _is_download_url(url):
    low = (url or "").lower().split("?")[0]
    if any(h in low for h in DOWNLOAD_HOSTS):
        return True
    if any(low.endswith(ext) for ext in DOWNLOAD_EXT):
        return True
    return False


def _player_sources_result(items):
    """Normalize every PLAYABLE STREAMING source returned by StreamBeta.

    StreamBeta returns separate v1..vN server entries. Older code stopped
    after the first entry (only v1 reached Stremio); today all of them are
    kept - but only the ones that are actually streaming files:
      * items whose only URL comes from a download_* field -> dropped
      * URLs on known download hosts / archive extensions  -> dropped
    The legacy top-level fields are kept for callers/cache compatibility.
    """
    sources = []
    seen = set()
    url_fields = (
        "stream_url", "streamUrl", "file_url", "fileUrl",
        "direct_url", "directUrl", "url", "file", "src",
    )
    download_fields = ("download_url", "downloadUrl", "dl_url", "dlUrl")
    for item in items or []:
        if isinstance(item, str):
            url, server, filename = item, "", ""
            only_download = False
        elif isinstance(item, dict):
            url = next((item.get(field) for field in url_fields
                        if isinstance(item.get(field), str) and
                        item[field].startswith(("http://", "https://"))), None)
            only_download = url is None and any(
                isinstance(item.get(field), str)
                for field in download_fields)
            server = (item.get("server") or item.get("version") or
                      item.get("source") or item.get("label") or item.get("name") or "")
            filename = item.get("filename") or item.get("title") or ""
        else:
            continue
        if not isinstance(url, str) or not url.startswith(("http://", "https://")):
            continue
        if only_download or _is_download_url(url):
            continue   # downloads are never exposed as streams
        label = str(server).lower()
        if "download" in label or label in ("dl", "zip"):
            continue
        if url in seen:
            continue
        seen.add(url)
        sources.append({
            "url": url,
            "server": str(server).strip(),
            "filename": str(filename).strip(),
            "hls": _is_hls_url(url),
        })

    if not sources:
        return None
    first = sources[0]
    return {
        "worker": first["url"],
        "r2": first["url"],
        "filename": first["filename"],
        "sources": sources,
    }


def _api_player_sources(data):
    """Collect source arrays from current and older StreamBeta payload shapes."""
    if not isinstance(data, dict):
        return []

    collected = []
    source_keys = {
        "player_sources", "playerSources", "player_sources_v1", "player_sources_v2",
        "v1_player_sources", "v2_player_sources", "server_v1", "server_v2", "v1", "v2",
    }
    for key, value in data.items():
        # Explicit names cover known payloads; the broader check keeps this
        # working when the host renames a source array but retains v1/v2.
        key_lower = str(key).lower()
        if key not in source_keys and not (
                ("source" in key_lower or "server" in key_lower) and
                ("player" in key_lower or "v1" in key_lower or "v2" in key_lower)):
            continue
        if isinstance(value, dict) and not any(
                field in value for field in (
                    "stream_url", "streamUrl", "download_url", "downloadUrl",
                    "file_url", "fileUrl", "direct_url", "directUrl",
                    "url", "file", "src")):
            values = []
            for server, grouped in value.items():
                entries = grouped if isinstance(grouped, list) else [grouped]
                for entry in entries:
                    if isinstance(entry, dict):
                        entry = dict(entry)
                        entry.setdefault("server", server)
                        values.append(entry)
        else:
            values = value if isinstance(value, list) else [value]
        for item in values:
            if isinstance(item, str):
                collected.append(item)
            elif isinstance(item, dict):
                item = dict(item)
                if "v1" in key_lower or "v2" in key_lower:
                    item.setdefault("server", "v2" if "v2" in key_lower else "v1")
                collected.append(item)
    return collected


def _extract_video_from_html(text):
    """Best-effort extraction of all direct video URLs out of a player page.

    NOTE: token character sets must cover the full base64/base64url alphabet
    (A-Za-z0-9, '-', '_', '/', '+', '='). Older patterns truncated tokens at
    '+'/'=' which produced signed URLs that 403 on playback."""
    if not text:
        return None
    # URLs in HTML attributes are &amp;-escaped
    text = text.replace("&amp;", "&")

    # everything a signed token / query string may contain (quotes, spaces,
    # angle brackets terminate a URL in HTML/JS)
    T = r"[A-Za-z0-9_\-\.~+/=&?%]+"

    # 1. Cloudflare worker signed URL (current StreamBeta format)
    #    https://<sub>.flashzipper.workers.dev/<base64 json {url,timestamp,hash}>
    #    Host part is constrained so a match cannot run across '&'.
    worker_sources = []
    for pat in (
        r'https?://[a-z0-9.-]*workers\.dev/' + T,
        r'https?://[a-z0-9.-]*flashzipper[a-z0-9.-]*/' + T,
    ):
        for m in re.finditer(pat, text):
            worker_sources.append({"stream_url": m.group(0).rstrip(" ,;\"')")})
    result = _player_sources_result(worker_sources)
    if result:
        return result

    # 2. <source src> / <video src> tags
    tag_sources = []
    for pat in (
        r'<source[^>]+src=["\']([^"\']+)[\"\']',
        r'<video[^>]+src=["\']([^"\']+)[\"\']',
    ):
        for m in re.finditer(pat, text, re.I):
            u = m.group(1)
            if u.startswith("http") and not any(x in u for x in ("vidstack", "player.js", "cdn.jsdelivr")):
                tag_sources.append({"stream_url": u})
    result = _player_sources_result(tag_sources)
    if result:
        return result

    # 3. playerSources JS array (legacy StreamBeta format)
    for pat in (
        r'playerSources\s*=\s*(\[.*?\])\s*[,;]',
        r'playerSources\s*=\s*(\[.*?\])\s*\n',
        r'(?:const|var|let)\s+playerSources\s*=\s*(\[.*?\])',
    ):
        m = re.search(pat, text, re.S)
        if m:
            try:
                srcs = json.loads(m.group(1))
                if isinstance(srcs, list):
                    result = _player_sources_result(srcs)
                    if result:
                        return result
            except Exception:
                pass

    # 4. direct video file URLs (signed googleusercontent / r2 / plain media)
    direct_sources = []
    for pat in (
        r'https?://[^"\'<>\s]+video-downloads\.googleusercontent\.com[^"\'<>\s]*',
        r'https?://[^"\'<>\s]+\.(?:mkv|mp4|webm|m3u8)(?:\?[^"\'<>\s]*)?',
        r'https?://[^"\'<>\s]+\.cloudflare\.r2\.dev[^"\'<>\s]*',
        r'https?://pub-[^"\'<>\s]+\.r2\.dev[^"\'<>\s]*',
    ):
        for m in re.finditer(pat, text, re.I):
            direct_sources.append({"stream_url": m.group(0)})
    result = _player_sources_result(direct_sources)
    if result:
        return result

    return None


def _resolve_codedew_zipper(zipper):
    """Resolve a codedew.com/zipper URL into direct, in-app playable video URLs.

    Current site flow (2026):
      1. GET  codedew.com/zipper/?url=<enc>  -> 302 -> codedew.com/streambeta/?url=<fileId>
      2. POST codedew.com/streambeta/?url=<fileId>   {"fileId": <fileId>}
         -> JSON with the v1..vN player source arrays
      3. each source's stream_url is a signed direct file
         (https://<x>.flashzipper.workers.dev/<base64> -> googleusercontent .mkv)

    Upstream calls go through _http/_http_post (retry+backoff on 403/429/5xx,
    circuit breaker on connection failures) and the result is normalized by
    _player_sources_result, which drops download mirrors (streaming files
    only). A blocked edge therefore degrades to a short-lived (FAIL_TTL)
    "no direct stream" state instead of poisoning the cache for hours.
    """
    if _upstream_down():
        return None
    res = None
    try:
        r = _http(zipper, follow=True)
        final_url = str(r.url)
        text = r.text or ""

        # The encrypted file id is base64, not a URL-safe token: it commonly
        # contains percent-encoded '/', '+', and '=' characters.  The old
        # regular expression silently truncated/ignored those ids. Only trust
        # it when the redirect actually landed on a 200 player page (not a
        # Cloudflare 403 challenge page).
        file_id = None
        if r.status_code == 200:
            file_id = parse_qs(urlparse(final_url).query).get("url", [None])[0]
            if not file_id:
                # Some redirects leave the query encoded more than once.
                m = re.search(r"[?&]url=([^&#]+)", final_url, re.I)
                if m:
                    file_id = unquote(m.group(1))
            if file_id:
                file_id = unquote(file_id).strip()

        # Strategy A: POST to the player endpoint to fetch player_sources.
        if file_id:
            candidates = []
            if "streambeta" in final_url.lower():
                candidates.append(final_url)
            candidates.append(f"https://codedew.com/streambeta/?url={quote(file_id, safe='')}")
            candidates = list(dict.fromkeys(candidates))
            api_sources = []
            for post_url in candidates:
                try:
                    rr = _http_post(post_url, {"fileId": file_id},
                                    referer=final_url or "https://codedew.com/")
                    if rr.status_code != 200:
                        continue
                    try:
                        data = rr.json()
                    except ValueError:
                        data = {}
                    api_sources.extend(_api_player_sources(data))
                except Exception:
                    continue
            res = _player_sources_result(api_sources)

        # Strategy B: parse the (possibly server-rendered) player HTML.
        if not res:
            res = _extract_video_from_html(text)

    except Exception:
        res = None
    return res


# --------------------------------------------------------------------------
# resolution cache + background resolver
# --------------------------------------------------------------------------
# Shared resolution cache for BOTH StreamBeta and MultiQuality zipper URLs
# (they use the same codedew player flow), keyed by zipper URL.
#
# Entry: zipper -> {"sources": [...], "embed": str|None, "ts": float,
#                   "exp": float, "ok": bool}
#
# Design goals
#   1. /stream NEVER blocks on the network for more than the caller's budget
#      (LIST_BUDGET, shared across the whole request). A cache miss is still
#      answered instantly with placeholder servers whose /watch token
#      resolves at playback time.
#   2. Stale entries are served immediately and refreshed in the background
#      (stale-while-revalidate), so a returning user always gets an instant
#      list.
#   3. Every StreamBeta server (v1..vN) is exposed - dead ones are not hidden
#      from the list, they simply fail over to a live server inside /watch.
_zipper_cache = {}
_resolver_lock = threading.Lock()

# zipper -> future, so N concurrent requests for the same episode cause ONE
# upstream resolve instead of N.
_inflight = {}
_inflight_lock = threading.Lock()
_resolve_pool = cf.ThreadPoolExecutor(max_workers=max(RESOLVE_CONCURRENCY, 2),
                                      thread_name_prefix="resolve")
_prefetch_pool = cf.ThreadPoolExecutor(max_workers=max(PREFETCH_WORKERS, 1),
                                       thread_name_prefix="prefetch")

# Long-lived memory of a zipper's *shape* (how many servers it has and how
# they are labelled). Survives restarts, so even a cold addon can list the
# right servers instantly and resolve them lazily at playback.
_server_hints = {}
_hints_dirty = False


def _load_hints():
    global _server_hints
    try:
        with open(HINTS_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        now = time.time()
        if isinstance(data, dict):
            _server_hints = {k: v for k, v in data.items()
                             if isinstance(v, dict) and v.get("ts", 0) + HINT_TTL > now}
    except Exception:
        _server_hints = {}


def _save_hints():
    global _hints_dirty
    try:
        with _resolver_lock:
            snapshot = dict(_server_hints)
            _hints_dirty = False
        tmp = str(HINTS_FILE) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(snapshot, fh)
        os.replace(tmp, HINTS_FILE)
    except Exception:
        pass


def _remember_shape(zipper, sources):
    """Remember server labels/filenames so the next cold listing is exact."""
    global _hints_dirty
    if not zipper or not sources:
        return
    hint = {
        "ts": time.time(),
        "servers": [{"server": s.get("server") or "",
                     "filename": s.get("filename") or "",
                     "ext": _url_ext(s.get("url"))}
                    for s in sources],
    }
    with _resolver_lock:
        _server_hints[zipper] = hint
        _hints_dirty = True
        if len(_server_hints) > 20000:
            oldest = sorted(_server_hints.items(), key=lambda kv: kv[1].get("ts", 0))
            for k, _ in oldest[:5000]:
                _server_hints.pop(k, None)


def _shape_of(zipper):
    with _resolver_lock:
        hint = _server_hints.get(zipper)
    if not hint:
        return None
    if hint.get("ts", 0) + HINT_TTL < time.time():
        return None
    return hint.get("servers") or None


VIDEO_EXT = (".mkv", ".mp4", ".webm", ".m3u8", ".mov", ".avi", ".ts", ".m4v",
             ".m4s", ".aac", ".mp3", ".fmp4", ".bin")


def _url_ext(url):
    """Container extension of a media URL/filename, '' when unknown."""
    low = (url or "").lower().split("?")[0].split("#")[0]
    for ext in VIDEO_EXT:
        if low.endswith(ext):
            return ext
    return ""


def _source_ext(source):
    return _url_ext(source.get("filename")) or _url_ext(source.get("url")) or ""


def _verify_sources(sources):
    """Keep only the sources that serve video RIGHT NOW (player-like probe)."""
    verified = []
    for src in sources or []:
        u = src.get("url")
        if u and _probe_media(u):
            verified.append(src)
    return verified


def _do_resolve(zipper):
    """Blocking upstream resolve of one zipper. Never raises.

    Returns the cache entry it also stored. All streaming servers are kept;
    download mirrors were already dropped in _player_sources_result.
    """
    now = time.time()
    sources, embed = [], None
    if not _upstream_down():
        res = _resolve_codedew_zipper(zipper)
        sources = list((res or {}).get("sources") or [])
        if VERIFY_ON_LIST and sources:
            verified = _verify_sources(sources)
            if not verified:
                # every token looked stale -> one forced fresh re-resolve
                res2 = _resolve_codedew_zipper(zipper)
                sources2 = list((res2 or {}).get("sources") or [])
                verified = _verify_sources(sources2) if sources2 else []
                if verified:
                    sources = sources2
            if verified:
                v = {s["url"] for s in verified}
                # verified servers first, the rest still listed (playback
                # re-resolves + fails over, so they are never a dead end)
                sources = [s for s in sources if s["url"] in v] + \
                          [s for s in sources if s["url"] not in v]
        if not sources:
            # MultiQuality player fallback: scrape its HLS master so the
            # quality-adaptive stream plays in-app instead of a browser tab.
            mq_hls = _resolve_mq_hls(zipper, codedew_res=res)
            if mq_hls and mq_hls.get("sources"):
                sources = mq_hls["sources"]
            else:
                legacy = _resolve_legacy_mq_embed(zipper)
                if legacy:
                    embed = legacy.get("embed")

    entry = {
        "sources": sources,
        "embed": embed,
        "ts": now,
        "ok": bool(sources or embed),
    }
    entry["exp"] = now + (SB_TTL if entry["ok"] else FAIL_TTL)
    if sources:
        _remember_shape(zipper, sources)
    with _resolver_lock:
        _zipper_cache[zipper] = entry
        if len(_zipper_cache) > 6000:
            dead = [k for k, e in _zipper_cache.items()
                    if e.get("ts", 0) + STALE_TTL < now]
            for k in dead:
                _zipper_cache.pop(k, None)
    return entry


def _resolve_future(zipper, pool=None):
    """Submit (or join) a background resolve for `zipper`; deduplicated."""
    with _inflight_lock:
        fut = _inflight.get(zipper)
        if fut is not None and not fut.done():
            return fut

        def _run():
            try:
                return _do_resolve(zipper)
            finally:
                with _inflight_lock:
                    if _inflight.get(zipper) is fut_holder[0]:
                        _inflight.pop(zipper, None)

        fut_holder = [None]
        try:
            fut = (pool or _resolve_pool).submit(_run)
        except RuntimeError:          # pool shut down -> resolve inline
            return None
        fut_holder[0] = fut
        _inflight[zipper] = fut
        return fut


def _cached_entry(zipper):
    with _resolver_lock:
        return _zipper_cache.get(zipper)


def prefetch(zipper):
    """Warm the cache for a zipper we will probably be asked about soon."""
    if not PREFETCH_ENABLED or not zipper or _upstream_down():
        return
    entry = _cached_entry(zipper)
    if entry and entry.get("exp", 0) > time.time():
        return
    _resolve_future(zipper, pool=_prefetch_pool)


def _playable(zipper, budget=None, deadline=None):
    """Resolve a zipper to streaming sources, honouring a latency budget.

    Returns {"sources": [...]} / {"embed": url} / {} exactly like before, but:

      * a FRESH cache entry is returned with zero network I/O;
      * a STALE entry is returned immediately and refreshed in the
        background (stale-while-revalidate);
      * a MISS waits at most `budget` seconds (or until `deadline`) for the
        background resolve and otherwise returns {} - the caller then lists
        the servers from the remembered shape and lets /watch resolve at
        playback time.

    budget=None means "block until resolved" (used by /watch, where we do
    want the real URL before redirecting the player).
    """
    if not zipper:
        return {}
    now = time.time()
    entry = _cached_entry(zipper)
    if entry:
        if entry.get("exp", 0) > now:
            return _entry_to_result(entry)
        if entry.get("ok") and entry.get("ts", 0) + STALE_TTL > now:
            prefetch(zipper)              # revalidate without making anyone wait
            return _entry_to_result(entry)

    fut = _resolve_future(zipper)
    if fut is None:                        # executor gone (shutdown) -> inline
        return _entry_to_result(_do_resolve(zipper))

    if budget is None and deadline is None:
        wait = None
    else:
        wait = budget if budget is not None else max(0.0, deadline - now)
        if deadline is not None and budget is not None:
            wait = min(wait, max(0.0, deadline - now))
    try:
        entry = fut.result(timeout=wait)
    except cf.TimeoutError:
        return {}                          # answer now; /watch finishes the job
    except Exception:
        return {}
    return _entry_to_result(entry)


def _entry_to_result(entry):
    if not entry:
        return {}
    if entry.get("sources"):
        return {"sources": list(entry["sources"])}
    if entry.get("embed"):
        return {"embed": entry["embed"]}
    return {}


def _resolve_legacy_mq_embed(zipper):
    """Legacy MultiQuality path: argon.razorshell.space embed (browser)."""
    try:
        r = _http(zipper, follow=False, retries=1)   # just a Location check
        loc = r.headers.get("Location", "")
        if "multiquality" in loc:
            m = re.search(r"multiquality/?\?url=([A-Za-z0-9]+)", loc)
            if m:
                return {"embed": f"https://argon.razorshell.space/embed/{m.group(1)}"}
        if "razorshell" in loc:
            return {"embed": loc}
        if loc:
            m2 = re.search(r"(https?://[^/\s]*razorshell[^/\s]*embed/[^\s]+)", loc)
            if m2:
                return {"embed": m2.group(1)}
    except Exception:
        pass
    return None


def _is_hls_url(url):
    """True when the URL points at an HLS playlist (.m3u8).

    Signed HLS URLs keep their extension before the query string, so a simple
    path check is safe (e.g. .../master.m3u8?sig=abc...).
    """
    return (url or "").lower().split("?")[0].split("#")[0].endswith(".m3u8")


# ---- JuicyCodes payload decoding ------------------------------------------
#
# The MultiQuality player page (argon.razorshell.space) no longer carries the
# HLS master URL in plain HTML: the JW-Player config (with the signed .m3u8)
# is emitted by an obfuscated  `_juicycodes("chunk"+"chunk"+...)`  call whose
# decoder lives in their player.js. The algorithm (recovered from that JS and
# verified byte-exact against it):
#
#   1. payload     = the concatenated string literal inside _juicycodes(...)
#   2. salt        = int(concat of (ord(c) - 100) for the last 3 chars)
#   3. body        = base64url-decode of payload[:-3]  (JSON-unescaped \/)
#   4. digits      = for each char of body: index in the 10-symbol alphabet
#   5. every 4-digit group g -> chr(int(g) % 1000 - salt)
#
_JUICY_ALPHABET = ("`", "%", "-", "+", "*", "$", "!", "_", "^", "=")
_JUICY_CALL_RE = re.compile(r"_juicycodes\(\s*((?:\"[^\"]*\"\s*\+?\s*)+)\)")


def _juicy_decode(payload):
    """Decode one JuicyCodes payload string to plaintext (or '')."""
    try:
        if not payload or len(payload) < 8:
            return ""
        salt = int("".join(str(ord(c) - 100) for c in payload[-3:]))
        b64 = payload[:-3].replace("_", "+").replace("-", "/")
        pad = "=" * ((-len(b64)) % 4)
        body = base64.b64decode(b64 + pad).decode("latin-1")
        digits = "".join(str(_JUICY_ALPHABET.index(c)) for c in body)
        groups = re.findall(r".{4}", digits)
        return "".join(chr(int(g) % 1000 - salt) for g in groups)
    except Exception:
        return ""


def _juicy_payloads(text):
    """Every decoded `_juicycodes("..." + "...")` blob embedded in a page."""
    out = []
    for m in _JUICY_CALL_RE.finditer(text or ""):
        chunks = re.findall(r'"([^"]*)"', m.group(1))
        decoded = _juicy_decode("".join(chunks))
        if decoded:
            out.append(decoded)
    return out


def _extract_hls_urls(text):
    """Every absolute HLS playlist URL found in a page / JS / JSON blob.

    The MultiQuality player is an HLS player, but the master URL is normally
    buried inside its script: a `hls.loadSource('...m3u8')` call, a JSON-ish
    config (`file:`, `src:`, `source:`, `url:`), a <source> tag, or a
    protocol-relative URL. JSON-escaped slashes (`\\/`) are unescaped first.
    Since the site moved to a JuicyCodes-obfuscated JW-Player config, the
    `.m3u8` often only exists inside a `_juicycodes("...")` payload - those
    are decoded and scanned too.
    """
    if not text:
        return []
    text = (text.replace("\\/", "/").replace("&amp;", "&")
                .replace("&quot;", '"').replace("&#39;", "'"))
    found = []
    for pat in (
        r"https?://[^\"'<>\\\s]+?\.m3u8[^\"'<>\\\s]*",
        r"(?<![\w:/])//[^\"'<>\\\s]+?\.m3u8[^\"'<>\\\s]*",
    ):
        for m in re.finditer(pat, text, re.I):
            u = m.group(0)
            if u.startswith("//"):
                u = "https:" + u
            u = u.rstrip(".,;)'\"")
            if _is_hls_url(u) and u not in found:
                found.append(u)
    # JuicyCodes-obfuscated player configs (the current MultiQuality player)
    for blob in _juicy_payloads(text):
        for m in re.finditer(r"https?://[^\"'<>\\\s]+?\.m3u8[^\"'<>\\\s]*",
                             blob.replace("\\/", "/"), re.I):
            u = m.group(0).rstrip(".,;)'\"")
            if _is_hls_url(u) and u not in found:
                found.append(u)
    return found


def _hls_filename(url):
    """A readable filename for an HLS URL (basename of its path)."""
    try:
        name = urlparse(url).path.rsplit("/", 1)[-1]
    except Exception:
        name = ""
    return name or "stream.m3u8"


# Remembered per-URL and per-token player-page referers for the HLS chain.
# The MultiQuality player page (argon.razorshell.space/*) is often the exact
# Referer the segment/key CDN requires; we capture it during resolve and reuse
# it (per token) when servicing every child in the chain, so strict path-based
# referer checks pass too.
_hls_referers = {}        # absolute HLS url -> exact player-page referer
_hls_chain_referer = {}   # hls token -> referer that worked for the chain

# Rewritten (same-origin) HLS playlists, cached briefly: VOD playlists are
# static text, and desktop players refetch the master + every variant
# playlist at each start/seek - serving the rewritten copy makes those
# requests instant instead of an upstream round trip each.
_HLS_PLAYLIST_TTL = 300          # seconds a rewritten playlist stays fresh
_HLS_PLAYLIST_MAX = 64           # bounded memory (~100-200KB per playlist)
_hls_playlist_cache = {}         # (token, upstream_url) -> (expires, bytes)
_hls_playlist_lock = threading.Lock()


def _hls_playlist_get(token, url):
    with _hls_playlist_lock:
        entry = _hls_playlist_cache.get((token, url))
        if not entry:
            return None
        exp, payload = entry
        if exp < time.time():
            _hls_playlist_cache.pop((token, url), None)
            return None
        return payload


def _hls_playlist_put(token, url, payload):
    with _hls_playlist_lock:
        _hls_playlist_cache[(token, url)] = (time.time() + _HLS_PLAYLIST_TTL,
                                             payload)
        while len(_hls_playlist_cache) > _HLS_PLAYLIST_MAX:
            _hls_playlist_cache.pop(next(iter(_hls_playlist_cache)))


def _warm_hls_variants(token, master_url, payload_text):
    """Pre-fetch + cache the variant playlists a master names.

    Desktop players (libmpv/ffmpeg - Stremio Desktop) fetch EVERY variant
    playlist before starting playback, so warming them right after the
    master is served removes 2-3 sequential upstream round trips from the
    player's start path. Failures are silent: the player's own request then
    takes the normal uncached path."""
    try:
        prefix = f"/hls/{token}/u/"
        kids = []
        for line in payload_text.splitlines():
            line = line.strip()
            if line.startswith(prefix) and line.endswith(".m3u8"):
                b64 = line[len(prefix):-len(".m3u8")]
                child = _unb64url(b64)
                if child and child != master_url and child not in kids:
                    kids.append(child)
        if not kids:
            return
        with _hls_referers_lock:
            ref = _hls_chain_referer.get(token) or _hls_referers.get(master_url)
        for child in kids[:4]:
            if _hls_playlist_get(token, child) is not None:
                continue
            r = _fetch_hls_child(child, ref)
            if r is None:
                continue
            try:
                body = b""
                try:
                    for chunk in r.iter_content(chunk_size=256 * 1024):
                        body += chunk
                except Exception:
                    body = getattr(r, "content", b"") or b""
            finally:
                try:
                    r.close()
                except Exception:
                    pass
            if not body.lstrip().startswith(b"#EXTM3U"):
                continue
            text = body.decode("utf-8", "replace")
            rewritten = _rewrite_hls_manifest(text, child, prefix)
            _hls_playlist_put(token, child, rewritten.encode("utf-8"))
    except Exception:
        pass


def _fetch_hls_child(url, referer=None):
    """Fetch one HLS resource for warming: Chrome-impersonated first (the
    juicy CDN fingerprints TLS), then the plain session with the site's
    player referers - the same ladder the request handler uses. Returns a
    response with a 200/206 status, or None."""
    r = _impersonated_get(url, referer=referer, timeout=RESOLVE_TIMEOUT,
                          stream=True)
    if r is not None and r.status_code in (200, 206):
        return r
    if r is not None:
        try:
            r.close()
        except Exception:
            pass
    s = _checkout_session()
    try:
        headers = {"User-Agent": UA, "Accept": "*/*"}
        if referer:
            headers["Referer"] = referer
        r = s.get(url, headers=headers, timeout=RESOLVE_TIMEOUT)
        if r.status_code in (200, 206):
            return r
    except Exception:
        pass
    finally:
        _checkin_session(s)
    return None
_hls_referers_lock = threading.Lock()


# Player pages usually fetch their source list from a small JSON endpoint
# (e.g. /api/..., /player/..., /source.json) which the HTML references.  Only
# fetch a bounded number of same-page candidates that actually look like data
# endpoints - never random URLs - and only when the page itself names them.
_HLS_API_HINT = re.compile(
    r"(?i)(api|source|player|config|embed|stream|hls|media|json|get)")
_HLS_ASSET_EXT = (".js", ".css", ".png", ".jpg", ".jpeg", ".webp", ".gif",
                  ".ico", ".svg", ".woff", ".woff2", ".mp4", ".mkv")
_MAX_HLS_CONFIG_FETCHES = _env_int("MAX_HLS_CONFIG_FETCHES", 2)


def _scan_page_endpoints(page_url, html):
    """Look for an HLS URL inside the small API/config endpoints the player
    page itself references (bounded, data-driven, same host where possible)."""
    if not html:
        return []
    cands = []
    for m in re.finditer(r"https?://[^\"'<>\\\s]+", html, re.I):
        u = m.group(0).rstrip(".,;)'\"")
        low = u.lower()
        if not _HLS_API_HINT.search(u):
            continue
        if low.endswith(_HLS_ASSET_EXT) or _is_download_url(u) or "m3u8" in low:
            continue
        if u not in cands:
            cands.append(u)
    found = []
    for u in cands[:_MAX_HLS_CONFIG_FETCHES]:
        try:
            rr = _http(u, retries=1)
            found = _extract_hls_urls(rr.text or "")
        except Exception:
            continue
        if found:
            return found
    return found


def _resolve_mq_hls(zipper, codedew_res=None):
    """MultiQuality player -> in-app HLS streams.

    The site's MultiQuality entry is a browser-only, multi-quality *HLS*
    player (argon.razorshell.space): the zipper JSON normally carries the
    direct file servers, but the quality switcher (1080p/720p/480p...) plays
    an HLS master playlist.  When the codedew payload has no playable source
    we therefore scrape the player page itself and expose the HLS master:

      1. re-use HLS sources the codedew payload already contains;
      2. follow the zipper redirect to the player page, then extract every
         `.m3u8` URL from its HTML/JS (and from data endpoints it names);
      3. return them as ordinary sources (`hls: True`) that Stremio can play
         in-app instead of opening the browser tab.

    Returns {"sources": [...]} or None.
    """
    # 1. The codedew/StreamBeta payload may already expose HLS masters.
    for src in list((codedew_res or {}).get("sources") or []):
        if _is_hls_url(src.get("url")):
            return {"sources": [dict(src)]}

    # 2. Locate the player page from the zipper redirect.
    loc = ""
    try:
        r = _http(zipper, follow=False, retries=1)
        loc = (r.headers.get("Location") or "").strip()
    except Exception:
        loc = ""
    pages = []
    if loc:
        pages.append(loc)
        m = re.search(r"multiquality/?\?url=([A-Za-z0-9]+)", loc, re.I)
        if m:
            pages.append(f"https://argon.razorshell.space/embed/{m.group(1)}")
    pages = [p for p in dict.fromkeys(pages)
             if p.startswith(("http://", "https://"))]
    # Only scrape the HLS player when the redirect actually lands on the
    # MultiQuality player. Plain StreamBeta pages were already parsed for
    # m3u8 by _resolve_codedew_zipper (strategy B), so scanning them again
    # (plus their config endpoints) would only burn upstream requests.
    if not any(p.lower().find(x) >= 0 for p in pages
               for x in ("razorshell", "multiquality", "multi-quality",
                         "quality", "/embed/")):
        return None

    found = []
    page_url = ""
    for page_url in pages:
        try:
            rr = _http(page_url, follow=True, retries=2)
            html = rr.text or ""
        except Exception:
            continue
        found = _extract_hls_urls(html)
        if not found:
            found = _scan_page_endpoints(page_url, html)
        if found:
            break

    if not found:
        return None
    sources = []
    for u in found[:12]:
        src = {"url": u, "server": "", "filename": _hls_filename(u),
               "hls": True}
        if page_url:
            src["referer"] = page_url
            # Remember the exact player-page referer per HLS URL so the
            # /hls handler and the playback probe can replay it.
            with _hls_referers_lock:
                _hls_referers[u] = page_url
        sources.append(src)
    return {"sources": sources}



def _ql_label(qtxt):
    """'1080p HEVC' / '720p' -> 'FHD 1080p'-style label."""
    m = re.search(r"(2160|1440|1080|960|720|576|480|360)", qtxt or "")
    if not m:
        return "AUTO"
    h = int(m.group(1))
    if h >= 2160:
        return "UHD 2160p"
    if h >= 1080:
        return "FHD 1080p"
    return ("HD %dp" if h >= 720 else "SD %dp") % h

def _human_size(sz):
    """v1.10.0: '350.4 MB' passes through; byte counts get units."""
    s = str(sz or "").strip()
    if not s:
        return ""
    if re.match(r"^\d+(\.\d+)?\s*[GMK]i?B$", s, re.I):
        return s
    if s.isdigit():
        b = int(s)
        for u, d in (("GB", 1e9), ("MB", 1e6)):
            if b >= d:
                return "%.1f %s" % (b / d, u)
        return "%d KB" % (b / 1e3)
    return ""

_ADDON_DISPLAY = "RareToons"     # manifest brand (full name is long)

def _fmt_card(ql, title, ep, audio, prov, size="", codec=""):
    """v1.11.0 card spec v2:
    ♧ QUALITY ✹ title / ◫ ep ◇ size ▧ codec / ◈ WEB-DL /
    ◈ audio lang / ⌗ RareToons / ⌬ server."""
    t1 = [ep]
    if size:
        t1.append("◇ %s" % size)
    if codec:
        t1.append("▧ %s" % codec)
    lines = [" ".join(t1), "◈ WEB-DL"]
    if audio:
        lines.append("◈ %s" % audio)          # audio lang (glass line)
    lines += ["⌗ %s" % _ADDON_DISPLAY, "⌬ %s" % prov]
    return ("♧ %s  ✹ %s" % (ql, title),
            "\n".join(lines))

def _server_suffix(source, index, total):
    label = str(source.get("server") or "").strip()
    if label:
        # Avoid awkward duplicated wording such as "Server Server v2".
        return label if label.lower().startswith("server") else f"Server {label}"
    return f"Server v{index + 1}" if total > 1 else ""


QUALITY_RE = re.compile(r"(2160p|1440p|1080p|720p|480p|360p|4k|hevc|x265|10bit)", re.I)


def _quality_of(source):
    text = f"{source.get('filename') or ''} {source.get('server') or ''} {source.get('url') or ''}"
    tags = []
    for m in QUALITY_RE.finditer(text):
        t = m.group(1).upper().replace("4K", "2160p")
        if t.endswith("P"):
            t = t[:-1] + "p"
        if t not in tags:
            tags.append(t)
    return " ".join(tags[:2])


def _token_for(zipper, index=0):
    return base64.urlsafe_b64encode(f"{zipper}\x00{index}".encode()).decode().rstrip("=")


def _watch_url(base, zipper, index=0, ext=""):
    """Playback URL that re-resolves at click time.

    The container extension is appended when known: Android (ExoPlayer),
    VLC and several TV players pick their demuxer from the URL suffix, so
    `/watch/<token>.mkv` plays where a bare token would not.
    """
    if not base or not zipper:
        return None
    if ext and not ext.startswith("."):
        ext = "." + ext
    return f"{base}/watch/{_token_for(zipper, index)}{ext}"


def _proxy_url(base, zipper, index=0, ext=""):
    """Same source, streamed THROUGH the addon (CORS + Range friendly)."""
    if not base or not zipper:
        return None
    if ext and not ext.startswith("."):
        ext = "." + ext
    return f"{base}/proxy/{_token_for(zipper, index)}{ext}"


def _hls_proxy_url(base, zipper):
    """HLS master through the addon: /hls/{token}.m3u8 rewrites every child
    URI (variants, segments, AES keys, EXT-X-MAP) to same-origin /hls paths,
    so Stremio Web / Chromecast / strict players can play the multi-quality
    adaptively without CORS or Referer headaches."""
    if not base or not zipper:
        return None
    return f"{base}/hls/{_token_for(zipper, 0)}.m3u8"


def _b64url_of(text):
    try:
        raw = text.encode("utf-8") if isinstance(text, str) else text
    except Exception:
        raw = str(text).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _unb64url(tok):
    try:
        pad = "=" * (-len(tok) % 4)
        return base64.urlsafe_b64decode((tok + pad).encode()).decode("utf-8")
    except Exception:
        return ""


_HLS_CHILD_EXTS = (".m3u8", ".m3u", ".ts", ".mpegts", ".m2t", ".mp4", ".m4s",
                   ".aac", ".key", ".vtt", ".bin")


def _hls_child_ext(url, fallback=""):
    """Extension hint to keep on a rewritten child URL.

    `fallback` is the container the surrounding playlist implies, used when
    the URL itself carries no extension (the juicy.codes CDN serves
    extension-less variant/segment paths): `.m3u8` inside a master playlist,
    `.m4s` inside an fMP4 media playlist (#EXT-X-MAP / #EXT-X-BYTERANGE),
    `.ts` for classic MPEG-TS media playlists.
    """
    try:
        path = urlparse(url).path.lower()
    except Exception:
        return fallback
    for ext in _HLS_CHILD_EXTS:
        if path.endswith(ext):
            return ext
    if "/seg" in path or re.search(r"/\d+$", path):
        return ".ts"
    return fallback


def _rewrite_hls_tag_uri(line, base_url, proxy_prefix, fallback=""):
    """Rewrite URI="..." attributes on HLS tag lines (EXT-X-KEY encryption
    keys, EXT-X-MAP fMP4 init segments, EXT-X-MEDIA rendition playlists,
    EXT-X-I-FRAME-STREAM-INF) to same-origin /hls paths."""
    if "URI=" not in line:
        return line

    def repl(m):
        quote_ch, uri = m.group(1), m.group(2)
        try:
            absu = urljoin(base_url, uri)
        except Exception:
            absu = uri
        ext = _hls_child_ext(absu, fallback)
        return f"URI={quote_ch}{proxy_prefix}{_b64url_of(absu)}{ext}{quote_ch}"

    return re.sub(r'URI\s*=\s*(["\'])([^"\']+)\1', repl, line, flags=re.I)


def _rewrite_hls_manifest(text, base_url, proxy_prefix):
    """Rewrite an HLS playlist so every child URL is served by the addon.

    * variant playlists, media segments and key lines are replaced with
      `/hls/{token}/u/<base64url(absolute-uri)>` and keep the real container
      extension (`/hls/{token}/u/<b64>.ts`, `.m3u8`, `.bin`, ...) so players
      can pick the right demuxer;
    * relative URIs are resolved against the playlist's own URL first;
    * non-URI lines are preserved byte-for-byte (tags, EXTINF, ...);
    * extension-less children (juicy.codes CDN) get the container the
      playlist implies: `.m3u8` from a master, `.m4s` from an fMP4 media
      playlist (#EXT-X-MAP / #EXT-X-BYTERANGE), `.ts` otherwise.
    """
    if "#EXT-X-STREAM-INF" in text:                       # master playlist
        fallback = ".m3u8"
    elif ("#EXT-X-MAP" in text or "#EXT-X-BYTERANGE" in text):   # fMP4 media
        fallback = ".m4s"
    else:                                                 # MPEG-TS media
        fallback = ".ts"
    lines = text.splitlines()
    out = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            out.append(line)
            continue
        if stripped.startswith("#"):
            out.append(_rewrite_hls_tag_uri(line, base_url, proxy_prefix, fallback))
            continue
        try:
            absu = urljoin(base_url, stripped)
        except Exception:
            absu = stripped
        out.append(f"{proxy_prefix}{_b64url_of(absu)}{_hls_child_ext(absu, fallback)}")
    joined = "\n".join(out)
    if text.endswith("\n"):
        joined += "\n"
    return joined


def _placeholder_sources(zipper):
    """Servers to advertise when the zipper is not resolved yet.

    Uses the remembered shape (exact labels from a previous resolve) and
    falls back to MAX_SERVERS generic slots. Nothing here touches
    the network - the /watch token resolves the real URL at playback.
    Only v1, v2, v3 are listed (capped at MAX_SERVERS).
    """
    shape = _shape_of(zipper)
    if shape:
        # Cap at MAX_SERVERS (v1, v2, v3) and skip HLS entries
        filtered = [s for s in shape
                    if not _is_hls_url(s.get("ext")) and not _is_hls_url(s.get("filename"))]
        return [{"url": "", "server": s.get("server") or "",
                 "filename": s.get("filename") or "", "ext": s.get("ext") or "",
                 "lazy": True}
                for s in filtered[:MAX_SERVERS]]
    return [{"url": "", "server": "", "filename": "", "ext": "", "lazy": True}
            for _ in range(max(1, min(MAX_SERVERS, DEFAULT_SERVER_SLOTS)))]


# --------------------------------------------------------------------------
# episode-content election
#
# A hub page sometimes carries two numbering schemes for the same
# (season, episode): an old block whose "Episode 4" video is really a
# different season's file, plus a re-upload with the correct one. The
# resolver can check a candidate against ground truth - the PUBLIC file
# name behind a pixeldrain mirror ("Show S02E04 [RareToonsIndia].mkv") -
# and elect the variant whose file really is the requested episode. The
# check is only ever used to CHOOSE between conflicting variants; a lone
# unverifiable variant is served exactly as before, so absolute-episode
# file numbering (e.g. "S01E220" for season 2 episode 20) never loses
# its stream.
# --------------------------------------------------------------------------
_PD_URL_RE = re.compile(r"pixeldra(?:in\.com|\.in)/(?:api/file/|u/)?([A-Za-z0-9]{6,14})")
_FLASHZIPPER_RE = re.compile(r"flashzipper\.workers\.dev/([A-Za-z0-9_\-]+)")
_PD_NAME_RE = re.compile(r"\bS(\d{1,2})\s*E(\d{1,3})\b", re.I)

_pd_name_cache = {}             # file id -> (expiry, name)
_pd_name_lock = threading.Lock()


def _pd_file_id(url):
    """pixeldrain file id from a source URL (direct or flashzipper-wrapped)."""
    if not url:
        return None
    m = _PD_URL_RE.search(url)
    if m:
        return m.group(1)
    m = _FLASHZIPPER_RE.search(url)
    if m:
        try:
            path = m.group(1)
            pad = "=" * (-len(path) % 4)
            payload = json.loads(base64.urlsafe_b64decode(path + pad))
            m2 = _PD_URL_RE.search(payload.get("url") or "")
            if m2:
                return m2.group(1)
        except Exception:
            pass
    return None


def _pd_file_name(fid):
    """Public pixeldrain file metadata (the upload's real name), cached.

    Positive results are remembered for a month (names do not change),
    failures for a few minutes. Never raises; None means "no answer".
    """
    with _pd_name_lock:
        c = _pd_name_cache.get(fid)
        if c and c[0] > time.time():
            return c[1]
    name = None
    try:
        r = requests.get(f"https://pixeldrain.com/api/file/{fid}/info",
                         headers={"User-Agent": UA}, timeout=3.5)
        if r.status_code == 200:
            name = (r.json() or {}).get("name") or None
    except Exception:
        name = None
    with _pd_name_lock:
        _pd_name_cache[fid] = (time.time() + (30 * 24 * 3600 if name else 300), name)
        if len(_pd_name_cache) > 20000:
            now = time.time()
            for k, (exp, _) in list(_pd_name_cache.items()):
                if exp < now:
                    _pd_name_cache.pop(k, None)
    return name


def _sb_episode_verdict(sources, season, episode):
    """Do the resolved sources belong to the requested (season, episode)?

    True  - a mirror file name says exactly this season+episode
    None  - no verifiable signal (no pixeldrain mirror / no SxxEyy in the
            name / metadata unreachable)
    Never returns False: a file named for a DIFFERENT episode only matters
    when some other variant matches exactly, and the caller prefers an
    exact match - never the other way around.
    """
    if not sources or episode is None:
        return None
    for s in sources:
        fid = _pd_file_id(s.get("url") or "")
        if not fid:
            continue
        name = _pd_file_name(fid)
        if not name:
            continue
        m = _PD_NAME_RE.search(name)
        if not m:
            continue
        fs, fe = int(m.group(1)), int(m.group(2))
        if fe == episode and (season is None or season == 0 or fs == season):
            return True
    return None


def _elect_episode_variant(row, season, episode, public_base, budget, deadline):
    """Pick the variant whose StreamBeta content really is this episode.

    Returns (elected_sb, elected_mq, elected_title). When the row has a
    single sb variant - the normal case - nothing is resolved or verified
    and the merged row is returned untouched (zero added latency).
    """
    variants = row.get("variants") or []
    sb_cands = [v for v in variants if v.get("sb")]
    if len(sb_cands) < 2:
        return row.get("sb"), row.get("mq"), None

    # Conflicting variants: resolving them all can take longer than the
    # normal listing budget, but serving another season's video under this
    # label is worse than a slightly slower first answer. Small extra grace.
    eff_deadline = (deadline + 1.8) if deadline is not None else None

    def _resolve(z):
        if public_base:
            return _playable(z, budget=budget, deadline=eff_deadline)
        return _playable(z)

    verified = []
    for v in sb_cands:
        if eff_deadline is not None and time.time() >= eff_deadline:
            break
        play = _resolve(v["sb"])
        if _sb_episode_verdict(play.get("sources") or [], season, episode):
            verified.append(v)
            break          # first exact match wins; nothing later outranks it
    if verified:
        # An mq from a LATER variant is still fine if that variant's own
        # sb verifies as this episode (same player block re-uploaded).
        if not verified[0].get("mq"):
            for v in sb_cands[sb_cands.index(verified[0]) + 1:]:
                if not v.get("mq"):
                    continue
                if eff_deadline is not None and time.time() >= eff_deadline:
                    break
                play = _resolve(v["sb"])
                if _sb_episode_verdict(play.get("sources") or [], season, episode):
                    verified.append(v)
                    break
        elected = verified[0]
        mq = next((v.get("mq") for v in verified if v.get("mq")), None)
        return elected["sb"], mq, (elected.get("ep_title") or "").strip() or None
    # No ground truth either way (or budget spent). Keep today's sb, but do
    # NOT borrow the merged mq: on a conflicting page it may belong to the
    # other numbering scheme's row and would play a different episode's
    # video under the right label. Warm the candidates so the next listing
    # can elect properly.
    for v in sb_cands:
        prefetch(v["sb"])
    return row.get("sb"), None, None


def resolve_one(row, public_base=None, deadline=None, budget=None):
    """Build the stream entries for one (episode, lang) row.

    Every StreamBeta streaming server (v1..vN) is listed - download mirrors
    are still never exposed. The listing itself is latency-bounded: with a
    known public host each entry is a /watch/{token} URL that resolves,
    verifies and FAILS OVER at playback time, so listing does not have to
    wait for (or probe) the upstream at all. With an unknown host
    (localhost dev) we must hand the player a real URL, so we resolve
    inline. Anything that cannot produce a file degrades to the site's
    browser player.
    """
    out = []
    seen = set()
    lang = (row.get("lang") or "").strip()
    lang = re.sub(r"\s*(uncut|censored)\s*$", "", lang, flags=re.I)
    if not lang:
        lang = "Hindi"
    ep = row.get("episode") or 0
    st = row.get("season") or 0
    prefix = f"S{st:02d}E{ep:02d}" if st else f"E{ep:02d}"
    skey = row.get("show_key") or show_key(row.get("show") or "") or "rt"
    binge_base = f"raretoons|{skey}|{lang.lower()}"

    if budget is None and deadline is None and public_base:
        budget = LIST_BUDGET

    # Elect the variant whose StreamBeta content really is this episode.
    # Hub pages that carry two numbering schemes can otherwise serve
    # another season's video under this label (wrong-episode bug).
    elected_sb, elected_mq, elected_title = _elect_episode_variant(
        row, st, ep, public_base, budget, deadline)
    _ep_title = elected_title or row.get("ep_title")
    title = f"{prefix} • {_ep_title}" if _ep_title else prefix

    def _emit_sources(name_base, zipper, sources, lazy=False):
        emitted = 0
        first_emitted_source = None
        # Filter HLS sources BEFORE capping — we only want direct files.
        # Track original indices so watch tokens point at the right position
        # in the full source list (important for placeholder sources that
        # are identical dicts — list.index() would always return 0).
        direct = [(i, s) for i, s in enumerate(sources)
                  if not _is_hls_url(s.get("url") or "") and not s.get("hls")]
        # Cap at MAX_SERVERS (v1, v2, v3 only)
        capped = direct[:MAX_SERVERS]
        capped_total = len(capped)
        for emit_idx, (orig_idx, source) in enumerate(capped):
            raw = source.get("url") or ""
            ext = source.get("ext") or _source_ext(source)
            server = _server_suffix(source, emit_idx, capped_total)
            quality = _quality_of(source)
            url = _watch_url(public_base, zipper, orig_idx, ext) or raw
            if not url or url in seen:
                continue
            seen.add(url)
            emitted += 1
            if first_emitted_source is None:
                first_emitted_source = source
            label = " • ".join(x for x in (server, quality) if x)
            hints = {
                # Signed CDN files are not CORS-enabled: Stremio's own
                # player/server must fetch them, not the web page.
                "notWebReady": True,
                "bingeGroup": f"{binge_base}|{server or 'v1'}",
                # Helps Stremio (desktop + Android) pick a demuxer and match
                # external subtitles.
                "filename": source.get("filename") or f"{prefix}.{(ext or '.mkv').lstrip('.')}",
                "proxyHeaders": {"request": {"User-Agent": UA}},
            }
            if source.get("size"):
                hints["videoSize"] = source["size"]
            desc = f"{title} — direct file" + (
                f" • {source['filename']}" if source.get("filename") else "")
            if lazy:
                desc += " • resolved at playback"
            prov = server or "Server v1"
            if lazy:
                prov += " · resolves at playback"
            cname, cdesc = _fmt_card(
                _ql_label(quality), _ep_title or prefix, prefix, lang,
                prov,
                size=_human_size(source.get("size")),
                codec=(ext or "").lstrip(".").upper())
            out.append({
                "name": cname,
                "title": desc,
                "description": cdesc,
                "url": url,
                "behaviorHints": hints,
            })
        # One Web+Cast entry per zipper: plays in Stremio Web, Chromecast
        # and the stricter Android/TV players that refuse a cross-origin
        # redirect to a signed URL.
        if emitted and EXPOSE_PROXY_STREAMS and public_base and first_emitted_source:
            ext = first_emitted_source.get("ext") or _source_ext(first_emitted_source)
            purl = _proxy_url(public_base, zipper, 0, ext)
            if purl and purl not in seen:
                seen.add(purl)
                wq = _ql_label(_quality_of(first_emitted_source)) if first_emitted_source else "AUTO"
                wname, wdesc = _fmt_card(
                    wq, (_ep_title or prefix) + " · Web+Cast", prefix, lang,
                    "addon proxy")
                out.append({
                    "name": wname,
                    "title": f"{title} — proxied through the addon (CORS + Range)",
                    "description": wdesc,
                    "url": purl,
                    "behaviorHints": {
                        "notWebReady": False,
                        "bingeGroup": f"{binge_base}|webcast",
                        "filename": first_emitted_source.get("filename") or f"{prefix}{ext or '.mkv'}",
                    },
                })
        return emitted

    def _emit_external(name, target):
        if target and target not in seen:
            seen.add(target)
            out.append({
                "name": name,
                "title": f"{title} — opens player in browser",
                "description": f"{title} — opens player in browser",
                "url": target,
                "externalUrl": target,
                "external": {"name": "Open player",
                             "description": "RareToons player (browser required)",
                             "url": target},
                "behaviorHints": {"notWebReady": True},
            })

    def _emit_mq_hls(zipper, lang):
        """In-app adaptive HLS for the site's MultiQuality player.

        `/hls/{token}.m3u8` resolves lazily at click time; the full chain
        (master -> variants -> segments / AES keys) is proxied + rewritten
        through the addon, CORS-open, with the player-page Referer, so it
        plays in Stremio Web / Chromecast / Android / TV instead of opening a
        browser tab. When the zipper has no HLS master, /hls/ degrades to a
        /watch-style 302, never to a mislabeled body.
        """
        if not public_base or not zipper:
            return False
        url = _hls_proxy_url(public_base, zipper)
        if not url or url in seen:
            return False
        seen.add(url)
        hints = {
            # The /hls/ endpoint is served by the addon itself (same-origin,
            # CORS-open, correct Content-Type), so this stream is web-ready.
            # No proxyHeaders: per the SDK spec that flag requires
            # notWebReady: true, which we do not want to set here.
            "notWebReady": False,
            "bingeGroup": f"{binge_base}|mq",
            "filename": f"{prefix}.m3u8",         # tells the player it's HLS
        }
        desc = (f"{title} — MultiQuality HLS (adaptive 1080p/720p/480p), "
                "streamed through the addon (CORS + Referer safe)")
        mname, mdesc = _fmt_card(
            "MULTI", (_ep_title or prefix) + " · MultiQuality", prefix, lang,
            "MultiQuality")
        out.append({
            "name": mname,
            "title": desc,
            "description": mdesc,
            "url": url,
            "behaviorHints": hints,
        })
        return True

    def _handle(zipper, name_base, allow_external=True, allow_raw_external=True):
        if not zipper:
            return
        play = _playable(zipper, budget=budget, deadline=deadline) if public_base \
            else _playable(zipper)
        if play.get("sources"):
            emitted = _emit_sources(name_base, zipper, play["sources"])
            if emitted:
                return
            # All sources were HLS (filtered out) — fall through to fallback
        if not public_base:
            # No /watch endpoint reachable -> the player needs a real URL now.
            if play.get("embed") and allow_external:
                _emit_external(name_base, play["embed"])
            elif allow_raw_external:
                _emit_external(name_base, zipper)
            return
        # Not resolved within the budget: advertise the servers anyway, the
        # /watch token resolves + verifies + fails over at playback time.
        entry = _cached_entry(zipper)
        if entry and not entry.get("ok") and entry.get("exp", 0) > time.time():
            # upstream said "nothing playable" recently -> browser fallback
            if entry.get("embed") and allow_external:
                _emit_external(name_base, entry["embed"])
            elif allow_raw_external:
                _emit_external(name_base, zipper)
            return
        prefetch(zipper)
        _emit_sources(name_base, zipper, _placeholder_sources(zipper), lazy=True)

    # StreamBeta -> direct in-app playback (streaming servers v1..v3 only)
    _handle(elected_sb, f"RareToons • {lang}")

    # MultiQuality -> an in-app, adaptive HLS stream. The raw argon player
    # requires the player-page Referer for segments, so we never 302 the
    # player straight at the CDN master; instead the whole chain is served
    # through /hls/ (rewritten + proxied, CORS + Referer safe). It resolves
    # lazily at click time and degrades to the browser player when no HLS.
    # ZERO-BANDWIDTH MODE (default): the juicy.codes CDN IP-locks its signed
    # URLs to the resolver, so /hls can only proxy the chain through this
    # server. Skip it; Server v1/v2/v3 already play via /watch 302 (direct,
    # zero bytes through the addon). Only an episode with NO in-app stream
    # at all still gets the MQ browser-player card.
    if elected_mq and MQ_INAPP_HLS:
        if not _emit_mq_hls(elected_mq, lang):
            # No /hls endpoint reachable (localhost/dev) -> we must hand the
            # player a real URL now: use the browser-only MQ player.
            mq_zipper = elected_mq
            mq_play = _playable(mq_zipper) if not public_base \
                else _playable(mq_zipper, budget=budget, deadline=deadline)
            mq_embed = None
            if mq_play.get("embed"):
                mq_embed = mq_play["embed"]
            elif mq_play.get("sources"):
                # MQ resolved to HLS sources but no /hls endpoint -> player page
                mq_embed = mq_zipper
            elif not mq_play:
                mq_embed = mq_zipper
            if mq_embed:
                _emit_external(f"RareToons • MQ • {lang}", mq_embed)
    elif elected_mq and not out:
        # zero-bandwidth mode, MQ-only episode: last resort -> browser player
        _emit_external(f"RareToons • MQ • {lang}", elected_mq)

    return out


# --------------------------------------------------------------------------
# playback-time resolution (what /watch and /proxy hand to the player)
# --------------------------------------------------------------------------
_pick_cache = {}          # (zipper, index) -> (expiry, url)


def _pick_source(sources, index):
    if not sources:
        return None
    return sources[min(max(index, 0), len(sources) - 1)]


def playback_target(zipper, index=0, verify_hls=True):
    """Resolve a /watch token to a URL that plays RIGHT NOW.

    This is where verification lives now (instead of the listing path):
      1. resolve (cached / stale-while-revalidate / blocking on a miss),
      2. probe the requested server,
      3. FAIL OVER to any other streaming server of the same episode,
      4. if every server is dead, force one fresh re-resolve (expired
         signed tokens) and probe again,
      5. last resort: the site's browser player.

    Returns (url, is_media). The choice is cached briefly so seeking or a
    player reconnect does not re-probe upstream.

    verify_hls=False (the /hls master route) skips the probe for HLS
    playlist URLs: the route fetches the master itself, so probing it
    first is a wasted round trip on EVERY playback start - the visible
    "takes forever to start" on desktop. A dead master then degrades in
    the route (browser-player fallback), not here. Unverified picks are
    NOT remembered, so /watch never trusts them.
    """
    now = time.time()
    key = (zipper, index)
    with _resolver_lock:
        cached = _pick_cache.get(key)
        if cached and cached[0] > now:
            return cached[1], True

    play = _playable(zipper, budget=PLAYBACK_BUDGET)
    sources = play.get("sources") or []
    target, is_media = None, False

    def _choose(srcs):
        if not srcs:
            return None
        if not PLAYBACK_VERIFY:
            pick = _pick_source(srcs, index)
            return pick["url"] if pick else None
        order = list(range(len(srcs)))
        first = min(max(index, 0), len(srcs) - 1)
        order.remove(first)
        order.insert(0, first)
        for i in order:
            u = srcs[i].get("url")
            if not u:
                continue
            if not verify_hls and _is_hls_url(u):
                return u           # the /hls chain self-validates on fetch
            if _probe_media(u):
                return u
        return None

    target = _choose(sources)
    if target is None and sources:
        # every advertised server refused us -> tokens expired, re-resolve
        with _resolver_lock:
            _zipper_cache.pop(zipper, None)
        fresh = _do_resolve(zipper)
        sources = fresh.get("sources") or []
        target = _choose(sources)
        if target is None:
            pick = _pick_source(sources, index)
            target = pick["url"] if pick else None      # let the player try
    if target:
        is_media = True
    else:
        target = play.get("embed") or zipper

    # Remember the pick only when it was (or did not need to be) verified:
    # an unverified HLS pick must not be trusted by a later /watch.
    if not (is_media and _is_hls_url(target) and not verify_hls):
        with _resolver_lock:
            _pick_cache[key] = (now + (PICK_TTL if is_media else 30), target)
            if len(_pick_cache) > 5000:
                for k, (exp, _) in list(_pick_cache.items()):
                    if exp < now:
                        _pick_cache.pop(k, None)
    return target, is_media


def decode_watch_token(tok):
    """(zipper, index) from a /watch|/proxy token, ('' , 0) when invalid."""
    try:
        pad = "=" * (-len(tok) % 4)
        raw = base64.urlsafe_b64decode((tok + pad).encode()).decode("utf-8")
        zipper, _, idx_s = raw.partition("\x00")
        return zipper, (int(idx_s) if idx_s.isdigit() else 0)
    except Exception:
        return "", 0


CONTENT_TYPES = {
    ".mkv": "video/x-matroska", ".mp4": "video/mp4", ".webm": "video/webm",
    ".m3u8": "application/vnd.apple.mpegurl", ".mov": "video/quicktime",
    ".avi": "video/x-msvideo", ".ts": "video/mp2t", ".m4v": "video/x-m4v",
    ".m4s": "video/mp4", ".aac": "audio/aac", ".mp3": "audio/mpeg",
    ".fmp4": "video/mp4", ".bin": "application/octet-stream",
}


MAX_ROWS = _env_int("MAX_ROWS", 12)
# Cap at 3 servers (v1, v2, v3 only) — no v4, v5, etc.
MAX_SERVERS = _env_int("MAX_SERVERS", 3)
# ordering: languages users pick most first, then anything else
LANG_ORDER = ("hindi", "tamil", "telugu", "english", "japanese")


def _stream_rank(entry):
    name = (entry.get("name") or "").lower()
    lang_rank = next((i for i, l in enumerate(LANG_ORDER) if l in name), len(LANG_ORDER))
    kind = 0 if entry.get("url") and not entry.get("externalUrl") else 2
    if "web+cast" in name or "webcast" in name:
        kind = 1                      # after direct, before browser fallback
    return (kind, lang_rank, name)


def build_streams(rows, public_base, deadline):
    """Resolve/emit the stream list for a set of rows within one deadline.

    All rows are worked in parallel; each of them is itself latency-bounded,
    so the endpoint answers in ~LIST_BUDGET seconds worst case instead of
    (rows x servers x probes) round trips.
    """
    rows = rows[:MAX_ROWS]
    if not rows:
        return []
    # Warm every sb zipper of this episode at once so the resolves overlap
    # instead of queueing behind each other. mq zippers are warmed too so the
    # MultiQuality HLS stream is ready in the background.
    for r in rows:
        prefetch(r.get("sb"))
        prefetch(r.get("mq"))
    streams = []
    workers = min(max(len(rows), 1), 8)
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(resolve_one, r, public_base, deadline) for r in rows]
        for fut in futures:
            try:
                streams.extend(fut.result(timeout=max(0.2, deadline - time.time() + 2)))
            except Exception:
                continue
    streams.sort(key=_stream_rank)
    return streams


def _rank_candidates(q, season):
    cands = set(k for k, _ in SERIES.keys()) | set(MOVIES.keys()) | SHOWS
    scored = []
    for key in cands:
        s = match_score(q, key)
        if s >= 0.6:
            has_season = (season is not None and (key, season) in SERIES)
            scored.append((s, key, has_season))
    scored.sort(key=lambda t: (-t[0], len(t[1].split()), 0 if t[2] else 1))
    return scored

SERIES, MOVIES, FALLBACK, SHOWS, SHOW_DISPLAY, SHOW_EP_COUNT = load_index()
_load_tmdb_cache()
_load_hints()

# Warm only the *next* episode in the background (ep+1). Earlier the default
# lookahead was 2, which also resolved ep+2 - i.e. watching ep1 pulled ep2
# AND ep3. The next-click cache only needs ep2; ep+2 is wasted upstream load.
PREFETCH_LOOKAHEAD = _env_int("PREFETCH_LOOKAHEAD", 1)


def prefetch_next_episodes(key, season, episode):
    """Warm the resolver cache for the next episode(s) of the same show."""
    if not PREFETCH_ENABLED or episode is None or PREFETCH_LOOKAHEAD <= 0:
        return
    wanted = {episode + i for i in range(1, PREFETCH_LOOKAHEAD + 1)}
    buckets = []
    if season is not None:
        buckets.append(SERIES.get((key, season), []))
    else:
        buckets.extend(rows for (k, _), rows in SERIES.items() if k == key)
    for rows in buckets:
        for r in rows:
            if r.get("episode") in wanted:
                prefetch(r.get("sb"))


def gather_episodes(query_name, season, episode):
    q = show_key(query_name)
    if not q:
        return []
    rows = []
    matched_key = None
    for s, key, _ in _rank_candidates(q, season)[:10]:
        matched_key = key
        if season is not None:
            rows = SERIES.get((key, season), [])
        else:
            rows = []
            for (k2, s2) in SERIES.keys():
                if k2 == key:
                    rows.extend(SERIES[(k2, s2)])
        if episode is not None:
            # Strict episode match. A requested episode the bucket does not
            # have must NEVER be answered with the whole season: the listing
            # then showed other episodes' streams under this episode's label
            # (Black Clover E52 - which the site does not have - listed
            # E1..E12). Keep looking in the next candidate instead; when no
            # candidate has it, the caller serves an empty list.
            rows = [r for r in rows if r.get("episode") == episode]
        if rows:
            break
    if not rows and season is not None:
        top = _rank_candidates(q, season)
        if top:
            matched_key = top[0][1]
            rows = [r for r in FALLBACK.get(matched_key, [])
                    if episode is None or r.get("episode") == episode]
    # Deduplicate: keep one row per (episode, lang), merging sb/mq from all
    merged = {}
    for r in rows:
        lang = (r.get("lang") or "").strip().lower() or "hindi"
        ep_key = (r.get("episode"), lang)
        if ep_key in merged:
            t = merged[ep_key]
            # Keep sb from whichever row has it
            if not t.get("sb") and r.get("sb"):
                t["sb"] = r["sb"]
            # Keep mq from whichever row has it
            if not t.get("mq") and r.get("mq"):
                t["mq"] = r["mq"]
            if not t.get("ep_title") and r.get("ep_title"):
                t["ep_title"] = r["ep_title"]
            for v in (r.get("variants") or []):
                _variant_add(t, v)
        else:
            merged[ep_key] = dict(r)
            merged[ep_key]["show_key"] = matched_key
    out_rows = list(merged.values())
    if matched_key is not None and episode is not None:
        # Binge-watching warms the next episodes in the background, so the
        # next "watch next" click has a ready cache (0 upstream latency).
        prefetch_next_episodes(matched_key, season, episode)
    return out_rows

def gather_movies(query_name):
    q = show_key(query_name)
    if not q:
        return []
    for s, key, _ in _rank_candidates(q, None)[:10]:
        rows = MOVIES.get(key, [])
        if rows:
            # Deduplicate: keep one row per (episode, lang), merging sb/mq
            merged = {}
            for r in rows:
                lang = (r.get("lang") or "").strip().lower() or "hindi"
                ep_key = (r.get("episode"), lang)
                if ep_key in merged:
                    t = merged[ep_key]
                    if not t.get("sb") and r.get("sb"):
                        t["sb"] = r["sb"]
                    if not t.get("mq") and r.get("mq"):
                        t["mq"] = r["mq"]
                else:
                    merged[ep_key] = dict(r)
                    merged[ep_key]["show_key"] = key
            return list(merged.values())
    return []

def gather_all_episodes_for_key(key):
    all_rows = []
    for (k, s), rows in SERIES.items():
        if k == key:
            for r in rows:
                all_rows.append((s, r.get("episode") or 0, r))
    for r in FALLBACK.get(key, []):
        all_rows.append((r.get("season") or 0, r.get("episode") or 0, r))
    all_rows.sort(key=lambda x: (x[0], x[1]))
    return all_rows

# --------------------------------------------------------------------------
# TMDB enriched catalog/meta helpers - FAST PATH for offline
# --------------------------------------------------------------------------
def build_meta_preview_fast(key, display_name, type_hint):
    """Fast preview without network, only cache lookup"""
    ck = f"enrich:{type_hint}:{key}"
    tmdb_enriched = _tmdb_cache_get(ck)
    if tmdb_enriched and tmdb_enriched.get("imdb_id"):
        meta_id = tmdb_enriched["imdb_id"]
    else:
        safe_key = re.sub(r"[^a-z0-9]+", "_", key)[:80].strip("_")
        meta_id = f"raretoons:{safe_key}"

    meta = {
        "id": meta_id,
        "type": type_hint,
        "name": tmdb_enriched["name"] if tmdb_enriched and tmdb_enriched.get("name") else display_name,
    }
    if tmdb_enriched:
        if tmdb_enriched.get("poster"):
            meta["poster"] = tmdb_enriched["poster"]
        if tmdb_enriched.get("background"):
            meta["background"] = tmdb_enriched["background"]
        if tmdb_enriched.get("overview"):
            meta["description"] = tmdb_enriched["overview"]
        else:
            meta["description"] = f"{display_name} - Hindi Dubbed available on RareToons"
        if tmdb_enriched.get("year"):
            meta["releaseInfo"] = tmdb_enriched["year"]
        if tmdb_enriched.get("genres"):
            meta["genres"] = tmdb_enriched["genres"]
        if tmdb_enriched.get("rating"):
            meta["imdbRating"] = str(tmdb_enriched["rating"])
        meta["_tmdb_id"] = tmdb_enriched.get("tmdb_id")
    else:
        meta["description"] = f"{display_name} - Hindi Dubbed (RareToonsIndia)"
        meta["poster"] = "https://www.rareanimes.com/wp-content/uploads/2023/11/cropped-Rare-Animes-India.png"
    meta["_rare_key"] = key
    return meta

def build_meta_preview(key, display_name, type_hint, tmdb_enriched=None, api_key=None, use_network=True):
    """Build preview, optionally with network"""
    if tmdb_enriched is None:
        ck = f"enrich:{type_hint}:{key}"
        cached = _tmdb_cache_get(ck)
        if cached:
            tmdb_enriched = cached
        elif use_network:
            tmdb_enriched = enrich_show_from_tmdb(display_name, type_hint, api_key_override=api_key, use_network=True)
            if tmdb_enriched:
                _tmdb_cache_set(ck, tmdb_enriched, ttl=TMDB_CACHE_TTL)

    if tmdb_enriched and tmdb_enriched.get("imdb_id"):
        meta_id = tmdb_enriched["imdb_id"]
    else:
        safe_key = re.sub(r"[^a-z0-9]+", "_", key)[:80].strip("_")
        meta_id = f"raretoons:{safe_key}"

    meta = {
        "id": meta_id,
        "type": type_hint,
        "name": tmdb_enriched["name"] if tmdb_enriched and tmdb_enriched.get("name") else display_name,
    }
    if tmdb_enriched:
        if tmdb_enriched.get("poster"):
            meta["poster"] = tmdb_enriched["poster"]
        if tmdb_enriched.get("background"):
            meta["background"] = tmdb_enriched["background"]
        if tmdb_enriched.get("overview"):
            meta["description"] = tmdb_enriched["overview"]
        else:
            meta["description"] = f"{display_name} - Hindi Dubbed available on RareToons"
        if tmdb_enriched.get("year"):
            meta["releaseInfo"] = tmdb_enriched["year"]
        if tmdb_enriched.get("genres"):
            meta["genres"] = tmdb_enriched["genres"]
        if tmdb_enriched.get("rating"):
            meta["imdbRating"] = str(tmdb_enriched["rating"])
        if tmdb_enriched.get("tmdb_id"):
            meta["_tmdb_id"] = tmdb_enriched["tmdb_id"]
    else:
        meta["description"] = f"{display_name} - Hindi Dubbed (RareToonsIndia)"
        meta["poster"] = "https://www.rareanimes.com/wp-content/uploads/2023/11/cropped-Rare-Animes-India.png"

    meta["_rare_key"] = key
    return meta

def build_full_meta(key, display_name, type_hint, base_id, tmdb_enriched=None, api_key=None, use_network=True):
    preview = build_meta_preview(key, display_name, type_hint, tmdb_enriched, api_key, use_network=use_network)
    rare_key = preview.pop("_rare_key", key)
    preview.pop("_tmdb_id", None)

    full = dict(preview)
    full["id"] = base_id

    if type_hint == "series":
        episodes = gather_all_episodes_for_key(rare_key)
        videos = []
        seen_se = set()
        for season, ep_num, row in episodes:
            if ep_num == 0:
                continue
            se_key = (season, ep_num)
            if se_key in seen_se:
                continue
            seen_se.add(se_key)
            vid_id = f"{base_id}:{season}:{ep_num}"
            ep_title = row.get("ep_title") or f"Episode {ep_num}"
            videos.append({
                "id": vid_id,
                "title": f"S{season:02d}E{ep_num:02d} - {ep_title}" if season else f"E{ep_num:02d} - {ep_title}",
                "season": season if season else 1,
                "episode": ep_num,
                "overview": ep_title,
                "released": f"{full.get('releaseInfo', '2020')}-01-01T00:00:00.000Z"
            })
        videos.sort(key=lambda v: (v["season"], v["episode"]))
        full["videos"] = videos

    return full

def handle_catalog_request(catalog_type, catalog_id, search, skip, api_key):
    """Fast catalog: no network unless search. Search uses single TMDB query."""
    if catalog_type == "movie":
        all_keys = list(MOVIES.keys())
    else:
        all_keys = list(set(k for k, _ in SERIES.keys()) | set(FALLBACK.keys()))

    tmdb_results_map = {}  # tmdb result matched to rare key

    if search:
        # Try TMDB search first (single request) to get posters
        # Use multi search for better coverage
        tmdb_data = None
        try:
            if catalog_type == "movie":
                tmdb_data = tmdb_search_movie(search)
            else:
                tmdb_data = tmdb_search_tv(search)
                if not tmdb_data or not tmdb_data.get("results"):
                    tmdb_data = tmdb_search_multi(search)
        except Exception:
            tmdb_data = None

        if tmdb_data and tmdb_data.get("results"):
            # Build map from tmdb results to rare keys
            for res in tmdb_data["results"][:20]:
                title = res.get("name") or res.get("title")
                if not title:
                    continue
                q = show_key(title)
                # find best matching rare key
                best_key = None
                best_score = 0
                for k in all_keys:
                    sc = match_score(q, k)
                    # also check display name
                    disp = SHOW_DISPLAY.get(k, "")
                    sc2 = match_score(show_key(search), show_key(disp))
                    sc = max(sc, sc2)
                    if search.lower() in disp.lower():
                        sc = max(sc, 0.85)
                    if sc > best_score:
                        best_score = sc
                        best_key = k
                if best_key and best_score >= 0.5:
                    # Build enriched from this tmdb result directly, no extra network
                    poster_path = res.get("poster_path")
                    backdrop_path = res.get("backdrop_path")
                    enriched = {
                        "tmdb_id": res.get("id"),
                        "imdb_id": None,  # will try to get later if needed, but avoid network for catalog
                        "name": title,
                        "poster": f"{TMDB_IMAGE_BASE}{poster_path}" if poster_path else None,
                        "background": f"{TMDB_BG_BASE}{backdrop_path}" if backdrop_path else None,
                        "overview": res.get("overview"),
                        "year": (res.get("first_air_date") or res.get("release_date") or "")[:4],
                        "genres": [],
                        "rating": res.get("vote_average"),
                    }
                    # cache this enrich
                    ck = f"enrich:{catalog_type}:{best_key}"
                    # only cache if we have poster
                    if enriched.get("poster"):
                        _tmdb_cache_set(ck, enriched, ttl=TMDB_CACHE_TTL)
                    tmdb_results_map[best_key] = enriched

            # Filtered keys are those matched via TMDB
            if tmdb_results_map:
                filtered_keys = list(tmdb_results_map.keys())
                # sort by vote count or original order?
                filtered_keys.sort(key=lambda k: -SHOW_EP_COUNT.get(k, 0))
            else:
                # fallback to local scoring
                q = show_key(search)
                scored = []
                for key in all_keys:
                    display = SHOW_DISPLAY.get(key, key)
                    s1 = match_score(q, key) if q else 0
                    s2 = match_score(show_key(search), show_key(display)) if search else 0
                    s = max(s1, s2)
                    if search.lower() in display.lower():
                        s = max(s, 0.85)
                    if s >= 0.4:
                        scored.append((s, key))
                scored.sort(key=lambda x: (-x[0], -SHOW_EP_COUNT.get(x[1], 0)))
                filtered_keys = [k for _, k in scored]
        else:
            # TMDB search failed (offline), fallback to local
            q = show_key(search)
            scored = []
            for key in all_keys:
                display = SHOW_DISPLAY.get(key, key)
                s1 = match_score(q, key) if q else 0
                s2 = match_score(show_key(search), show_key(display)) if search else 0
                s = max(s1, s2)
                if search.lower() in display.lower():
                    s = max(s, 0.85)
                if s >= 0.4:
                    scored.append((s, key))
            scored.sort(key=lambda x: (-x[0], -SHOW_EP_COUNT.get(x[1], 0)))
            filtered_keys = [k for _, k in scored]
    else:
        filtered_keys = sorted(all_keys, key=lambda k: -SHOW_EP_COUNT.get(k, 0))

    try:
        skip_i = int(skip) if skip else 0
    except ValueError:
        skip_i = 0
    slice_keys = filtered_keys[skip_i:skip_i+100]

    metas = []
    for k in slice_keys:
        disp = SHOW_DISPLAY.get(k, k.title())
        th = "movie" if catalog_type == "movie" else "series"
        if k in tmdb_results_map:
            # use TMDB result we already have
            m = build_meta_preview(k, disp, th, tmdb_enriched=tmdb_results_map[k], use_network=False)
        else:
            # fast path, cache only
            m = build_meta_preview_fast(k, disp, th)
        m.pop("_rare_key", None)
        m.pop("_tmdb_id", None)
        metas.append(m)

    return {"metas": metas}

def handle_meta_request(meta_type, meta_id, api_key):
    key = None
    display_name = None
    tmdb_enriched = None
    type_hint = meta_type

    if meta_id.startswith("raretoons:"):
        safe = meta_id[len("raretoons:"):]
        candidates = []
        for k in SHOW_DISPLAY.keys():
            sk = re.sub(r"[^a-z0-9]+", "_", k)[:80].strip("_")
            if sk == safe:
                key = k
                break
            if safe in sk or sk in safe:
                candidates.append(k)
        if not key and candidates:
            key = candidates[0]
        if key:
            display_name = SHOW_DISPLAY.get(key, key.title())
            # try network enrichment for meta (single show, ok to network)
            tmdb_enriched = enrich_show_from_tmdb(display_name, type_hint, api_key_override=api_key, use_network=True)
        else:
            return None

    elif meta_id.startswith("tt"):
        if api_key:
            _tl.tmdb_key = api_key
        try:
            tmdb_info = get_tmdb_from_imdb(meta_id)
            if tmdb_info:
                t_type, t_id, t_data = tmdb_info
                if t_type == "movie":
                    details = tmdb_movie_details(t_id)
                else:
                    details = tmdb_tv_details(t_id)
                name = None
                if details:
                    name = details.get("title") or details.get("name") or t_data.get("title") or t_data.get("name")
                else:
                    name = t_data.get("title") or t_data.get("name")
                if name:
                    display_name = name
                    q = show_key(name)
                    best_key = None
                    best_score = 0
                    for k in SHOW_DISPLAY.keys():
                        sc = match_score(q, k)
                        if sc > best_score:
                            best_score = sc
                            best_key = k
                    if best_score >= 0.6:
                        key = best_key
                    else:
                        tmdb_enriched = enrich_show_from_tmdb(name, type_hint, api_key_override=api_key, use_network=True)
                        if not tmdb_enriched:
                            poster_path = (details.get("poster_path") if details else None) or t_data.get("poster_path")
                            backdrop_path = (details.get("backdrop_path") if details else None) or t_data.get("backdrop_path")
                            tmdb_enriched = {
                                "tmdb_id": t_id,
                                "imdb_id": meta_id,
                                "name": name,
                                "poster": f"{TMDB_IMAGE_BASE}{poster_path}" if poster_path else None,
                                "background": f"{TMDB_BG_BASE}{backdrop_path}" if backdrop_path else None,
                                "overview": (details.get("overview") if details else "") or t_data.get("overview"),
                                "year": ((details.get("release_date") or details.get("first_air_date") or "")[:4] if details else ""),
                                "genres": [g["name"] for g in details.get("genres", [])] if details and details.get("genres") else [],
                                "rating": details.get("vote_average") if details else None,
                            }
                        if not key:
                            full = {
                                "id": meta_id,
                                "type": type_hint,
                                "name": tmdb_enriched.get("name") or display_name,
                                "poster": tmdb_enriched.get("poster"),
                                "background": tmdb_enriched.get("background"),
                                "description": tmdb_enriched.get("overview") or f"{display_name} - via TMDB",
                                "releaseInfo": tmdb_enriched.get("year"),
                                "genres": tmdb_enriched.get("genres"),
                            }
                            if type_hint == "series":
                                full["videos"] = []
                            return full
                    if key:
                        display_name = SHOW_DISPLAY.get(key, display_name)
                        tmdb_enriched = enrich_show_from_tmdb(display_name, type_hint, api_key_override=api_key, use_network=True)
            else:
                # TMDB unreachable — try local fallback using cached enrich data
                for ck, (exp, val) in list(_tmdb_cache.items()):
                    if exp > time.time() and isinstance(val, dict) and val.get("imdb_id") == meta_id:
                        name = val.get("name")
                        if name:
                            display_name = name
                            q = show_key(name)
                            best_key = None
                            best_score = 0
                            for k in SHOW_DISPLAY.keys():
                                sc = match_score(q, k)
                                if sc > best_score:
                                    best_score = sc
                                    best_key = k
                            if best_score >= 0.6:
                                key = best_key
                                display_name = SHOW_DISPLAY.get(key, display_name)
                                tmdb_enriched = val
                        break
                if not key:
                    return None
        finally:
            if api_key:
                try:
                    delattr(_tl, 'tmdb_key')
                except AttributeError:
                    pass
    else:
        if meta_id.isdigit():
            try:
                tmdb_id = int(meta_id)
                if type_hint == "movie":
                    details = tmdb_movie_details(tmdb_id)
                    if details:
                        name = details.get("title")
                        display_name = name
                        q = show_key(name) if name else ""
                        best_key = None
                        best_score = 0
                        for k in SHOW_DISPLAY.keys():
                            sc = match_score(q, k)
                            if sc > best_score:
                                best_score = sc
                                best_key = k
                        if best_score >= 0.6:
                            key = best_key
                            tmdb_enriched = enrich_show_from_tmdb(display_name, type_hint, api_key_override=api_key, use_network=True)
                        else:
                            poster_path = details.get("poster_path")
                            backdrop_path = details.get("backdrop_path")
                            return {
                                "id": meta_id,
                                "type": type_hint,
                                "name": name,
                                "poster": f"{TMDB_IMAGE_BASE}{poster_path}" if poster_path else None,
                                "background": f"{TMDB_BG_BASE}{backdrop_path}" if backdrop_path else None,
                                "description": details.get("overview"),
                                "releaseInfo": (details.get("release_date") or "")[:4],
                                "genres": [g["name"] for g in details.get("genres", [])],
                            }
                else:
                    details = tmdb_tv_details(tmdb_id)
                    if details:
                        name = details.get("name")
                        display_name = name
                        q = show_key(name) if name else ""
                        best_key = None
                        best_score = 0
                        for k in SHOW_DISPLAY.keys():
                            sc = match_score(q, k)
                            if sc > best_score:
                                best_score = sc
                                best_key = k
                        if best_score >= 0.6:
                            key = best_key
                            tmdb_enriched = enrich_show_from_tmdb(display_name, type_hint, api_key_override=api_key, use_network=True)
                        else:
                            poster_path = details.get("poster_path")
                            backdrop_path = details.get("backdrop_path")
                            return {
                                "id": meta_id,
                                "type": type_hint,
                                "name": name,
                                "poster": f"{TMDB_IMAGE_BASE}{poster_path}" if poster_path else None,
                                "background": f"{TMDB_BG_BASE}{backdrop_path}" if backdrop_path else None,
                                "description": details.get("overview"),
                                "releaseInfo": (details.get("first_air_date") or "")[:4],
                                "genres": [g["name"] for g in details.get("genres", [])],
                                "videos": []
                            }
            except Exception:
                pass
        safe = meta_id
        for k in SHOW_DISPLAY.keys():
            sk = re.sub(r"[^a-z0-9]+", "_", k)[:80].strip("_")
            if sk == safe:
                key = k
                display_name = SHOW_DISPLAY.get(k)
                break

    if not key:
        return None
    if not display_name:
        display_name = SHOW_DISPLAY.get(key, key.title())

    return build_full_meta(key, display_name, type_hint, meta_id, tmdb_enriched, api_key, use_network=True)

def parse_stream_id(stream_id):
    season = None
    episode = None
    base_id = stream_id

    if stream_id.startswith("raretoons:"):
        rest = stream_id[len("raretoons:"):]
        parts = rest.split(":")
        if len(parts) >= 3:
            try:
                s = int(parts[-2])
                e = int(parts[-1])
                season = s
                episode = e
                base_id = f"raretoons:{':'.join(parts[:-2])}"
                return base_id, season, episode
            except ValueError:
                pass
        if len(parts) >= 2:
            try:
                s = int(parts[-2])
                e = int(parts[-1])
                season = s
                episode = e
                base_id = f"raretoons:{':'.join(parts[:-2])}"
                return base_id, season, episode
            except ValueError:
                try:
                    e = int(parts[-1])
                    if len(parts) == 2:
                        episode = e
                        base_id = f"raretoons:{parts[0]}"
                        return base_id, season, episode
                except ValueError:
                    pass
        return base_id, None, None
    else:
        parts = stream_id.split(":")
        if len(parts) >= 3:
            try:
                season = int(parts[-2])
                episode = int(parts[-1])
                base_id = ":".join(parts[:-2])
                return base_id, season, episode
            except ValueError:
                pass
        elif len(parts) == 2:
            try:
                e = int(parts[1])
                base_id = parts[0]
                episode = e
                return base_id, season, episode
            except ValueError:
                pass
        return base_id, None, None

def resolve_name_from_stream_base(base_id, api_key):
    if base_id.startswith("raretoons:"):
        safe = base_id[len("raretoons:"):]
        for k in SHOW_DISPLAY.keys():
            sk = re.sub(r"[^a-z0-9]+", "_", k)[:80].strip("_")
            if sk == safe:
                return SHOW_DISPLAY.get(k, k)
        for k, disp in SHOW_DISPLAY.items():
            sk = re.sub(r"[^a-z0-9]+", "_", k)[:80].strip("_")
            if safe in sk or sk in safe:
                return disp
        return None
    elif base_id.startswith("tt"):
        # Try TMDB first
        if api_key:
            _tl.tmdb_key = api_key
        try:
            info = get_tmdb_from_imdb(base_id)
            if info:
                _, _, t_data = info
                name = t_data.get("title") or t_data.get("name")
                if name:
                    return name
        except Exception:
            pass
        finally:
            if api_key:
                try:
                    delattr(_tl, 'tmdb_key')
                except AttributeError:
                    pass
        # Fallback: check if we have a cached enrich that maps this IMDB to a show
        for ck, (exp, val) in list(_tmdb_cache.items()):
            if exp > time.time() and isinstance(val, dict) and val.get("imdb_id") == base_id:
                name = val.get("name")
                if name:
                    return name
        return None
    elif base_id.isdigit():
        if api_key:
            _tl.tmdb_key = api_key
        try:
            d = tmdb_tv_details(int(base_id))
            if d:
                return d.get("name")
            d = tmdb_movie_details(int(base_id))
            if d:
                return d.get("title")
        finally:
            if api_key:
                try:
                    delattr(_tl, 'tmdb_key')
                except AttributeError:
                    pass
        return None
    return None

# --------------------------------------------------------------------------
# manifest with TMDB
# --------------------------------------------------------------------------
def get_manifest(api_key=None):
    key_note = f" (TMDB key: {api_key[:6]}...)" if api_key and api_key != DEFAULT_TMDB_KEY else ""
    return {
        "id": "community.raretoons.stremio",
        "version": "1.11.1",
        "name": "RareToons (Hindi / Tamil / Telugu)",
        "description": ("Direct file streams (Server v1, v2, v3) from RareToonsIndia "
                        "(rareanimes.com). Links resolve, verify and fail over at "
                        "playback time, then play straight from the CDN via a 302 - "
                        "zero video bytes through this addon (free-plan friendly). "
                        f"TMDB enabled{key_note}."),
        "logo": "https://www.rareanimes.com/wp-content/uploads/2023/11/cropped-Rare-Animes-India.png",
        "background": "https://www.rareanimes.com/wp-content/uploads/2024/08/bg2024.webp",
        "types": ["series", "movie"],
        "catalogs": [
            {
                "type": "series",
                "id": "raretoons_series",
                "name": "RareToons Hindi Dubbed",
                "extra": [
                    {"name": "search", "isRequired": False},
                    {"name": "skip", "isRequired": False}
                ]
            },
            {
                "type": "movie",
                "id": "raretoons_movies",
                "name": "RareToons Movies Hindi",
                "extra": [
                    {"name": "search", "isRequired": False},
                    {"name": "skip", "isRequired": False}
                ]
            }
        ],
        "resources": [
            {"name": "catalog", "types": ["series", "movie"], "idPrefixes": ["raretoons", "tt"]},
            {"name": "meta", "types": ["series", "movie"], "idPrefixes": ["raretoons", "tt"]},
            {"name": "stream", "types": ["series", "movie"], "idPrefixes": ["tt", "raretoons"]},
        ],
        "idPrefixes": ["tt", "raretoons"],
        "behaviorHints": {"configurable": False, "configurationRequired": False,
                          "adult": False, "p2p": False},
    }

# --------------------------------------------------------------------------
# http handler
# --------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    server_version = "RareToonsAddon/1.8"

    def log_message(self, fmt, *args):
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))

    def _send(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Cache-Control", "max-age=60")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not getattr(self, "_is_head", False):
            self.wfile.write(body)

    def do_HEAD(self):
        # Android/ExoPlayer, VLC and Stremio's server probe with HEAD before
        # they start playback; answering it correctly avoids "stream not
        # available" on players that treat a failed HEAD as fatal.
        self._is_head = True
        try:
            self.do_GET()
        finally:
            self._is_head = False

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()

    def _public_base(self):
        """Public base URL (scheme://host) of this addon instance, or None
        when we can't know it (localhost dev / unknown proxy scheme). Only
        used to build /watch redirect URLs for players; when unknown we
        serve the verified raw signed URL instead - never a guessed scheme."""
        host = (self.headers.get("Host") or "").strip()
        if not host:
            return None
        h = host.split(":")[0].lower()
        if (h in ("localhost", "0.0.0.0", "::1", "[::1]")
                or h.startswith(("127.", "10.", "167.222.", "192.168."))
                or re.match(r"^172\.(1[6-9]|2\d|3[01])\.", h)
                or h.endswith(".local")):
            return None
        proto = (self.headers.get("X-Forwarded-Proto") or "").split(",")[0].strip().lower()
        if proto not in ("http", "https"):
            # Self-hosted without a reverse proxy (VPS/NAS/Android host): a
            # public hostname is still perfectly usable for /watch links, so
            # infer the scheme instead of dropping to raw signed URLs.
            port = host.split(":")[1] if ":" in host else ""
            proto = "http" if port and port not in ("443", "8443") else "https"
        return f"{proto}://{host}"

    def _extract_api_key(self, raw_path):
        parsed = urlparse(raw_path)
        path = parsed.path
        qs = parse_qs(parsed.query)
        api_key = None

        for k in ("tmdbApiKey", "tmdb_api_key", "api_key", "tmdbKey"):
            if k in qs and qs[k]:
                api_key = qs[k][0].strip()
                break

        parts = path.strip("/").split("/")
        if parts and len(parts[0]) >= 20 and re.fullmatch(r"[A-Za-z0-9]{20,64}", parts[0]):
            if len(parts) > 1 and parts[1] in ("manifest.json", "catalog", "meta",
                                               "stream", "watch", "proxy", "hls", ""):
                if not api_key:
                    api_key = parts[0]
                path = "/" + "/".join(parts[1:])
                if path == "/":
                    path = "/"

        effective_key = api_key or _get_effective_tmdb_key()
        return effective_key, path, qs

    def _stream_proxy(self, target, ext=""):
        """Byte-range proxy: same file, but same-origin, CORS-enabled and
        with a sane Content-Type. Fixes playback in Stremio Web, Chromecast
        and players that refuse cross-origin redirects to signed URLs."""
        req_headers = {"User-Agent": UA, "Accept": "*/*"}
        rng = self.headers.get("Range")
        if rng:
            req_headers["Range"] = rng
        sess = _checkout_session()
        upstream = None
        try:
            upstream = sess.get(target, headers=req_headers, stream=True,
                                timeout=RESOLVE_TIMEOUT, allow_redirects=True)
            code = upstream.status_code
            if code not in (200, 206, 416):
                self._send({"error": f"upstream {code}"}, 502)
                return
            self.send_response(code)
            ctype = upstream.headers.get("Content-Type")
            if not ctype or "text" in ctype.lower() or "octet-stream" in ctype.lower():
                ctype = CONTENT_TYPES.get(ext.lower() or _url_ext(target),
                                          ctype or "video/mp4")
            self.send_header("Content-Type", ctype)
            for h in ("Content-Length", "Content-Range"):
                v = upstream.headers.get(h)
                if v:
                    self.send_header(h, v)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Expose-Headers",
                             "Content-Length, Content-Range, Accept-Ranges")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            self.end_headers()
            if getattr(self, "_is_head", False):
                return
            for chunk in upstream.iter_content(chunk_size=256 * 1024):
                if not chunk:
                    continue
                self.wfile.write(chunk)
        except (BrokenPipeError, ConnectionResetError):
            pass          # player seeked / closed the connection - normal
        except Exception as exc:
            try:
                self._send({"error": f"proxy failed: {exc}"}, 502)
            except Exception:
                pass
        finally:
            try:
                if upstream is not None:
                    upstream.close()
            except Exception:
                pass
            _checkin_session(sess)

    # Order matters with `u` in the path: the master route is matched first.
    # A child may carry a container extension (`.ts`, `.m3u8`, `.bin`, ...)
    # appended by _rewrite_hls_manifest so players keep the segment type.
    _HLS_MASTER_RE = re.compile(r"^/hls/([A-Za-z0-9_\-]+)(?:\.m3u8?)?$")
    _HLS_RESOURCE_RE = re.compile(r"^/hls/([A-Za-z0-9_\-]+)/u/([A-Za-z0-9_\-]+)(?:\.[A-Za-z0-9]{1,5})?$")

    def _stream_hls_manifest(self, url, token, warm_variants=False):
        """Serve one HLS resource through the addon.

        * An HLS playlist (`#EXTM3U`) is rewritten so every child URI
          (variant playlists, media segments, AES keys, EXT-X-MAP) is
          fetched from `/hls/{token}/u/<base64url>` - same-origin, CORS-open,
          Referer handled - which is what lets Stremio Web / Chromecast /
          strict players play the MultiQuality stream adaptively instead of
          opening a browser tab (`/proxy` alone would only proxy the master).
        * Segments and keys are streamed unchanged with their upstream type.
        * HLS CDNs sometimes require the player-page Referer: 401/403 are
          retried with the site's player referers before giving up.
        * Rewritten playlists are cached briefly (VOD text is static), and
          the master warms its variant playlists in the background - desktop
          players fetch every variant before starting, so this keeps the
          start path free of upstream round trips.

        Returns True when something was served, False when the upstream
        fetch failed entirely (the master route then degrades to the site's
        browser player instead of a dead 502).
        """
        is_head = getattr(self, "_is_head", False)
        client_range = (self.headers.get("Range") or "").strip()
        # Playlist cache hit: VOD playlists are static, and players refetch
        # them at every start/seek - the rewritten copy is byte-identical.
        cached = _hls_playlist_get(token, url)
        if cached is not None:
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.apple.mpegurl")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Expose-Headers", "*")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(cached)))
            self.send_header("Connection", "close")
            self.end_headers()
            if not is_head:
                self.wfile.write(cached)
            return True
        s = _checkout_session()
        upstream = None
        # This endpoint only serves HLS-chain resources (a MultiQuality
        # player), and such CDNs often require the player-page Referer for
        # segments/keys as well as playlists. Prefer the exact referer captured
        # during resolve/rewrite (`_hls_chain_referer` for children, the stored
        # per-URL referer for the master), then fall back to the site's
        # player referers.
        chain_ref = None
        with _hls_referers_lock:
            chain_ref = _hls_chain_referer.get(token) or _hls_referers.get(url)
        referers = []
        if chain_ref:
            referers.append(chain_ref)
        referers += [None, "https://argon.razorshell.space/",
                     "https://www.rareanimes.com/", "https://codedew.com/"]
        used_ref = None
        # 0) Chrome-impersonated fetch first: the juicy.codes CDN validates
        #    the client TLS fingerprint and answers plain `requests` clients
        #    with a synthetic 403 "Invalid signature" - even for perfectly
        #    valid signed URLs. The player's Range header (EXT-X-BYTERANGE
        #    segments are byte ranges into one big fMP4 file) is forwarded.
        imp = _impersonated_get(url, referer=chain_ref,
                                range_header=client_range or None,
                                stream=True)
        if imp is not None:
            if imp.status_code in (200, 206):
                upstream = imp
                used_ref = chain_ref
            else:
                try:
                    imp.close()
                except Exception:
                    pass
        if upstream is None:
            for ref in referers:
                try:
                    headers = {"User-Agent": UA, "Accept": "*/*"}
                    if ref:
                        headers["Referer"] = ref
                    if client_range:
                        headers["Range"] = client_range
                    upstream = s.get(url, headers=headers, stream=True,
                                     timeout=RESOLVE_TIMEOUT, allow_redirects=True)
                    if upstream.status_code in (200, 206):
                        used_ref = ref
                        break
                    upstream.close()
                    upstream = None
                except Exception:
                    upstream = None
        if upstream is None:
            self._send({"error": "hls upstream unavailable"}, 502)
            return False
        code = upstream.status_code
        # Remember the referer that actually worked for this chain so every
        # child of the same token (variants/segments/keys) replays it.
        if used_ref:
            with _hls_referers_lock:
                _hls_chain_referer[token] = used_ref
        try:
            it = upstream.iter_content(chunk_size=256 * 1024)
            first = b""
            for chunk in it:
                first = chunk
                break
            is_playlist = first.lstrip().startswith(b"#EXTM3U")
            proxy_prefix = f"/hls/{token}/u/"
            if is_playlist:
                body = first
                for chunk in it:
                    body += chunk
                text = body.decode("utf-8", "replace")
                payload = _rewrite_hls_manifest(text, url, proxy_prefix).encode("utf-8")
                _hls_playlist_put(token, url, payload)
                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.apple.mpegurl")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Expose-Headers", "*")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("Connection", "close")
                self.end_headers()
                if not is_head:
                    self.wfile.write(payload)
                if warm_variants:
                    # Desktop players fetch every variant playlist before
                    # starting: warm them now so the start path stays local.
                    threading.Thread(
                        target=_warm_hls_variants,
                        args=(token, url, payload.decode("utf-8", "replace")),
                        daemon=True, name="hls-variant-warm").start()
                return True
            # Segment / key / init: pass the bytes through. Prefer the real
            # container type inferred from the child URL (`.ts` -> video/mp2t,
            # `.m4s`/`.mp4` -> video/mp4) when the CDN sends a generic type —
            # some players pick the demuxer from the Content-Type.
            ctype = upstream.headers.get("Content-Type") or ""
            if not ctype or "text" in ctype.lower() or "octet-stream" in ctype.lower():
                ctype = CONTENT_TYPES.get(_url_ext(url), "application/octet-stream")
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Expose-Headers", "*")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            # Byte-range passthrough (EXT-X-BYTERANGE segments: the player
            # asks for a slice of one big file and needs the 206 + the
            # Content-Range header to place it).
            crange = upstream.headers.get("Content-Range")
            if crange:
                self.send_header("Content-Range", crange)
            aranges = upstream.headers.get("Accept-Ranges")
            if aranges:
                self.send_header("Accept-Ranges", aranges)
            clen = upstream.headers.get("Content-Length")
            if clen:
                self.send_header("Content-Length", clen)
                self.end_headers()
                if not is_head:
                    if first:
                        self.wfile.write(first)
                    for chunk in it:
                        if chunk:
                            self.wfile.write(chunk)
            return True
        except (BrokenPipeError, ConnectionResetError):
            pass          # player seeked / closed the connection - normal
            return True
        except Exception as exc:
            try:
                self._send({"error": f"hls proxy failed: {exc}"}, 502)
            except Exception:
                pass
            return False
        finally:
            try:
                if upstream is not None:
                    upstream.close()
            except Exception:
                pass
            _checkin_session(s)

    def do_GET(self):
        effective_key, path, qs = self._extract_api_key(self.path)
        _tl.tmdb_key = effective_key
        try:
            path = path.rstrip("/") or "/"
            if path in ("", "/"):
                self._send({"addon": "RareToons", "version": "1.11.1",
                            "manifest": "/manifest.json",
                            "shows_indexed": len(SHOWS),
                            "stream_config": {
                                "servers": "v1, v2, v3 only",
                                "multiquality": "browser player (in-app HLS off: CDN is IP-locked, proxy would cost bandwidth)"
                                                if not MQ_INAPP_HLS else "hls (in-app, proxied)",
                                "webcast": "enabled" if EXPOSE_PROXY_STREAMS else "disabled",
                                "video_bytes_through_addon": "zero (cards 302 to the CDN)",
                            },
                            "endpoints": {
                                "watch": "/watch/{token}[.ext] -> 302 to a live server (failover)",
                                "proxy": "/proxy/{token}[.ext] -> same file, CORS + byte ranges (Web+Cast)",
                                "hls": "/hls/{token}.m3u8 -> MultiQuality master + rewritten chain (Web/Cast)",
                            },
                            "resolver": {
                                "list_budget_s": LIST_BUDGET,
                                "verify_on_list": VERIFY_ON_LIST,
                                "playback_verify": PLAYBACK_VERIFY,
                                "cached_zippers": len(_zipper_cache),
                                "remembered_server_shapes": len(_server_hints),
                            },
                            "tmdb_enabled": bool(effective_key),
                            "tmdb_key_preview": f"{effective_key[:6]}...{effective_key[-4:]}" if effective_key else None,
                            "catalogs": ["/catalog/series/raretoons_series.json", "/catalog/movie/raretoons_movies.json"]})
                return

            if path == "/manifest.json":
                self._send(get_manifest(api_key=effective_key))
                return

            if path.startswith("/catalog/"):
                m = re.match(r"^/catalog/([^/]+)/([^/]+?)(?:/([^/]+))?\.json$", path)
                if not m:
                    m = re.match(r"^/catalog/([^/]+)/([^/]+)/([^/]+)$", path)
                if not m:
                    m2 = re.match(r"^/catalog/([^/]+)/([^/]+)\.json$", path)
                    if m2:
                        ctype, cid = m2.group(1), m2.group(2)
                        search = qs.get("search", [""])[0] if qs else ""
                        skip = qs.get("skip", [""])[0] if qs else ""
                        result = handle_catalog_request(ctype, cid, search, skip, effective_key)
                        self._send(result)
                        self.log_message("catalog %s %s search=%r skip=%s -> %d metas", ctype, cid, search, skip, len(result.get("metas", [])))
                        return
                    self._send({"error": "invalid catalog path"}, 404)
                    return

                ctype = m.group(1)
                cid = m.group(2)
                extra_raw = m.group(3) or ""
                extra_qs = parse_qs(extra_raw) if extra_raw else {}
                search = ""
                skip = ""
                if extra_qs.get("search"):
                    search = extra_qs["search"][0]
                if qs.get("search"):
                    search = qs["search"][0] or search
                if extra_qs.get("skip"):
                    skip = extra_qs["skip"][0]
                if qs.get("skip"):
                    skip = qs["skip"][0] or skip
                if not search and extra_raw and "=" not in extra_raw:
                    search = extra_raw

                result = handle_catalog_request(ctype, cid, search, skip, effective_key)
                self._send(result)
                self.log_message("catalog %s %s search=%r skip=%s -> %d metas", ctype, cid, search, skip, len(result.get("metas", [])))
                return

            if path.startswith("/meta/"):
                m = re.match(r"^/meta/([^/]+)/([^/]+)\.json$", path)
                if not m:
                    self._send({"error": "invalid meta path"}, 404)
                    return
                mtype, mid = m.group(1), m.group(2)
                mid = unquote(mid)
                meta = handle_meta_request(mtype, mid, effective_key)
                if meta:
                    self._send({"meta": meta})
                    self.log_message("meta %s %s -> %s", mtype, mid, meta.get("name"))
                else:
                    self._send({"meta": {}}, 404)
                    self.log_message("meta %s %s -> NOT FOUND", mtype, mid)
                return

            m = re.match(r"^/(watch|proxy)/([A-Za-z0-9_\-]+)(\.[A-Za-z0-9]{1,5})?$", path)
            if m:
                mode, tok, ext = m.group(1), m.group(2), (m.group(3) or "")
                zipper, idx = decode_watch_token(tok)
                if not zipper.startswith("https://codedew.com/zip"):
                    self._send({"error": "invalid watch token"}, 400)
                    return
                t0 = time.time()
                target, is_media = playback_target(zipper, idx)
                if mode == "proxy" and is_media:
                    self._stream_proxy(target, ext)
                    self.log_message("proxy %s#%d -> %s (%.1fs)", tok[:12], idx,
                                     target[:110], time.time() - t0)
                    return
                # 302: the player follows it and talks to the CDN directly
                # (zero bandwidth through the addon, best seek performance).
                self.send_response(302)
                self.send_header("Location", target)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Expose-Headers", "*")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", "0")
                self.end_headers()
                self.log_message("watch %s#%d -> %s (%.1fs)", tok[:12], idx,
                                 target[:110], time.time() - t0)
                return

            # MultiQuality HLS master: same token scheme, but the playlist
            # (and every child URI it references) is proxied + rewritten.
            m = self._HLS_MASTER_RE.match(path)
            if m:
                tok = m.group(1)
                zipper, idx = decode_watch_token(tok)
                if not zipper.startswith("https://codedew.com/zip"):
                    self._send({"error": "invalid hls token"}, 400)
                    return
                t0 = time.time()
                # verify_hls=False: the master is fetched (and validated)
                # right below - probing it first costs a full upstream round
                # trip on EVERY playback start, which is the visible
                # "takes forever to start" on desktop players.
                target, is_media = playback_target(zipper, idx, verify_hls=False)
                if not is_media or not _is_hls_url(target):
                    # No HLS master right now (or the zipper resolved to a
                    # direct file) -> hand the player the URL as-is, like
                    # /watch: the site's browser player or the direct file.
                    self.send_response(302)
                    self.send_header("Location", target)
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.send_header("Access-Control-Expose-Headers", "*")
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    self.log_message("hls %s#%d -> %s (%.1fs)", tok[:12], idx,
                                     target[:110], time.time() - t0)
                    return
                ok = self._stream_hls_manifest(target, tok, warm_variants=True)
                if ok is False:
                    # Dead master (expired signatures upstream): degrade to
                    # the site's browser player instead of a dead 502, the
                    # same last resort /watch uses.
                    play = _playable(zipper, budget=0) or {}
                    fallback = play.get("embed") or zipper
                    self.send_response(302)
                    self.send_header("Location", fallback)
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.send_header("Access-Control-Expose-Headers", "*")
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    self.log_message("hls-degraded %s#%d -> %s (%.1fs)",
                                     tok[:12], idx, fallback[:110],
                                     time.time() - t0)
                    return
                self.log_message("hls %s#%d -> %s (%.1fs)", tok[:12], idx,
                                 target[:110], time.time() - t0)
                return

            # A rewritten child of an HLS chain (variant/segment/key/init).
            m = self._HLS_RESOURCE_RE.match(path)
            if m:
                tok, b64 = m.group(1), m.group(2)
                url = _unb64url(b64)
                if not url.lower().startswith(("http://", "https://")):
                    self._send({"error": "invalid hls resource"}, 400)
                    return
                self._stream_hls_manifest(url, tok)
                return

            m = re.match(r"^/stream/(series|movie)/([^/]+)\.json$", path)
            if m:
                stype = m.group(1)
                raw_id = unquote(m.group(2))
                base_id, season_from_id, episode_from_id = parse_stream_id(raw_id)

                name = qs.get("name", [""])[0] if qs else ""
                try:
                    season_q = int(qs.get("season", [""])[0]) if qs and qs.get("season", [""])[0] else None
                except ValueError:
                    season_q = None
                try:
                    episode_q = int(qs.get("episode", [""])[0]) if qs and qs.get("episode", [""])[0] else None
                except ValueError:
                    episode_q = None

                season = season_q if season_q is not None else season_from_id
                episode = episode_q if episode_q is not None else episode_from_id

                if not name:
                    resolved = resolve_name_from_stream_base(base_id, effective_key)
                    if resolved:
                        name = resolved
                    else:
                        if base_id.startswith("raretoons:"):
                            safe = base_id[len("raretoons:"):]
                            for k, disp in SHOW_DISPLAY.items():
                                sk = re.sub(r"[^a-z0-9]+", "_", k)[:80].strip("_")
                                if sk == safe:
                                    name = disp
                                    break

                t0 = time.time()
                public_base = self._public_base()
                # One shared deadline for the WHOLE request: no matter how
                # many rows/languages an episode has, the listing is served
                # within ~LIST_BUDGET seconds. Unresolved servers are still
                # listed and resolve at playback.
                deadline = t0 + LIST_BUDGET
                streams = []
                if name:
                    if stype == "series":
                        rows = gather_episodes(name, season, episode)
                    else:
                        rows = gather_movies(name)
                    if rows:
                        streams = build_streams(rows, public_base, deadline)

                # If no name resolved from IMDB, try a broader local search
                # by checking if any cached TMDB enrichment maps to this IMDB id
                if not streams and not name and base_id.startswith("tt"):
                    for ck, (exp, val) in list(_tmdb_cache.items()):
                        if exp > time.time() and isinstance(val, dict) and val.get("imdb_id") == base_id:
                            cached_name = val.get("name")
                            if cached_name:
                                name = cached_name
                                if stype == "series":
                                    rows = gather_episodes(name, season, episode)
                                else:
                                    rows = gather_movies(name)
                                if rows:
                                    streams = build_streams(rows, public_base, deadline)
                                break

                self._send({"streams": streams})
                self.log_message("stream %s %r (base=%s) s/e=%s/%s name=%r -> %d streams (%.1fs)",
                                 stype, raw_id, base_id, season, episode, name, len(streams), time.time() - t0)
                return

            self._send({"error": "not found"}, 404)
        finally:
            try:
                delattr(_tl, 'tmdb_key')
            except AttributeError:
                pass
            if _tmdb_cache_dirty and time.time() % 10 < 1:
                threading.Thread(target=_save_tmdb_cache, daemon=True).start()
            if _hints_dirty and time.time() % 30 < 1:
                threading.Thread(target=_save_hints, daemon=True).start()

# ------------------------------------------------- index auto-refresh -----
INDEX_AUTO_REFRESH = _env_flag("INDEX_AUTO_REFRESH", True)


def _reload_index():
    """Recompute the in-memory index from the (refreshed) files."""
    global SERIES, MOVIES, FALLBACK, SHOWS, SHOW_DISPLAY, SHOW_EP_COUNT
    (SERIES, MOVIES, FALLBACK, SHOWS,
     SHOW_DISPLAY, SHOW_EP_COUNT) = load_index()


def _index_auto_refresh(feed_pages=8):
    """Pull shows newly uploaded to the site since the last crawl.

    The addon serves from the pre-crawled index, so every show uploaded
    after the last refresh is invisible (Stremio shows it via TMDB but the
    episode list comes back empty). This runs ONCE at boot in a daemon
    thread - every deploy or spin-up re-syncs: it scans the site feed +
    home links, crawls hubs not yet in streams.json, merges them into
    episodes_index.jsonl + streams.json and swaps the in-memory index.
    Any failure just leaves the current index serving.
    """
    try:
        import refresh_index as _R
        n = _R.run(feed_pages=feed_pages, quiet=True)
        if n and (n.get("new_hubs") or n.get("new_rows")):
            _reload_index()
            print(f"[refresh] +{n['new_hubs']} hubs, +{n['new_rows']} episode "
                  f"rows -> index reloaded ({n['shows']} shows)", flush=True)
        else:
            print("[refresh] nothing new on the site", flush=True)
    except Exception as e:
        print(f"[refresh] failed (serving the committed index): "
              f"{type(e).__name__}: {e}", flush=True)


def main():
    print(f"[addon] index: {len(SHOWS)} shows, "
          f"{sum(len(v) for v in SERIES.values())} season-ep buckets, "
          f"{len(MOVIES)} movie keys", flush=True)
    print(f"[addon] TMDB key: {TMDB_API_KEY[:6]}...{TMDB_API_KEY[-4:]} (override via /{{key}}/manifest.json or ?tmdbApiKey=)", flush=True)
    print(f"[addon] tmdb cache: {len(_tmdb_cache)} entries", flush=True)
    print(f"[addon] server hints: {len(_server_hints)} zippers remembered "
          f"(instant listing)", flush=True)
    print(f"[addon] latency budget: {LIST_BUDGET:.1f}s per /stream, "
          f"verify-on-list={VERIFY_ON_LIST}, playback-verify={PLAYBACK_VERIFY}, "
          f"proxy-entries={EXPOSE_PROXY_STREAMS}", flush=True)
    httpd = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    httpd.daemon_threads = True
    if INDEX_AUTO_REFRESH:
        threading.Thread(target=_index_auto_refresh, name="index-refresh",
                         daemon=True).start()
    print(f"[addon] listening on 0.0.0.0:{PORT}", flush=True)
    try:
        httpd.serve_forever()
    finally:
        _save_tmdb_cache()
        _save_hints()

if __name__ == "__main__":
    main()
