## v1.9.0 — zero media bytes through the addon

- **Web+Cast `/proxy` cards removed by default** (`EXPOSE_PROXY_STREAMS` now
  defaults to False): the Server v1/v2/v3 cards already play via `/watch`
  302 -> CDN direct (verified: plain client gets 206 with no Referer).
- **In-app MultiQuality HLS off by default** (new `MQ_INAPP_HLS`, default
  False): the juicy.codes CDN IP-locks its signed segment/master URLs to the
  resolving IP, so the `/hls` chain could only ever be proxied through the
  addon = full video bandwidth on every MQ playback. Even a Chrome-impersonated
  fetch from another IP gets 403, so no direct-segment rewrite is possible.
- MQ-only episodes (no Server v1/v2/v3) now fall back to the site's browser
  player card instead of a proxied stream.
- Routes `/proxy` and `/hls` remain available for manual use; no card points
  at them anymore. Result: the addon serves JSON/302s only — free-plan safe.

# RareToons Addon v1.9.3 Changes

## Summary
Cut the **MultiQuality (HLS) start latency** on desktop players: the visible
symptom was "click MultiQuality -> a long loading spinner before playback
starts, almost every time" on Stremio Desktop (Windows), while Android was
fine. Measured cold start through the addon was ~6s of sequential upstream
round trips before the first segment; it is now ~1.2s warm (0.0s on replays
and seeks), and a dead master degrades to the site's browser player instead
of a dead 502.

## Why desktop was slow
Every playback start paid, one after another:
1. resolve (cached warm from the listing prefetch - fine),
2. a **probe round trip** on the HLS master that the very next step
   repeated anyway,
3. the **master fetch itself**,
4. **3 variant-playlist fetches** - desktop players (libmpv/ffmpeg) fetch
   EVERY variant playlist before starting, mobile players fetch one,
   which is why Android barely noticed.

## What changed (addon.py)
1. **`/hls` no longer probes the master** - `playback_target(...,
   verify_hls=False)`: the route fetches (and thereby validates) the master
   itself, so the pre-probe was a wasted round trip on every start. `/watch`
   still verifies exactly as before, and unverified picks are never cached,
   so `/watch` never trusts them.
2. **Dead master degrades instead of 502** - if the master fetch fails
   entirely (expired upstream signatures), the route 302s to the site's
   browser player, the same last resort `/watch` uses.
3. **Rewritten playlists are cached 5 minutes** (bounded at 64 entries):
   VOD playlists are static text, and players refetch master + variants at
   every start/seek/reconnect - those now serve from memory (0ms). Segments
   and keys are never cached.
4. **The master warms its variants in the background**: right after serving
   the master, a daemon thread pre-fetches + rewrites + caches each variant
   playlist (Chrome-impersonated fetch with the plain-session referer
   fallback), so the player's own variant requests are instant.
5. `_fetch_hls_child()` helper shared by the warmer: impersonated first
   (the juicy CDN fingerprints TLS), then plain requests with the player
   referers - the same ladder the request handler uses.

## Verified live (Windows-desktop simulation via ffmpeg/libmpv)
- master 0.45s + variants 0.00/0.00/0.77s = **1.22s start** (was ~6.1s cold)
- master replay 0.00s; 60s decode at 11-13x, seek to 12:00 and 24:00 clean
- `/watch` 302, `/proxy` 206 byte-range, movie listing - all unchanged
- Tests: `test_episode_election.py` 39/39 (new T13/T14), `test_403_fix.py`
  108/108.

# RareToons Addon v1.9.2 Changes

## Summary
Fixed the **missing-episode leak**: clicking an episode the site does not have
(e.g. Black Clover E52 - the site only carries S1 E1-E51) listed **other
episodes' streams** (E1..E12, capped by MAX_ROWS) under that episode. The
listing must simply be empty when the episode does not exist.

## Root cause
`gather_episodes()` kept the pre-filter rows when the requested episode had no
exact match in the bucket (`if exact: rows = exact` falls through to the whole
season on a miss). Stremio requests such episodes whenever the meta grid comes
from Cinemeta/TMDB (their grid has more episodes than the site).

## What changed (addon.py)
- Strict episode match: when `episode` is requested, the bucket is filtered to
  that episode **even when empty** - the candidate loop then looks in the next
  show candidate, and if nobody has it, `/stream` returns an empty list.
- Whole-season requests (`episode=None`) and specials (season 0) unchanged.

## Tests
- `test_episode_election.py` T12: Black Clover S1E52 -> 0 rows; S1E51 -> exactly
  episode 51; whole-season request unchanged; specials unchanged. 30/30.
- `test_403_fix.py`: 108/108 still green.

# RareToons Addon v1.9.1 Changes

## Summary
Fixed the **wrong-episode bug**: on some shows, clicking an episode listed (and
sometimes played) a *different season's* video under the right label - e.g.
SWAT Kats S1E1's MultiQuality entry actually streamed **S02E01**, and pages
with two numbering schemes could list another season's files as S1 episodes.

## Root cause
Hub pages occasionally carry **two numbering schemes** for the same
(season, episode): an old block whose "Episode 4" video is really a *season 2*
file, plus a re-upload ("NEw!") with the correct season/episode files.
`parse_hubs.py` trusts the page text, so both land in the same S1 bucket, and
the addon's row merge kept **the first row's sb and the first row that had an
mq** - which on conflicting pages mixes the two schemes (verified against
ground truth: the public pixeldrain file names behind the mirrors, e.g.
`SWAT Kats ... S02E01 [RareToonsIndia].mkv` served as S1E1's MultiQuality).

## What changed (addon.py)
1. **Variants kept per (episode, lang)** - `_merge_rows()` (was the closure
   `merge()` inside `load_index()`) now records every distinct
   `(sb, mq, ep_title)` source-row on the merged row (`variants`), and
   `gather_episodes()` carries them through. 570 rows across the index are
   affected; all other rows behave exactly as before.
2. **Episode-content election at listing time** - new
   `_elect_episode_variant()` (used by `resolve_one()`): when a row has 2+
   StreamBeta variants, each candidate's resolved mirrors are checked against
   the *public pixeldrain file name* (`_pd_file_id()` / `_pd_file_name()`,
   cached for a month). The **first variant whose file is really the requested
   season+episode** is elected; its `sb`, its `mq` and its title are served.
3. **MultiQuality is never borrowed across numbering schemes** - the MQ entry
   now comes from the elected (or another verified) variant of the SAME
   episode. On a conflicting row that cannot be verified, the merged mq is
   suppressed (a missing MQ entry is better than a different episode's video)
   and the candidates are prefetched so the next listing elects correctly.
4. **Conservative by construction** - a lone variant is never resolved or
   verified (zero added latency for normal shows, MQ untouched), and a file
   whose numbers merely *disagree* (e.g. absolute numbering "S01E220" for
   season 2 episode 20) is never rejected - only an exact match can win.
   Conflicting rows get +1.8s of extra resolve grace so a cold listing can
   still elect.

## Tests
- `test_episode_election.py` (new): 26 tests - election, MQ attribution,
  absolute-numbering safety, single-variant zero-overhead, variant merge,
  prefetch warmup, pixeldrain id parsing.
- `test_403_fix.py`: 108/108 still green.

## Verified live
- SWAT Kats S1E1 (cold + warm): wrong MultiQuality entry gone; entries come
  from the verified `S01E01 [RareToonsIndia].mkv` variant; `/watch` serves the
  same variant's mirrors.
- Attack on Titan S1E1 (normal show): 10 streams incl. MultiQuality -
  completely unchanged.

# RareToons Addon v1.8.0 Changes

## Summary
MultiQuality streams are listed **again, as real in-app HLS streams** that play
in Stremio (desktop / web / Chromecast / Android / TV) instead of falling back
to a browser tab. The HLS child URIs now keep their container extension, and
segment Content-Type is inferred from it, so players pick the right demuxer.

## Why MultiQuality did not play in Stremio

1. **v1.7 only emitted `MultiQuality` as a browser-tab external link** — Stremio
   never received an actual `.m3u8`/HLS URL, so on Android/TV/web there was
   nothing to play in-app.
2. The `/hls/{token}.m3u8` chain (master -> variants -> segments/AES keys) that
   the addon proxies and rewrites produced **extension-less child URIs**
   (`/hls/{token}/u/<base64url>`). Strict players (ExoPlayer / VLC / hls.js)
   rely on the URL suffix to pick the TS vs fMP4 vs playlist demuxer, and the
   generic `application/octet-stream` Content-Type did not help.

## What changed

### 1. MultiQuality -> in-app adaptive HLS stream
- `resolve_one()` now emits `RareToons • MultiQuality • {lang}` pointing at
  `/hls/{token}.m3u8` for any row that carries an `mq` zipper (when a public
  host is known). The URL resolves lazily at click time; `/hls/` proxies +
  rewrites the whole chain same-origin with CORS + the player-page Referer, so
  the adaptive multi-quality stream plays in-app.
- When there is no `/hls/` endpoint (localhost/dev), MultiQuality still
  degrades to the browser-only player (external) as a fallback.
- `build_streams()`/prefetch now warms `mq` zippers too.

### 2. HLS child URIs keep their container extension
- New `_hls_child_ext()` derives the real extension (`.ts`, `.m3u8`, `.bin`,
  `.m4s`, `.mp4`, ...) of a rewritten child and appends it:
  `/hls/{token}/u/<b64>.ts`, `.../u/<b64>.m3u8`, `.../u/<b64>.bin`.
- `_HLS_RESOURCE_RE` accepts the optional extension; the base64 token is
  decoded without it.

### 3. Segment Content-Type inferred from the URL
- `_stream_hls_manifest()` now maps the child's extension to a real content
  type (`.ts` -> `video/mp2t`, `.m4s`/`.mp4` -> `video/mp4`) when the CDN sends
  a generic `application/octet-stream` or `text/*`.
- Added `.m4s`, `.aac`, `.mp3`, `.fmp4`, `.bin` to `VIDEO_EXT` and
  `CONTENT_TYPES`, so fMP4 segments (`*.m4s`) are served as `video/mp4` instead
  of the generic `application/octet-stream`.

### 4. `/hls/{token}.m3u8` degrades like `/watch`
- If the MultiQuality zipper has no HLS master (or resolves to a direct file),
  `/hls/` 302s to the resolved URL (browser player / direct file) instead of
  serving a non-HLS body as a playlist.

### 5. Stremio SDK stream-spec compliance
- **Removed `proxyHeaders` from the MultiQuality stream entry.** Per the official
  Stremio SDK spec, `behaviorHints.proxyHeaders` only applies to `url` streams
  and **must** be paired with `behaviorHints.notWebReady: true`. Our `/hls/`
  endpoint injects the Referer internally and is web-ready, so `notWebReady`
  stays `false` and `proxyHeaders` is no longer sent.
- **The exact player-page Referer is threaded through the HLS chain.** The
  MultiQuality player page (e.g. `argon.razorshell.space/...`) is the exact
  thing strict CDNs check. `_resolve_mq_hls()` now records the page URL per HLS
  master (`_hls_referers`), `_stream_hls_manifest()` replays it (via
  `_hls_chain_referer`, set once a referer works) for every child in the chain,
  and `_probe_media()` uses the remembered Referer when probing an HLS URL.

### 6. Housekeeping
- Version bumped to `1.8.0` in manifest, root info, `server_version`.
- Manifest description updated to mention the in-app MultiQuality HLS stream.

## Test Results
- ✅ **106 tests passed, 0 failed** (full suite `test_403_fix.py`)
- The v1.8 MultiQuality assertions cover: the in-app `/hls/` stream is listed,
  it is web-ready, the `.ts`/`.m3u8`/`.bin` extensions survive rewriting and
  round-trip through the addon, and the segment is served as `video/mp2t`.

<br/>

---

# RareToons Addon v1.7.0 Changes

## Summary
Simplified stream listing to only show **direct file streams from Server v1, v2, v3** with **Web+Cast** support for Stremio web/Chromecast playback.

## Changes Made

### 1. Server Cap (v1, v2, v3 only)
- Added `MAX_SERVERS = 3` configuration (env: `MAX_SERVERS`)
- Only the first 3 direct-file servers from StreamBeta payload are listed
- Servers v4, v5, etc. are no longer advertised
- Placeholder sources also capped at 3 servers

### 2. Removed MultiQuality Support
- Removed `_handle(row.get("mq"), ...)` call from `resolve_one()`
- No more MultiQuality stream entries
- No HLS (.m3u8) streams listed
- `/hls/` endpoint still exists (backward compat) but isn't advertised in listings
- Direct file streams only (.mkv/.mp4)

### 3. Renamed "Proxy" to "Web+Cast"
- Stream entry renamed from `"Proxy (Web/Cast)"` to `"Web+Cast"`
- `bingeGroup` changed from `|proxy` to `|webcast`
- Removed HLS (Web/Cast) proxy entry entirely
- Only direct file proxy entries are created

### 4. Updated Manifest & Endpoints
- Version bumped to `1.7.0`
- Manifest description updated to reflect v1/v2/v3 servers and Web+Cast
- Root endpoint shows new `stream_config`

### 5. Stream Ranking Updated
- `_stream_rank()` now checks for `"web+cast"` or `"webcast"` instead of `"proxy"`

## Bugs Found & Fixed During Review

### Bug 1: HLS filtered AFTER capping → lost valid servers
**Problem:** Sources were capped at 3 first, then HLS filtered inside the loop. If source[0] was HLS, we'd cap to `[HLS, v1, v2]`, skip HLS, and only emit v1+v2 — missing v3.
**Fix:** Filter HLS sources BEFORE capping: `direct = [(i, s) for i, s in enumerate(sources) if not _is_hls_url(...)]` then `capped = direct[:MAX_SERVERS]`.

### Bug 2: Web+Cast used `capped[0]` which could be HLS
**Problem:** The Web+Cast proxy entry used `first = capped[0]` which might be the HLS source that was skipped.
**Fix:** Track `first_emitted_source` during the emit loop — the first source that actually produced a stream entry.

### Bug 3: `_handle` always returned after `_emit_sources` even if 0 emitted
**Problem:** If all sources were HLS (filtered out), `_emit_sources` returned 0 but `_handle` still `return`ed — no fallback to external/placeholder.
**Fix:** Check the return value: `if emitted: return` — fall through to fallback logic when 0.

### Bug 4: Server labels wrong after HLS filtering
**Problem:** `index` from `enumerate(capped)` counted HLS sources too, so the first non-HLS server was mislabeled "Server v2" instead of "Server v1".
**Fix:** Use `emit_idx` (position among emitted sources) for server labels, separate from `orig_idx` (position in the full source list for watch tokens).

### Bug 5: `sources.index(source)` broken for identical placeholder dicts
**Problem:** Placeholder sources are identical dicts (`{"url": "", "server": "", ...}`), so `list.index()` always returns 0 — all tokens used index 0, and the `seen` set deduplicated them to a single entry.
**Fix:** Track original indices during filtering: `direct = [(i, s) for i, s in enumerate(sources) if ...]` — no `list.index()` call needed.

### Bug 6: Wasted mq prefetch calls
**Problem:** `build_streams` and `prefetch_next_episodes` still prefetched `row.get("mq")` zippers — wasted upstream requests since mq is no longer used.
**Fix:** Removed all `prefetch(r.get("mq"))` calls.

## Stream Entry Comparison

**Before (v1.6):** ~7 entries per episode
```
• RareToons • Hindi • Server v1 • 1080p
• RareToons • Hindi • Server v2 • 720p
• RareToons • Hindi • Server v3
• RareToons • Hindi • Server v4
• RareToons • Hindi • Proxy (Web/Cast)
• RareToons • MultiQuality • Hindi • Server v1
• RareToons • MultiQuality • Hindi • HLS (Web/Cast)
```

**After (v1.7):** ~4 entries per episode
```
• RareToons • Hindi • Server v1 • 1080p
• RareToons • Hindi • Server v2 • 720p
• RareToons • Hindi • Server v3
• RareToons • Hindi • Web+Cast
```

## Test Results
- ✅ **101 tests passed, 0 failed** (full test suite `test_403_fix.py`)
- ✅ 7 inline unit tests for HLS filtering + capping logic
- ✅ Addon loads (1237 shows indexed)
- ✅ Manifest returns v1.7.0
- ✅ Stream endpoint returns simplified entries
- ✅ Server cap working (max 3 servers per episode)
- ✅ Placeholder sources produce unique tokens (Bug 5 fix verified)
- ✅ Web+Cast entry uses correct first non-HLS source (Bug 2 fix verified)
- ✅ Fallback works when all sources are HLS (Bug 3 fix verified)

## v1.8.1 — index refresh: newly uploaded shows finally have episodes (2026-09-02)

The addon serves from the pre-crawled index, so every show uploaded to
rareanimes.mov after the last crawl was invisible: Stremio showed the show
(from TMDB) but the RareToons meta matched nothing -> **empty episode list**.
New since the last crawl: Code Geass S2, SAO S1, Mob Psycho 100 S3,
Iruma-kun S3, Mushoku Tensei S3, Grand Blue S3, Gundam Hathaway movie,
Justice League S2, Recess, BoBoiBoy Galaxy Sori S2, Sparks of Tomorrow,
Cinderella Chef S2, Clevatess S2, Slime S4, Tamon's B-Side, ...

* **parse_hubs.py**: the site's new one-line episode layout
  (`Episode 01 – Title Hindi – [ WatchMultQuality ] [ StreamBeta ] …` in a
  single `<p>`) parsed to zero rows — the header regex consumed the element
  and never collected its anchors. Now the header falls through to anchor
  collection, the episode title is cut at the first bracket block, and a
  trailing language token (`… Boy Hindi`) is lifted into `lang`.
* **episodes_index.jsonl / streams.json**: refreshed from the RSS feed
  window + every /hindi/ link on /home/ (100 hub pages re-crawled, ~150 new
  episode rows, store pages merged).
* **refresh_index.py** (new): one-command incremental refresh for the next
  time the site adds shows (`python3 refresh_index.py`, then commit+push).
* Verified locally: meta + /stream for Iruma-kun S3E1, Mushoku S3E1,
  Code Geass S2E1, Sparks S1E1, Grand Blue S3E1 all list streams again
  (Server v1-v3 + MultiQuality + Web+Cast).

## v1.8.2 — boot-time index auto-refresh (2026-09-02)

The index only changed when someone ran the crawler, so shows newly
uploaded to rareanimes.mov stayed invisible until then. Now every boot
(and therefore every deploy / spin-up) starts a daemon thread that scans
the site feed + home/month listings, crawls hub pages not yet in
streams.json, merges them into episodes_index.jsonl + streams.json and
swaps the in-memory index. Any failure keeps the current index serving
(INDEX_AUTO_REFRESH=0 disables it). This boot's refresh also picked up
47 older hub pages that had never been indexed (Doraemon S2-S6, DBS
S2-S5, ...) — +48 hubs, +1293 episode rows.
