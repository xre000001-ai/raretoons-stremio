---
title: RareToons Stremio Addon
emoji: "📺"
colorFrom: red
colorTo: purple
sdk: docker
app_port: 7860
pinned: false
---

# RareToons Stremio Addon + TMDB

Indexes **all streams** scraped from RareToonsIndia (`rareanimes.com` + its
streaming host `store.animetoonhindi.com` / `codedew.com`) and serves them
through the standard [Stremio addon protocol](https://github.com/Stremio/stremio-addon-sdk).

Now with **TMDB integration** like other Stremio addons (Cinemeta-style) for posters, catalogs, and metadata fetching.

## What it serves

| Stream type | How it works |
|---|---|
| `RareToons • Hindi/Tamil/Telugu • Server vN • 1080p` | **Every** StreamBeta streaming server (v1..vN) the site exposes, listed instantly. The URL is a `/watch/{token}.mkv` link that resolves, verifies and **fails over** to a live server at playback time. Plays in Stremio desktop, Android/AndroidTV and web (via the proxy entry). |
| `RareToons • … • Proxy (Web/Cast)` | Same file, streamed **through the addon**: same-origin, CORS-open, byte-range/seek capable, correct `Content-Type`. For Stremio Web, Chromecast and strict Android/TV players that refuse a cross-origin redirect to a signed CDN URL. |
| `RareToons • MultiQuality • …` `/hls/{token}.m3u8` | The MultiQuality player is an **HLS** player (1080p/720p/480p… are HLS variants). The addon scrapes the player page and serves the **full rewritten + proxied chain** through `/hls/{token}.m3u8` — the addon rewrites the master → variants → segments/AES keys, keeps the real container extension on every child (`…/u/<b64>.ts`, `.m3u8`, `.bin`), sets the right Content-Type (`video/mp2t` for `.ts`) and sends the player-page Referer, so the adaptive multi-quality stream plays **in-app** in Stremio Web / Chromecast / Android / TV instead of opening a browser tab. |
| Whole-season ZIP packs (Mega/GoFile/Cloud), downloads | **Not** exposed as streams — streaming files only, by policy (both in the resolver and in the crawlers). |

Coverage at build time: **1,237 shows**, 21.0k per-episode/per-language
streaming link rows, 18.1k per-episode store links.
(If a show/season/episode returns nothing, the site simply doesn't carry it.)

## v1.8 — MultiQuality plays in-app (extension-preserving HLS)

The MultiQuality HLS stream now actually plays in Stremio:

* `resolve_one()` emits `RareToons • MultiQuality • {lang}` → `/hls/{token}.m3u8`
  for any row with an `mq` zipper (public host present). The endpoint resolves
  lazily at click time.
* Every rewritten child of the chain keeps its **container extension**
  (`/hls/{token}/u/<b64>.ts`, `.m3u8`, `.bin`…), so ExoPlayer / VLC / hls.js
  can tell a TS segment from a playlist, and segment `Content-Type` is inferred
  from it (`.ts` → `video/mp2t`) — this addresses the "does the .ts extension
  matter / why won't the HLS file play" question: yes, it does, and the addon
  now preserves it.
* When the zipper has no HLS master, `/hls/{token}.m3u8` 302s (like `/watch`)
  instead of serving a non-HLS body as a playlist.
* **The exact player-page Referer is threaded through the chain.** The MQ
  player page is often what the segment/key CDN checks, so `_resolve_mq_hls()`
  records it per HLS master, `_stream_hls_manifest()` replays it (via
  `_hls_chain_referer`) for every child, and `_probe_media()` uses it too.
* **Stremio SDK spec compliance:** the MQ stream entry is web-ready
  (`notWebReady: false`) and no longer sends `behaviorHints.proxyHeaders`
  (per the spec that flag requires `notWebReady: true`). fMP4 segments
  (`.m4s`) are now mapped to `video/mp4` in `CONTENT_TYPES`.

## v1.6 — MultiQuality turned into an HLS stream

The site's **MultiQuality** entries are a browser-only *HLS* player
(`argon.razorshell.space`): its quality switcher plays an HLS master playlist
(1080p/720p/480p…). Previously the addon could only resolve them to direct
`.mkv` servers (when the payload allowed) or fall back to **opening the
browser tab**. Now:

* When the codedew payload has no direct file, `_resolve_mq_hls` follows the
  zipper redirect into the player page, extracts every `.m3u8` URL from its
  HTML/JS (JSON-escaped slashes included) and, if the player loads its
  sources from a small API/config endpoint, from those too (bounded:
  `MAX_HLS_CONFIG_FETCHES`, default 2).
* The master playlist becomes an ordinary stream — one entry per playlist,
  name carries the quality from the filename (`… • 1080p`), URLs keep the
  `.m3u8` extension so ExoPlayer/VLC pick the HLS demuxer, and `/watch`
  resolves + verifies + fails over exactly like the direct servers.
* A new **`/hls/{token}.m3u8`** endpoint serves the playlist **through the
  addon** with every child URI rewritten to `/hls/{token}/u/<b64url>`:
  variants, media segments, `EXT-X-KEY` AES keys and `EXT-X-MAP` init
  segments are fetched same-origin with CORS and the right content-type
  (Referer fallbacks included — some HLS CDNs require the player-page
  referer). This is what makes the adaptive stream work on Stremio Web and
  Chromecast, where `/proxy` alone could only cover the master.
* If the player page yields no HLS either, the old browser-embed fallback is
  kept.

Tested end-to-end in the offline suite (`test_403_fix.py`, 106 checks): mocked
MultiQuality player page → resolver emits `.m3u8` streams → live `/hls`
endpoint serves rewritten master/variant/key/segment chain.

## v1.5 — all servers, fast listing, wide player support

### 1. All StreamBeta servers are advertised

The StreamBeta payload mixes streaming sources (v1..vN) with download
mirrors. v1.4 listed only servers that passed a live probe *at listing
time*, so a slow/blocked probe silently hid working servers. Now:

* every **streaming** server is listed (download mirrors — mega/mediafire/
  zip packs, `download_url` entries, archive extensions — are still never
  exposed);
* dead servers are not hidden, they are **failed over** at playback: if the
  server you clicked does not answer, `/watch` probes the others and
  redirects you to one that does, re-resolving expired signed tokens first;
* server labels/qualities come from the payload (`Server v3 • 1080p`).

### 2. Listing is fast (the "it takes too long to fetch streams" fix)

`/stream` used to do, per episode row: 1 redirect fetch + 1 POST + **one
probe per server**, ×12 rows, 3 at a time, with retry sleeps — tens of
seconds before Stremio showed anything.

| Technique | Effect |
|---|---|
| **Hard latency budget** (`LIST_BUDGET`, default 2 s, shared by the whole request) | `/stream` always answers in ~2 s worst case, no matter how many languages/rows an episode has |
| **No probing while listing** (`VERIFY_ON_LIST=0`) | removes N HTTP round trips per episode; verification moved to `/watch`, where it costs nothing perceivable |
| **Lazy tokens** | anything not resolved inside the budget is still listed — the `/watch` token resolves it at playback |
| **Server-shape memory** (`stream_hints.json`) | remembers how many servers a zipper has and how they're labelled, so even a cold start lists the right servers instantly |
| **Stale-while-revalidate** (`STALE_TTL`, 24 h) | a returning user gets the cached list immediately while it refreshes in the background |
| **In-flight dedupe** | N parallel requests for the same episode cause **one** upstream resolve |
| **Prefetch** | all zippers of the requested episode are warmed in parallel, and the next `PREFETCH_LOOKAHEAD` episodes are warmed in the background — binge-watching never waits |
| **Playback pick cache** (`PICK_TTL`, 15 min) | seeking/reconnecting does not re-probe upstream |

Measured with the offline mock (`test_403_fix.py`): 3 s upstream → listing
returns in **<1 s**; second listing **0.03 s**, zero upstream calls.

### 3. Player compatibility (Stremio desktop / web / Android / TV)

* `/watch/{token}.mkv` — the **container extension** is part of the URL, so
  ExoPlayer (Android/AndroidTV), VLC and TV players pick the right demuxer.
* **HEAD** is answered on every endpoint (players and Stremio's server
  pre-flight with HEAD; a failed HEAD reads as "stream not available").
* **CORS** headers everywhere, `Cache-Control: no-store` on playback URLs.
* `/proxy/{token}.mkv` — full byte-range proxy (`206`, `Content-Range`,
  `Accept-Ranges`, corrected `Content-Type`) for Stremio Web, Chromecast and
  players that reject cross-origin redirects. Exposed as one extra entry per
  language (`EXPOSE_PROXY_STREAMS=0` to hide it).
* Per-stream `behaviorHints`: `bingeGroup` (auto next-episode stays on the
  same server), `filename` (demuxer + subtitle matching), `videoSize`,
  `proxyHeaders` (browser UA), and honest `notWebReady` flags (`true` for
  signed CDN files, `false` for the proxied entry).
* Both `title` and `description` are sent (old and new Stremio clients).
* The public host is inferred even without `X-Forwarded-Proto`, so
  self-hosted installs (VPS/NAS/Android) still get `/watch` links instead of
  bare expiring URLs.

### Tuning (all optional env vars)

| Var | Default | Meaning |
|---|---|---|
| `LIST_BUDGET` | `2.0` | max seconds `/stream` may spend on upstream |
| `PLAYBACK_BUDGET` | `25.0` | max seconds `/watch` may spend resolving |
| `VERIFY_ON_LIST` | `0` | probe every server while listing (slow, old behaviour) |
| `PLAYBACK_VERIFY` | `1` | probe + fail over at playback |
| `DEFAULT_SERVER_SLOTS` | `4` | servers to advertise for an unknown zipper |
| `PREFETCH_ENABLED` / `PREFETCH_LOOKAHEAD` | `1` / `1` | background warming — the **next** episode only, never the one after it (no pulling ep3 while watching ep1) |
| `EXPOSE_PROXY_STREAMS` | `1` | show the proxied Web/Cast (`/proxy` or `/hls`) entry |
| `MAX_HLS_CONFIG_FETCHES` | `2` | player-page data endpoints inspected for an HLS master |
| `SB_TTL` / `FAIL_TTL` / `STALE_TTL` / `PICK_TTL` | `4h` / `120s` / `24h` / `15m` | cache lifetimes |
| `MAX_ROWS` / `RESOLVE_CONCURRENCY` / `PREFETCH_WORKERS` | `12` / `3` / `6` | fan-out limits |

## Why streams no longer 403 (v1.4, still in place)

1. **Download mirrors are filtered** out of the payload, the index and the
   crawlers — streaming files only.
2. **Signed URLs are verified** with a player-like request (browser UA, no
   Referer) before the player is sent to them — now at playback time.
3. **Failures cache for 2 min** (`FAIL_TTL`), never 6 h, so a transient
   Cloudflare 403 self-heals.
4. **Small bursts + retry/backoff + a 60 s circuit breaker** after
   connection failures.
5. **Expiring tokens** are re-resolved at playback by `/watch/{token}`.

Anything that can't be resolved degrades to the site's **browser player**
(`external`) — a working link is always preferable to a 403.

Offline self-test (no internet needed, mocks the upstream, 106 checks):
`python3 test_403_fix.py`

## TMDB integration (like Stremio addons do)

This addon now uses TMDB API key `1af06616dcbb28ff03088d87d63211f5` (embedded default) to:

- **Fetch posters, backgrounds, overviews, ratings** for your RareToons shows
- Provide **catalogs**: `/catalog/series/raretoons_series.json` and `/catalog/movie/raretoons_movies.json` with search support (`?search=naruto`)
- Provide **meta**: `/meta/series/{id}.json` and `/meta/movie/{id}.json` where `id` can be:
  - `tt...` (IMDB id) - resolved via TMDB `/find` to get name/poster, then matched to RareToons index for videos
  - `raretoons:{sanitized_key}` - internal RareToons ID (used in our own catalog)
  - numeric TMDB id
- Resolve **stream requests** without `?name=` by looking up IMDB id via TMDB

Like other Stremio community addons, you can override the TMDB key via:

- Env var: `TMDB_API_KEY=your_key python3 addon.py`
- URL prefix: `https://your-host/{TMDB_KEY}/manifest.json` -> all subsequent catalog/meta/stream calls under that prefix use that key
- Query param: `?tmdbApiKey=YOUR_KEY` or `?api_key=YOUR_KEY`

TMDB responses are cached in `tmdb_cache.json` (7 day TTL, 3 day for searches) to avoid rate limits, same pattern as popular Stremio addons.

### Endpoints

- `GET /` - addon info
- `GET /manifest.json` or `GET /{tmdbKey}/manifest.json` - manifest with catalogs
- `GET /catalog/{type}/{id}.json?search=...&skip=...` - catalog (type=series|movie, id=raretoons_series|raretoons_movies)
- `GET /catalog/{type}/{id}/{extra}.json` - alternative Stremio format where extra is `search=...&skip=...`
- `GET /meta/{type}/{id}.json` - metadata with posters + videos
- `GET /stream/{type}/{id}.json?name=...&season=...&episode=...` - streams (old Cinemeta style)
- `GET /stream/{type}/{id}.json` - new style where id is `tt123:1:2` or `raretoons:key:1:2`, name resolved via TMDB
- `GET|HEAD /watch/{token}[.ext]` - token = base64url(`zipper_url + NUL + server_index`); 302-redirects to a freshly resolved + verified streaming media URL, failing over to another server when the requested one is dead (direct `.mkv`/`.mp4` files; the MultiQuality player is served via `/hls` below)
- `GET|HEAD /proxy/{token}[.ext]` - same source streamed through the addon: same-origin, CORS-open, byte ranges (`206` + `Content-Range`), corrected `Content-Type` — for Stremio Web / Chromecast / strict Android players
- `GET|HEAD /hls/{token}.m3u8` - MultiQuality HLS master streamed + **rewritten** through the addon: every child URI (variants, segments, AES keys, `EXT-X-MAP`) is served from `/hls/{token}/u/<base64url>` same-origin with CORS and Referer fallbacks and **keeps its container extension** (`…/u/<b64>.m3u8`, `…/u/<b64>.ts`, `…/u/<b64>.bin`), with the right `Content-Type` (`video/mp2t` for `.ts`) — the multi-quality stream plays in-app, including Stremio Web / Cast
- `GET|HEAD /hls/{token}/u/<base64url>[.ext]` - one rewritten child of the HLS chain (regular HLS playlists are rewritten in turn; segments/keys streamed unchanged); the optional `.ext` restores the real container type

## Run

```bash
python3 addon.py [port]     # default 8080, binds 0.0.0.0, stdlib only
# with custom TMDB key:
TMDB_API_KEY=1af06616dcbb28ff03088d87d63211f5 python3 addon.py
```

Required files (all in this folder):
- `addon.py` — the addon server (now includes TMDB client)
- `episodes_index.jsonl` — per-episode × per-language **streaming** link index (rebuilt by `parse_hubs.py`; download-only rows are excluded)
- `streams.json` — full scrape (streaming links only: per-episode store links + hub watch links; downloads are excluded by policy)
- `test_403_fix.py` — offline self-test (mocked upstream, 106 checks)
- `stream_hints.json` — auto-created, remembered server shapes for instant listing (gitignored)
- `tmdb_cache.json` — auto-created, cached TMDB responses (gitignored)

### Refreshing the index (site adds new episodes over time)

```bash
python3 crawler.py     # re-crawl site (resumable via crawl_state.json)
python3 parse_hubs.py  # rebuild episodes_index.jsonl (resumable via parse_done.json)
# restart addon.py
```

Both crawlers are resumable and batched; `parse_hubs.py` needs ~5–8 min and
moderate RAM. Keep sequential `timeout`-bounded runs if your environment kills
long idle processes.

> **Streaming-only policy:** the crawlers and the addon index *streaming* files
> only — codedew `/zipper/` links (StreamBeta / MultiQuality players). Download
> links (DLBeta, zipcloud ZIP packs, mega, mediafire, filepress) are filtered
> out at crawl time, and the resolver drops any remaining download-only
> entries (hosts, `.zip`/`.mkv` file extensions, "download"-style labels)
> before listing, so Stremio never shows a stream that opens a download.

## Install in Stremio

1. Stremio → **Addons** → **Addons manager** (top right, plug icon) →
   **Install addon from URL**.
2. Paste: `https://<your-public-host>/manifest.json`
   - For custom TMDB key: `https://<host>/1af06616dcbb28ff03088d87d63211f5/manifest.json`
   - Or `https://<host>/manifest.json?tmdbApiKey=1af06616dcbb28ff03088d87d63211f5`
   (for this sandbox: the **preview URL of port 8080** from the workspace UI,
   i.e. `https://8080-<sandboxId>.e2b.app/manifest.json`).
3. The addon is named **RareToons (Hindi / Tamil / Telugu)**. You'll see two catalogs:
   - RareToons Hindi Dubbed (series)
   - RareToons Movies Hindi (movies)
   Pick episodes and choose a stream — direct ones play in-app, and the
   `MultiQuality` **HLS** stream plays in-app too (served through `/hls/{token}.m3u8`); it only falls back to opening a browser tab when the zipper has no HLS master.

> The sandbox preview only lives while this workspace is running. For a
> permanent addon, host this folder anywhere (VPS, Render, Fly.io, Raspberry Pi,
> …) — `addon.py` has **zero dependencies** beyond `requests`
> (`pip install requests beautifulsoup4 lxml` are only needed for the crawlers).

## Notes & caveats

- Direct URLs are signed and expire (~8 h). The addon resolves them fresh per
  request, then live-probes each source (player-style, no referer) and keeps a
  good resolver response for 4 h; a failed resolve is re-tried after only
  ~2 min, so a transient 403/TLS cut self-heals. When every listed source is
  dead, `GET /watch/{token}` re-resolves at click time and 302-redirects the
  player to a freshly verified URL.
- Only streaming sources are exposed. Download mirrors the aggregator lists as
  "Server v2..vN" (mega/mediafire/zipcloud packs) are filtered out of the
  index, the resolver output, and the crawlers.
- The player hosts sit behind Cloudflare and rotate worker domains occasionally;
  if direct streams stop resolving, the `MultiQuality` **HLS** path (and the
  browser fallback as last resort) keeps working (and a re-crawl may be
  needed if the site changes markup).
- Stream names follow the source files, e.g. `[RAI] Naruto Shippuden S08E01 (152)
  in Hindi.mkv`.
- TMDB fetching requires outbound internet (blocked in some sandboxes but works on HuggingFace Spaces / Render / VPS). If TMDB fails, addon falls back to local titles and still serves streams.
- This is an index of a third-party piracy aggregator — for personal use, and
  the copyright exposure is yours. Sites/links may break at any time.
