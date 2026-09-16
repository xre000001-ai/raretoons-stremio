#!/usr/bin/env python3
"""Incremental RareToons index refresh — add new shows + recent updates.

The addon serves from the pre-crawled index (episodes_index.jsonl +
streams.json), so shows uploaded to rareanimes.mov AFTER the last crawl are
invisible in Stremio (no meta match -> empty episode list). This script:

  1. collects the newest hub pages: the WordPress RSS feed (/feed/?paged=N)
     plus every /hindi/ link on /home/ and the current month archives,
  2. keeps those not already in streams.json (new shows) plus everything
     from the feed window (refresh: new episodes on updated pages),
  3. (re)crawls them with crawler.parse_hub_page + parse_hubs.parse_html
     and their store.animetoonhindi.com watch pages,
  4. merges everything into streams.json + episodes_index.jsonl.

Run from the repo root:   python3 refresh_index.py [feed_pages]

Usage notes:
  * needs `pip install requests beautifulsoup4 lxml`
  * idempotent: re-running replaces the same hubs' rows, keeps the rest
  * commit + push afterwards; the Render service redeploys with the new data
"""
import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
import crawler as C          # noqa: E402
import parse_hubs as P       # noqa: E402

SITE = "https://www.rareanimes.mov"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"}


def strong(u):
    p = urlparse(u.split("#")[0])
    return unquote(p.path).lower().rstrip("/")


def is_real_hub(u):
    p = strong(u)
    if "/category/" in p or "replytocom" in u or p.endswith("/feed") or "/feed/" in p:
        return False
    return p.startswith("/hindi/")


def collect_targets(feed_pages):
    cand, feed = set(), []
    for page in range(1, feed_pages + 1):
        r = requests.get(f"{SITE}/feed/?paged={page}", headers=UA, timeout=30)
        items = re.findall(r"<item>(.*?)</item>", r.text, re.S)
        if not items:
            break
        for it in items:
            l = re.search(r"<link>(.*?)</link>", it, re.S)
            if l:
                feed.append(l.group(1).strip())
        time.sleep(0.2)
    for listing in ("/home/", "/2026/09/", time.strftime("/%Y/%m/")):
        try:
            r = requests.get(SITE + listing, headers=UA, timeout=30)
            cand |= set(re.findall(r'href="' + SITE + r'(/hindi/[^"]+?)/?"', r.text))
        except requests.RequestException:
            pass
    cand = {SITE + c for c in cand}
    cand |= set(feed)
    return cand, feed


def run(feed_pages=12, quiet=False):
    """Refresh the index; returns a summary dict. Never raises for network
    problems (they are reported as 'fails')."""
    def log(*a):
        if not quiet:
            print(*a, flush=True)

    cand, feed = collect_targets(feed_pages)

    streams = json.loads((ROOT / "streams.json").read_text())
    hub_keys = {strong(h["url"]) for h in streams["hub_pages"]}
    targets = sorted({u for u in cand if is_real_hub(u)}
                     - {u for u in cand if strong(u) in hub_keys}) \
        + sorted({u for u in feed if is_real_hub(u) and strong(u) in hub_keys})
    targets = list(dict.fromkeys(targets))
    log(f"[targets] {len(targets)} hub pages to (re)crawl "
        f"(new + feed-window refresh)")

    hub_by_key = {strong(h["url"]): h for h in streams["hub_pages"]}
    ep_seen = {e["stream"] for e in streams["episode_streams"]}
    old_idx = [json.loads(l) for l in
               (ROOT / "episodes_index.jsonl").read_text().splitlines() if l.strip()]
    target_keys = {strong(t) for t in targets}

    new_hubs, new_rows, new_store_rows, fails = [], [], [], []
    t0 = time.time()
    for i, url in enumerate(targets):
        html, _ = C.fetch(url)
        if html is None:
            fails.append(url)
            continue
        rec = C.parse_hub_page(url, html)
        new_hubs.append(rec)
        season_hint = None
        info = rec.get("info") or {}
        m = re.search(r"\d+", str(info.get("season") or ""))
        if m:
            season_hint = int(m.group())
        else:
            m2 = P.SEASON_RE.search(rec.get("title", ""))
            if m2:
                season_hint = int(m2.group(1))
        new_rows.extend(P.parse_html(html, rec["url"], rec.get("title", ""),
                                      season_hint))
        for w in rec.get("watch_pages", []):
            shtml, _ = C.fetch(w)
            if shtml is None:
                time.sleep(2)
                shtml, _ = C.fetch(w)
            if shtml is None:
                fails.append("STORE " + w)
                continue
            pt, srows = C.parse_store_page(w, shtml)
            for r in srows:
                if r["stream"] not in ep_seen:
                    ep_seen.add(r["stream"])
                    new_store_rows.append({"source": w, "page": pt,
                                           "episode": r["episode"],
                                           "stream": r["stream"]})
        if (i + 1) % 20 == 0:
            log(f"  [{i+1}/{len(targets)}] hubs={len(new_hubs)} "
                f"ep-rows={len(new_rows)} store={len(new_store_rows)} "
                f"fails={len(fails)} {time.time()-t0:.0f}s")

    for rec in new_hubs:
        hub_by_key[strong(rec["url"])] = rec
    streams["hub_pages"] = sorted(hub_by_key.values(), key=lambda r: r["url"])
    streams["episode_streams"] = streams["episode_streams"] + new_store_rows
    streams["stats"] = {
        "hub_pages": len(streams["hub_pages"]),
        "hub_stream_links": sum(r.get("stream_count", 0)
                                for r in streams["hub_pages"]),
        "store_pages_crawled": len({e.get("source")
                                    for e in streams["episode_streams"]}),
        "episode_streams_from_store": len(streams["episode_streams"]),
        "errors": len(fails),
    }
    streams["generated_at"] = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())

    kept = [r for r in old_idx if strong(r.get("hub_url", "")) not in target_keys]
    final = kept + new_rows
    (ROOT / "streams.json").write_text(
        json.dumps(streams, ensure_ascii=False, indent=1))
    with open(ROOT / "episodes_index.jsonl", "w", encoding="utf-8") as f:
        for r in final:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    log(f"[crawl] hubs={len(new_hubs)} ep-rows={len(new_rows)} "
        f"store-rows={len(new_store_rows)} fails={len(fails)} "
        f"({time.time()-t0:.0f}s)")
    for f_ in fails[:10]:
        log("   FAIL:", f_)
    log(f"[merge] hub_pages={len(streams['hub_pages'])} "
        f"episode_streams={len(streams['episode_streams'])}")
    log(f"[merge] index rows {len(old_idx)} -> {len(final)} "
        f"(kept {len(kept)}, fresh {len(new_rows)})")
    log(f"[merge] shows {len({r['show'] for r in old_idx})} -> "
        f"{len({r['show'] for r in final})}")
    return {"targets": len(targets), "new_hubs": len(new_hubs),
            "new_rows": len(new_rows),
            "new_store_rows": len(new_store_rows),
            "fails": len(fails),
            "shows": len({r["show"] for r in final})}


def main():
    feed_pages = int(sys.argv[1]) if len(sys.argv) > 1 else 12
    run(feed_pages)


if __name__ == "__main__":
    main()
