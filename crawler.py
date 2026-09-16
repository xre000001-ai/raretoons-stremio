#!/usr/bin/env python3
"""
Stream crawler for rareanimes.com (Rare Toons India).

Phase 1: BFS crawl rareanimes.com content pages -> extract "watch" hub links
         (store.animetoonhindi.com), per-episode STREAM links (codedew.com
         zipper), Vimeo embeds, and page info.
Phase 2: crawl discovered store.animetoonhindi.com archive pages ->
         per-episode stream links (codedew.com zipper).

STREAMING FILES ONLY: download links (zipcloud ZIP packs, mega, mediafire,
filepress) are never pulled - the addon indexes streams, not downloads.

Output: streams.json  (+ incremental JSONL logs, resumable via state file)
"""
import concurrent.futures as cf
import json
import random
import re
import threading
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).parent
OUT_JSON = ROOT / "streams.json"
HUB_JSONL = ROOT / "hub_pages.jsonl"
EP_JSONL = ROOT / "episode_streams.jsonl"
STATE_JSON = ROOT / "crawl_state.json"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

HOST = "www.rareanimes.com"
STORE_HOST = "store.animetoonhindi.com"

SKIP_SUBSTR = (
    "replytocom", "comment-page", "/?s=", "/page/", "/category/", "/tag/",
    "/wp-", "/feed", "/comments/", "/upload/", "/my-account/",
)
STATIC_SKIP = {
    "home", "disclaimer", "dmca", "contact-us", "cookie-policy",
    "dead-link-report", "upload-video", "my-account", "videos",
    "tv-shows", "movies", "persons", "upload", "category", "tag", "page",
    "feed", "comments", "wp-json", "wp-admin", "wp-content", "anime",
}

WORKERS = 12
DELAY = 0.1
TIMEOUT = 30

lock = threading.Lock()
done = set()          # strong keys of hub URLs
store_urls = set()
store_done = set()
errors = []


# ---------- url helpers ----------
def strong(u):
    """dedup key: (netloc, unquoted lowercase path w/o trailing slash)."""
    p = urlparse(u.split("#")[0])
    return (p.netloc, unquote(p.path).lower().rstrip("/"))


def canon(u):
    """normalized fetch URL for rareanimes content pages."""
    p = urlparse(u.split("#")[0])
    path = unquote(p.path).lower()
    if path != "/" and not path.endswith(".xml"):
        path = path.rstrip("/") + "/"
    return f"{p.scheme}://{p.netloc}{path}"


def is_download_link(url, label=""):
    """Download mirrors/packs (zipcloud ZIP, mega, mediafire, filepress,
    labels like 'ZIP'/'Download') - never pulled: streaming files only."""
    u = (url or "").lower()
    l = (label or "").lower()
    if any(x in u for x in ("zipcloud", "mega.nz", "mediafire",
                            "filepress", "gofile", "1fichier")):
        return True
    if "zip" in l or "download" in l or "dld" in l:
        return True
    return False


def is_content(u):
    p = urlparse(u.split("#")[0])
    if p.netloc != HOST:
        return False
    q = p.path + (f"?{p.query}" if p.query else "")
    if any(k in q for k in SKIP_SUBSTR):
        return False
    segs = [s for s in unquote(p.path).lower().split("/") if s]
    if not segs or segs[0] in STATIC_SKIP:
        return False
    if segs[0] == "hindi":
        return len(segs) > 1
    return True


# ---------- state ----------
def load_state():
    if STATE_JSON.exists():
        s = json.loads(STATE_JSON.read_text())
        for k in s.get("done", []):
            done.add(tuple(k))
        store_urls.update(s.get("store_urls", []))
        store_done.update(s.get("store_done", []))


def save_state():
    with lock:
        STATE_JSON.write_text(json.dumps({
            "done": [list(k) for k in sorted(done)],
            "store_urls": sorted(store_urls),
            "store_done": sorted(store_done),
        }, indent=1))


# ---------- http ----------
session_local = threading.local()


def get_session():
    if not hasattr(session_local, "s"):
        s = requests.Session()
        s.headers.update({
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        s.mount("https://", requests.adapters.HTTPAdapter(
            max_retries=3, pool_maxsize=16))
        session_local.s = s
    return session_local.s


def fetch(url, referer=None):
    """returns (html_text, final_url) or (None, final_url_or_None)."""
    s = get_session()
    headers = {"Referer": referer} if referer else {}
    final = None
    for attempt in range(4):
        try:
            r = s.get(url, headers=headers, timeout=TIMEOUT)
            final = str(r.url)
            if r.status_code == 200 and "html" in r.headers.get("content-type", ""):
                time.sleep(DELAY + random.random() * 0.15)
                return r.text, final
            if r.status_code in (429, 503):
                time.sleep(5 * (attempt + 1))
                continue
            return None, final
        except requests.RequestException:
            time.sleep(2 * (attempt + 1))
    with lock:
        errors.append({"url": url, "error": "failed after retries"})
    return None, final


def log_jsonl(path, obj):
    with lock:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


# ---------- parsing ----------
def internal_links(html):
    out = set()
    for m in re.finditer(r'href="(https?://www\.rareanimes\.mov/[^"]+)"', html):
        u = m.group(1).split("#")[0]
        if is_content(u):
            out.add(canon(u))
    return out


INFO_RE = re.compile(
    r"Anime Series Info:\s*📰\s*Full Name:\s*(?P<name>[^\n]+?)\s*"
    r"🍂\s*Season:\s*(?P<season>\S+)\s*"
    r"🎞\s*Episodes:\s*(?P<episodes>[^\n]+?)\s*"
    r"🌐\s*Network:\s*(?P<network>[^\n]+?)\s*"
    r"Year:\s*(?P<year>[^\n]+?)\s*"
    r"🔊\s*Language:\s*(?P<language>[^\n]+?)\s*"
    r"🎬\s*Quality:\s*(?P<quality>[^\n]+?)\s*(?:Synopsis|Watch-Download)",
    re.S)


def parse_hub_page(url, html):
    soup = BeautifulSoup(html, "lxml")
    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else url
    title = re.sub(r"\s*-\s*Rare Toons India.*$", "", title)

    content = soup.find("div", class_=lambda c: c and "entry-content" in c)
    info = {}
    if content:
        m = INFO_RE.search(content.get_text(" ", strip=True))
        if m:
            info = m.groupdict()

    watch_pages, streams, vimeo = [], [], []
    vimeo_ids = re.findall(r"player\.vimeo\.com/video/(\d+)", html)
    vimeo = [f"https://player.vimeo.com/video/{v}" for v in dict.fromkeys(vimeo_ids)]
    if content:
        for a in content.find_all("a", href=True):
            h = a["href"].split("#")[0]
            t = a.get_text(" ", strip=True)
            if STORE_HOST in h:
                if h not in watch_pages:
                    watch_pages.append(h)
            elif "codedew.com" in h:
                if is_download_link(h, t):
                    continue   # streaming files only, no downloads
                streams.append({"label": t or "stream", "url": h})

    return {
        "url": url,
        "title": title,
        "info": info,
        "watch_pages": watch_pages,
        "streams": streams,
        "vimeo": vimeo,
        "stream_count": len(streams),
    }


EP_RE = re.compile(r"\bS\d{1,2}E\d{1,3}\b|\bE\d{1,3}\b|\bEpisode\s*\d+", re.I)


def parse_store_page(url, html):
    soup = BeautifulSoup(html, "lxml")
    title_tag = soup.find("title")
    page_title = re.sub(r"\s*–?\s*Linker.*$", "",
                        title_tag.get_text(strip=True) if title_tag else url)
    rows, seen = [], set()
    for a in soup.find_all("a", href=True):
        h = a["href"].split("#")[0]
        t = a.get_text(" ", strip=True)
        if "codedew.com" in h and EP_RE.search(t) and h not in seen:
            if is_download_link(h, t):
                continue   # streaming files only, no downloads
            seen.add(h)
            rows.append({"episode": t, "stream": h})
    for a in soup.find_all("a", href=True):
        h = a["href"].split("#")[0]
        t = a.get_text(" ", strip=True)
        if "codedew.com" in h and h not in seen:
            if is_download_link(h, t):
                continue   # streaming files only, no downloads
            seen.add(h)
            rows.append({"episode": t or "unknown", "stream": h})
    return page_title, rows


# ---------- workers ----------
def crawl_hub(url):
    key = strong(url)
    with lock:
        if key in done:
            return None
        done.add(key)
    html, final = fetch(url)
    if html is None:
        return []
    # redirect to an already-crawled canonical page -> skip double record
    if final and strong(final) != key and strong(final) in done:
        return []
    if final:
        done.add(strong(final))
    rec = parse_hub_page(url, html)
    log_jsonl(HUB_JSONL, rec)
    for w in rec["watch_pages"]:
        store_urls.add(w)
    return list(internal_links(html))


def crawl_store(url):
    with lock:
        if url in store_done:
            return 0
        store_done.add(url)
    html, _ = fetch(url)
    if html is None:
        return 0
    page_title, rows = parse_store_page(url, html)
    for r in rows:
        log_jsonl(EP_JSONL, {"source": url, "page": page_title, **r})
    return len(rows)


# ---------- main ----------
def main():
    load_state()

    # re-seed store urls from previously collected hub records
    if HUB_JSONL.exists():
        for line in HUB_JSONL.read_text().splitlines():
            if not line.strip():
                continue
            try:
                r = json.loads(line)
                store_urls.update(r.get("watch_pages", []))
            except json.JSONDecodeError:
                pass

    seeds = set()
    if (ROOT / "sitemap.xml").exists():
        import xml.etree.ElementTree as ET
        root = ET.parse(ROOT / "sitemap.xml").getroot()
        for u in root.iter("{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
            if is_content(u.text):
                seeds.add(canon(u.text))
    for f in ("naruto_s1.html",):
        p = ROOT / f
        if p.exists():
            seeds |= internal_links(p.read_text(encoding="utf-8", errors="replace"))

    # resume: re-seed queue from links of already-crawled pages is unnecessary;
    # anything linked from done pages was already enqueued in a prior run or
    # will be re-discovered via cross-links. Start with seeds + state store set.
    queue = sorted({u for u in seeds if strong(u) not in done})
    print(f"[init] hub queue seeds: {len(queue)}  done: {len(done)}  "
          f"store known: {len(store_urls - store_done)}", flush=True)

    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        batch_no = 0
        while queue:
            batch, queue = queue[:WORKERS * 2], queue[WORKERS * 2:]
            results = list(ex.map(crawl_hub, batch))
            grow = set()
            for r in results:
                if r:
                    grow |= set(r)
            newq = {u for u in grow if strong(u) not in done}
            queue = sorted(newq | set(queue))
            batch_no += 1
            if batch_no % 5 == 0:
                save_state()
                dt = time.time() - t0
                ep = sum(1 for _ in open(EP_JSONL)) if EP_JSONL.exists() else 0
                print(f"[hub] done={len(done)} queue={len(queue)} "
                      f"rate={len(done) / dt:.1f}/s stores={len(store_urls - store_done)} "
                      f"episodes={ep}", flush=True)
            if not any(results) and not newq:
                break
    save_state()

    stores = sorted(store_urls - store_done)
    print(f"[init] store pages to fetch: {len(stores)}", flush=True)
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for i in range(0, len(stores), WORKERS):
            for r in ex.map(crawl_store, stores[i:i + WORKERS]):
                pass
            if i % 200 == 0:
                ep = sum(1 for _ in open(EP_JSONL)) if EP_JSONL.exists() else 0
                print(f"[store] {min(i + WORKERS, len(stores))}/{len(stores)} "
                      f"episodes={ep}", flush=True)
            if i % 100 == 0:
                save_state()
    save_state()

    # ---------- merge ----------
    hubs = [json.loads(l) for l in HUB_JSONL.read_text().splitlines() if l.strip()]
    by_key = {}
    for r in hubs:
        k = strong(r["url"])
        if k not in by_key or r["stream_count"] > by_key[k]["stream_count"]:
            by_key[k] = r
    hubs = sorted(by_key.values(), key=lambda r: r["url"])

    eps = [json.loads(l) for l in EP_JSONL.read_text().splitlines() if l.strip()]
    seen, eps2 = set(), []
    for e in eps:
        if e["stream"] not in seen:
            seen.add(e["stream"])
            eps2.append(e)

    out = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "source": "https://www.rareanimes.com/",
        "note": ("All stream links found on the site. episode_streams = "
                 "per-episode stream links (codedew.com zipper redirector, "
                 "browser/JS-based). hub_pages.streams = per-episode links "
                 "posted directly on hub pages; hub_pages.watch_pages = "
                 "streaming store pages; hub_pages.vimeo = embeds. "
                 "ZIP labels mark whole-season packs."),
        "stats": {
            "hub_pages": len(hubs),
            "hub_stream_links": sum(r["stream_count"] for r in hubs),
            "store_pages_crawled": len(store_done),
            "episode_streams_from_store": len(eps2),
            "errors": len(errors),
        },
        "hub_pages": hubs,
        "episode_streams": eps2,
        "errors": errors[-50:],
    }
    OUT_JSON.write_text(json.dumps(out, indent=1, ensure_ascii=False))
    print(f"[done] hubs={len(hubs)} hub_streams={out['stats']['hub_stream_links']} "
          f"store_eps={len(eps2)} -> {OUT_JSON}", flush=True)


if __name__ == "__main__":
    main()
