#!/usr/bin/env python3
"""
Context-aware re-parse of rareanimes hub pages.

Extracts per-episode, per-language stream links:
  "Episode 01 - Somber News"
  "Hindi Uncut - [WatchMultiQuality] [StreamBeta] [DLBeta]"
  "Tamil       - [WatchMultiQuality] [StreamBeta] [DLBeta]"

Output: episodes_index.jsonl
  {show, hub_url, season, episode, ep_title, lang, mq, sb}

STREAMING FILES ONLY: download links (DLBeta/ZIP packs/zipcloud, mega,
mediafire, filepress) are never indexed - the addon serves streams, not
downloads.
"""
import concurrent.futures as cf
import json
import re
import threading
import time
from pathlib import Path
from urllib.parse import urlparse, unquote

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).parent
OUT = ROOT / "episodes_index.jsonl"
DONE = ROOT / "parse_done.json"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
WORKERS = 12
TIMEOUT = 30

lock = threading.Lock()
done = set()
if DONE.exists():
    done.update(json.loads(DONE.read_text()))

sess_local = threading.local()


def get_s():
    if not hasattr(sess_local, "s"):
        s = requests.Session()
        s.headers.update({
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        s.mount("https://", requests.adapters.HTTPAdapter(max_retries=3, pool_maxsize=16))
        sess_local.s = s
    return sess_local.s


def fetch(url):
    s = get_s()
    for attempt in range(4):
        try:
            r = s.get(url, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.text
            if r.status_code in (429, 503):
                time.sleep(4 * (attempt + 1))
                continue
            return None
        except requests.RequestException:
            time.sleep(2 * (attempt + 1))
    return None


EP_HEAD_RE = re.compile(
    r"^\s*(?:Episode|Ep(?:isode)?)\s*[-–:]?\s*(\d{1,3})\b\s*(?:[-–:]\s*(?P<title>.*))?$",
    re.I)
MAYBE_EP_RE = re.compile(r"\b(?:episode|ep)\.?\s*(\d{1,3})\b", re.I)
SEASON_RE = re.compile(r"\bseason\s*[-–:]?\s*(\d{1,3})", re.I)
S_E_RE = re.compile(r"\bS(\d{1,2})E(\d{1,3})\b", re.I)

# trailing language token of a one-line episode entry
# ('Episode 01 - The Electric Boy Hindi - [ WatchMultQuality ] ...')
LANG_TAIL_RE = re.compile(
    r"[\u2013\s-]\s*((?:hindi|english|tamil|telugu|malayalam|bengali|kannada|"
    r"marathi|punjabi|dual\s*audio|dual|multi\s*audio|multi)(?:\s*(?:sub|dub|"
    r"subbed|dubbed|audio))?)\s*$", re.I)


def classify_label(t):
    tl = t.lower()
    if re.search(r"watch\s*multi\s*quality|mult\s*quality|muliquality", tl):
        return "mq"
    if re.search(r"stream\s*beta|streambeta", tl):
        return "sb"
    return "dl"


def is_streaming_url(u):
    """True for playable stream links (codedew zipper player redirector).
    Downloads (zipcloud ZIP packs, mega/mediafire/filepress) are NOT
    streaming and are never indexed."""
    ul = (u or "").lower()
    if any(x in ul for x in ("zipcloud", "mega.nz", "mediafire",
                             "filepress", "gofile", "1fichier")):
        return False
    return "codedew.com/zipper" in ul


def parse_html(html, hub, title, season_hint):
    soup = BeautifulSoup(html, "lxml")
    content = soup.find("div", class_=lambda c: c and "entry-content" in c)
    if content is None:
        return []
    out = []
    cur_ep = None
    cur_title = ""
    season = season_hint

    for el in content.find_all(["p", "li", "h1", "h2", "h3", "h4", "h5", "h6"]):
        if el.find(["p", "li", "h1", "h2", "h3", "h4", "h5", "h6"]):  # container -> skip
            continue
        txt = el.get_text(" ", strip=True)
        if not txt:
            continue
        if season is None and len(txt) < 120:
            m = SEASON_RE.search(txt)
            if m:
                season = int(m.group(1))
        m = EP_HEAD_RE.match(txt)
        if m and len(txt) < 200:
            cur_ep = int(m.group(1))
            t = (m.group("title") or "")
            # one-line layout: the links ride in the SAME element — the
            # episode title runs only up to the first bracket block
            t = re.split(r"\s*\[", t, maxsplit=1)[0].strip(" \u2013-:[]")
            cur_title = t
            # fall through: this very element may carry the episode's links
            # (old two-line layout keeps working: no anchors here -> the
            # next element's links are attributed to cur_ep)
        anchors = [(classify_label(a.get_text(" ", strip=True)), a["href"])
                   for a in el.find_all("a", href=True) if "codedew.com" in a["href"]]
        # streaming files only: drop download (dl) links entirely
        anchors = [(k, h) for k, h in anchors if k in ("mq", "sb") and is_streaming_url(h)]
        if not anchors:
            continue
        # language: text before the first bracketed link
        lang = re.split(r"\s*[\u2013-]\s*\[", txt, maxsplit=1)[0].strip(" []\u2013-")
        mlang = LANG_TAIL_RE.search(lang)
        if mlang:
            lang = mlang.group(1).strip()      # one-liner: '... Boy Hindi'
            cur_title = re.sub(r"^(?:[\u2013\s-]\s*)?" + re.escape(mlang.group(1))
                               + r"\s*$", "", cur_title).strip(" \u2013-")
        else:
            lang = re.sub(r"^\s*(?:Episode|Ep)\.?\s*\d+\b.*$", "", lang, flags=re.I)
            lang = lang.split("\u2013")[0].split("-")[0].strip() or "Default"
        if re.match(r"(?i)^\s*(?:watch|stream|dl|mega)", lang):
            lang = "Default"
        bucket = {}
        for kind, h in anchors:
            bucket.setdefault(kind, h)
        if cur_ep is None:
            m2 = S_E_RE.search(txt)
            if m2:
                cur_ep = int(m2.group(2))
                if season is None:
                    season = int(m2.group(1))
            else:
                m3 = MAYBE_EP_RE.search(txt)
                if m3:
                    cur_ep = int(m3.group(1))
            if cur_ep is None:
                cur_ep = 1                  # single-episode page (movie)
        out.append({
            "show": title,
            "hub_url": hub,
            "season": season,
            "episode": cur_ep,
            "ep_title": cur_title,
            "lang": lang,
            "mq": bucket.get("mq"),
            "sb": bucket.get("sb"),
        })
    return out


def strong(u):
    p = urlparse(u)
    return unquote(p.path).lower().rstrip("/")


def main():
    d = json.loads((ROOT / "streams.json").read_text())
    hubs = {}
    for h in d["hub_pages"]:
        hubs[strong(h["url"])] = h

    todo = [(key, h) for key, h in hubs.items() if key not in done]
    total_rows = 0
    t0 = time.time()
    BATCH = 200
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for b0 in range(0, len(todo), BATCH):
            batch = todo[b0:b0 + BATCH]
            futs = {ex.submit(fetch, h["url"]): (key, h) for key, h in batch}
            for i, fut in enumerate(cf.as_completed(futs)):
                key, h = futs[fut]
                html = fut.result()
                futs[fut] = None            # drop reference
                with lock:
                    done.add(key)
                if html is None:
                    continue
                season_hint = None
                info = h.get("info") or {}
                if info.get("season"):
                    m = re.search(r"\d+", info["season"])
                    if m:
                        season_hint = int(m.group())
                elif SEASON_RE.search(h.get("title", "")):
                    season_hint = int(SEASON_RE.search(h["title"]).group(1))
                rows = parse_html(html, h["url"], h.get("title", ""), season_hint)
                del html
                with lock:
                    with open(OUT, "a", encoding="utf-8") as f:
                        for r in rows:
                            f.write(json.dumps(r, ensure_ascii=False) + "\n")
                    total_rows += len(rows)
            with lock:
                DONE.write_text(json.dumps(sorted(done)))
            el = time.time() - t0
            print(f"[parse] {len(done)}/{len(hubs)} pages  ep-rows={total_rows}  "
                  f"rate={len(done)/el:.1f}/s", flush=True)
    print(f"[done] pages={len(done)} ep-rows={total_rows}", flush=True)


if __name__ == "__main__":
    main()
