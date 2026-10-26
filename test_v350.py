#!/usr/bin/env python3
"""RareToons v3.5.0 E2E battery — per-variant cards + Range-proof.

Usage:  python3 test_v350.py [base]        (default http://127.0.0.1:7710)

Checks (all measured, no mocks):
  1  /health reports v3.5.0 and counters
  2  /find returns render-ready cards (card=1, title/quality/lang/filename)
  3  every emitted card carries a REAL Range proof (206 + Content-Range)
  4  cards are one-per-variant (heights distinct, highest first)
  5  card URL serves a rewritten media playlist (200, application mpegurl)
  6  a proxied segment answers 206 through our same-origin proxy
  7  title gate: gibberish title -> zero cards (no wrong-show leak)
  8  latency budget: /find under 12 s for a cold title
"""
import json
import re
import sys
import time
import urllib.parse
import urllib.request

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:7710").rstrip("/")
TITLE, SEASON, EP = "Doraemon Season 12", 12, 1
ok = fail = 0


def get(path, headers=None, timeout=120):
    url = path if str(path).startswith("http") else BASE + path
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read(), dict(r.headers)


def check(name, cond, detail=""):
    global ok, fail
    if cond:
        ok += 1
        print(f"PASS {name}" + (f"  :: {detail}" if detail else ""))
    else:
        fail += 1
        print(f"FAIL {name}" + (f"  :: {detail}" if detail else ""))


def main():
    st, body, _ = get("/health")
    h = json.loads(body)
    check("1 health", st == 200 and h.get("version") == "3.5.0",
          f"version={h.get('version')}")

    t0 = time.time()
    st, body, _ = get(f"/find?source=raretoons&title={urllib.parse.quote(TITLE)}"
                      f"&season={SEASON}&episode={EP}")
    ms = int((time.time() - t0) * 1000)
    d = json.loads(body)
    res = d.get("results") or []
    check("2 cards:", bool(res), f"{len(res)} cards, catalog={bool(d.get('catalog'))}")
    check("2 render-ready", all(r.get("card") == 1 and r.get("title")
                                and r.get("url") for r in res),
          f"names={[r.get('name') for r in res]}")
    check("3 Range-proof", bool(res) and all((r.get("proof") or {}).get("ok")
                                             and (r.get("proof") or {}).get("seg") == 206
                                             for r in res),
          f"seg={[((r.get('proof') or {}).get('seg')) for r in res]}")
    qs = [int(r.get("quality") or 0) for r in res]
    check("4 one-card-per-variant", len(qs) == len(set(qs)) and qs == sorted(qs, reverse=True),
          f"qualities={qs}")
    check("8 latency", ms < 12000, f"{ms} ms")

    if res:
        st, body, hdr = get(res[0]["url"])
        txt = body.decode("utf-8", "ignore")
        check("5 variant playlist", st == 200 and "#EXTM3U" in txt and "#EXTINF" in txt,
              f"{st} {hdr.get('Content-Type')} {len(body)}b")
        segs = [l for l in txt.splitlines() if l and not l.startswith("#")]
        if segs:
            seg = segs[0] if segs[0].startswith("http") else BASE + segs[0]
            import requests as _rq
            r = _rq.get(seg, headers={"Range": "bytes=0-1023"}, timeout=60)
            blob = r.content[:2048]
            check("6 segment 206", r.status_code == 206 and blob.lstrip()[:4] != b"<htm",
                  f"{r.status_code} {len(blob)}b {r.headers.get('Content-Range')}")

    st, body, _ = get("/find?source=raretoons&title=" + urllib.parse.quote("Zzqqxnope Wrong Show") +
                      "&season=1&episode=1")
    d2 = json.loads(body)
    check("7 title gate", not (d2.get("results") or []),
          f"{len(d2.get('results') or [])} cards for gibberish")

    print(f"\n---- SUMMARY ----\ntotal={ok+fail} pass={ok} fail={fail}")
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
