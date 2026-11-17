"""
Take Kibana screenshots (and a text backup of the top 10) at set minutes after start.

Start it right after starting streaming_app.py:
  python scripts/kibana_snapshots.py --url "<your saved Kibana chart URL>" --out output/screenshots

Defaults: snapshots at 15, 30, 45 and 60 minutes. For a quick test use: --minutes 0 1
One-time setup:  pip install playwright && playwright install --with-deps chromium
"""
import argparse
import json
import os
import time
from datetime import datetime

import requests


def parse_args():
    p = argparse.ArgumentParser(description="Timed Kibana screenshots + top-10 text snapshots")
    p.add_argument("--url", required=True, help="Kibana URL of the saved bar chart (copy from the browser)")
    p.add_argument("--out", required=True, help="Folder to save PNGs and text snapshots")
    p.add_argument("--minutes", type=float, nargs="+", default=[15, 30, 45, 60], help="When to snapshot")
    p.add_argument("--es", default="http://localhost:9200", help="Elasticsearch URL")
    p.add_argument("--index", default="hn-entities", help="Elasticsearch index")
    p.add_argument("--window", default="5m", help="Time window for the text top 10 (default 5m)")
    p.add_argument("--wait", type=int, default=15, help="Seconds to let Kibana render before the shot")
    return p.parse_args()


def text_top10(args):
    """Same numbers the chart shows: top 10 entities by max running count in the recent window."""
    q = {"size": 0,
         "query": {"range": {"@timestamp": {"gte": f"now-{args.window}"}}},
         "aggs": {"top": {"terms": {"field": "entity.keyword", "size": 10, "order": {"mx": "desc"}},
                          "aggs": {"mx": {"max": {"field": "count"}}}}}}
    r = requests.post(f"{args.es}/{args.index}/_search", json=q, timeout=20)
    r.raise_for_status()
    return [(b["key"], int(b["mx"]["value"])) for b in r.json()["aggregations"]["top"]["buckets"]]


def screenshot(args, path):
    from playwright.sync_api import sync_playwright
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(viewport={"width": 1600, "height": 1000})
        page.goto(args.url, wait_until="networkidle", timeout=120_000)
        page.wait_for_timeout(args.wait * 1000)
        page.screenshot(path=path, full_page=True)
        browser.close()


def main():
    args = parse_args()
    os.makedirs(args.out, exist_ok=True)
    start = time.time()
    print(f"Started {datetime.now():%H:%M:%S}. Snapshots at minutes: {args.minutes}. Saving to {args.out}")
    for m in sorted(args.minutes):
        delay = start + m * 60 - time.time()
        if delay > 0:
            print(f"  waiting {delay / 60:.1f} min for the {m:g}-minute snapshot ...", flush=True)
            time.sleep(delay)
        stamp = datetime.now().strftime("%H%M")
        base = os.path.join(args.out, f"kibana_{m:g}min_{stamp}")
        try:
            top = text_top10(args)
            with open(base + ".txt", "w") as f:
                f.write(f"Top 10 named entities at {m:g} minutes ({datetime.now():%Y-%m-%d %H:%M:%S})\n")
                for i, (name, c) in enumerate(top, 1):
                    f.write(f"{i:>2}. {name:<30} {c}\n")
            print(f"  [{m:g} min] text top 10 saved: {base}.txt  ->  {top[:3]}")
        except Exception as e:  # keep going even if ES hiccups
            print(f"  [{m:g} min] text snapshot failed: {e}")
        try:
            screenshot(args, base + ".png")
            print(f"  [{m:g} min] screenshot saved: {base}.png")
        except Exception as e:
            print(f"  [{m:g} min] screenshot failed: {e}  (take a manual one with Win+Shift+S)")
    print("Done.")


if __name__ == "__main__":
    main()
