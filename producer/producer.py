"""
CS4371 Group Project 1 - Part 1 producer
Hacker News -> Kafka topic1 (+ a saved copy, one JSON message per line)

Polls the free Hacker News API (no key needed) for new items, keeps stories and
comments that have text, and for each one:
  * sends a JSON message to Kafka:  {"id","type","by","time","title","text"}
  * appends the same JSON line to the --out file (used by prep_and_train.py)

Comment text is HTML; tags are removed later by the streaming app and training script.

Example:
  python producer/producer.py --out data/hn_messages.txt
  python producer/producer.py --out data/hn_messages.txt --backfill 1500   # also fetch the latest 1500 items first

No paths or keys are hard coded. Stop with Ctrl+C.
"""
import argparse
import json
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import requests

API = "https://hacker-news.firebaseio.com/v0"


def parse_args():
    p = argparse.ArgumentParser(description="Hacker News -> Kafka producer (GP1 Part 1)")
    p.add_argument("--out", required=True, help="File to append every sent message to (one JSON per line)")
    p.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    p.add_argument("--topic", default="topic1", help="Kafka topic (default topic1)")
    p.add_argument("--poll-seconds", type=float, default=10, help="Seconds between polls (default 10)")
    p.add_argument("--backfill", type=int, default=0,
                   help="On start, also fetch this many of the most recent items (default 0 = only new ones)")
    p.add_argument("--types", default="story,comment", help="Item types to keep (default story,comment)")
    p.add_argument("--workers", type=int, default=8, help="Parallel HTTP requests (default 8)")
    p.add_argument("--no-kafka", action="store_true", help="Test mode: only write the file, don't send to Kafka")
    return p.parse_args()


SESSION = requests.Session()


def get_json(path, retries=3):
    for attempt in range(retries):
        try:
            r = SESSION.get(f"{API}/{path}.json", timeout=10)
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError):
            if attempt == retries - 1:
                return None
            time.sleep(1 + attempt)


def to_message(item, keep_types):
    """Turn an HN item into our message dict, or None if it should be skipped."""
    if not item or item.get("deleted") or item.get("dead"):
        return None
    if item.get("type") not in keep_types:
        return None
    title, text = item.get("title") or "", item.get("text") or ""
    if not (title.strip() or text.strip()):
        return None
    return {"id": item["id"], "type": item["type"], "by": item.get("by"),
            "time": item.get("time"), "title": title, "text": text}


def main():
    args = parse_args()
    keep_types = {t.strip() for t in args.types.split(",") if t.strip()}

    producer = None
    if not args.no_kafka:
        from kafka import KafkaProducer  # pip install kafka-python
        producer = KafkaProducer(bootstrap_servers=args.bootstrap,
                                 value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                                 linger_ms=50, retries=5)

    out = open(args.out, "a", encoding="utf-8", buffering=1)  # line-buffered: every line hits disk
    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))

    max_item = get_json("maxitem")
    if max_item is None:
        sys.exit("Could not reach the Hacker News API. Check your internet connection.")
    last_seen = max_item - max(args.backfill, 0)
    sent = 0
    print(f"Starting at item {last_seen + 1} (maxitem {max_item}). "
          f"Kafka: {'OFF (test mode)' if producer is None else args.bootstrap + ' / ' + args.topic}. "
          f"Saving to {args.out}. Ctrl+C to stop.", flush=True)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        while not stop["flag"]:
            newest = get_json("maxitem")
            if newest is not None and newest > last_seen:
                # fetch in chunks so a big backfill doesn't block Ctrl+C for long
                for start in range(last_seen + 1, newest + 1, 200):
                    if stop["flag"]:
                        break
                    ids = range(start, min(start + 200, newest + 1))
                    for item in pool.map(lambda i: get_json(f"item/{i}"), ids):
                        msg = to_message(item, keep_types)
                        if msg is None:
                            continue
                        if producer is not None:
                            producer.send(args.topic, msg)
                        out.write(json.dumps(msg, ensure_ascii=False) + "\n")
                        sent += 1
                    last_seen = ids[-1]
                    if producer is not None:
                        producer.flush()
                print(f"{time.strftime('%H:%M:%S')}  up to item {last_seen}  |  messages sent: {sent}", flush=True)
            for _ in range(int(args.poll_seconds * 10)):
                if stop["flag"]:
                    break
                time.sleep(0.1)

    if producer is not None:
        producer.flush()
        producer.close()
    out.close()
    print(f"\nStopped. Total messages sent: {sent}. File: {args.out}")


if __name__ == "__main__":
    main()
