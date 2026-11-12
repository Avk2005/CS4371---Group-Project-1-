"""Part 1 producer (Bluesky): stream live public Bluesky posts into Kafka topic1 and save a copy.

Source: Bluesky Jetstream (free, no account or API key). Keeps English posts only, removes
links, drops very short posts, and limits the speed (--rate) so the volume stays manageable.

Message sent to Kafka (UTF-8 JSON), one per post:
    {"id": "<post key>", "type": "post", "time": 1700000000, "text": "..."}
The same JSON line is appended to --save-file so Parts 3/4 can use it without Kafka.
Stops when --max-messages or --max-minutes is reached (or on Ctrl-C).
"""
import argparse
import json
import re
import sys
import time

import websocket  # pip install websocket-client
from kafka import KafkaProducer

# Public Jetstream endpoints, tried in order. The first two are the newer (v2) service,
# the last two are the older (v1) service as a fallback.
DEFAULT_URLS = [
    "wss://jetstream.us-east.bsky.network/xrpc/network.bsky.jetstream.subscribeEvents"
    "?collections=app.bsky.feed.post&kinds=commit",
    "wss://jetstream.us-west.bsky.network/xrpc/network.bsky.jetstream.subscribeEvents"
    "?collections=app.bsky.feed.post&kinds=commit",
    "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
    "wss://jetstream1.us-west.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
]
URL_RE = re.compile(r"https?://\S+")
WS_RE = re.compile(r"\s+")


def clean_text(raw: str) -> str:
    """Remove links and extra whitespace."""
    return WS_RE.sub(" ", URL_RE.sub("", raw or "")).strip()


def extract_post(event: dict):
    """Return (rkey, record) for a newly created post, or None for anything else.

    Handles both message layouts: v2 wraps the event in {"$type": "message", "payload": {...}},
    v1 has {"kind": "commit", "commit": {...}} at the top level.
    """
    if event.get("$type") == "message":
        c = event.get("payload") or {}
    elif event.get("kind") == "commit":
        c = event.get("commit") or {}
    else:
        return None
    if c.get("operation") != "create" or c.get("collection") != "app.bsky.feed.post":
        return None
    record = c.get("record") or {}
    return c.get("rkey"), record


def is_english(record: dict) -> bool:
    langs = record.get("langs") or []
    return any(str(l).lower().startswith("en") for l in langs)


def connect(urls, start_index):
    """Try each URL in turn (starting at start_index); return (socket, index) for the first that works."""
    for k in range(len(urls)):
        i = (start_index + k) % len(urls)
        for protocols in (["xrpc.v1.json"], None):  # some servers want this subprotocol, some do not
            try:
                ws = websocket.create_connection(urls[i], timeout=60, subprotocols=protocols)
                ws.settimeout(5)  # wake up every 5 s so the time limit is checked even if the stream is quiet
                print(f"connected to {urls[i].split('?')[0]}", file=sys.stderr)
                return ws, i
            except Exception as e:  # try the next option
                last_err = e
        print(f"could not connect to {urls[i].split('?')[0]}: {last_err}", file=sys.stderr)
    return None, start_index


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--bootstrap-servers", default="localhost:9092")
    ap.add_argument("--topic", default="topic1")
    ap.add_argument("--save-file", required=True, help="JSONL file that receives a copy of every message")
    ap.add_argument("--rate", type=float, default=2.0, help="max messages per second sent (default 2)")
    ap.add_argument("--min-chars", type=int, default=20, help="drop posts shorter than this after cleaning")
    ap.add_argument("--max-messages", type=int, default=0, help="stop after N sent messages (0 = no limit)")
    ap.add_argument("--max-minutes", type=float, default=0, help="stop after N minutes (0 = no limit)")
    ap.add_argument("--url", action="append", help="Jetstream URL (repeatable); overrides the built-in list")
    args = ap.parse_args()
    urls = args.url or DEFAULT_URLS

    producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers,
                             value_serializer=lambda d: json.dumps(d).encode("utf-8"))
    sent = 0
    last_sent = 0.0
    min_gap = 1.0 / args.rate if args.rate > 0 else 0.0
    deadline = time.monotonic() + args.max_minutes * 60 if args.max_minutes else None
    print(f"Sending to {args.topic}; saving to {args.save_file}", file=sys.stderr)

    def finished():
        return (args.max_messages and sent >= args.max_messages) or \
               (deadline is not None and time.monotonic() >= deadline)

    with open(args.save_file, "a", encoding="utf-8") as out:
        ws, idx = None, 0
        try:
            while not finished():
                if ws is None:
                    ws, idx = connect(urls, idx)
                    if ws is None:
                        print("no Jetstream server reachable; retrying in 10 s", file=sys.stderr)
                        time.sleep(10)
                        continue
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue  # quiet moment: just wait again
                except Exception as e:  # dropped connection: reconnect (next URL)
                    print(f"connection lost ({e}); reconnecting", file=sys.stderr)
                    try:
                        ws.close()
                    except Exception:
                        pass
                    ws, idx = None, idx + 1
                    continue
                try:
                    found = extract_post(json.loads(raw))
                except ValueError:
                    continue
                if found is None:
                    continue
                rkey, record = found
                if not is_english(record):
                    continue
                text = clean_text(record.get("text"))
                if len(text) < args.min_chars:
                    continue
                now = time.monotonic()
                if now - last_sent < min_gap:  # speed limit: skip posts that arrive too fast
                    continue
                last_sent = now
                msg = {"id": rkey, "type": "post", "time": int(time.time()), "text": text}
                producer.send(args.topic, msg)
                out.write(json.dumps(msg, ensure_ascii=False) + "\n")
                out.flush()
                sent += 1
                if sent % 50 == 0:
                    print(f"sent {sent} messages", file=sys.stderr)
        except KeyboardInterrupt:
            pass
        finally:
            producer.flush()
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass
            print(f"Done. {sent} messages sent.", file=sys.stderr)


if __name__ == "__main__":
    main()
