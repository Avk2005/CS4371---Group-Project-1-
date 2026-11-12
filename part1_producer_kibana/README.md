# Part 1: Bluesky Producer

`producer_bluesky.py` reads live public Bluesky posts and sends them to Kafka.

## What it does

- Reads live public posts from Bluesky Jetstream (free, no API key or account needed).
- Keeps English posts only.
- Removes links from the text.
- Drops posts shorter than `--min-chars` (after cleaning).
- Limits speed with `--rate` (default 2 messages per second).
- Sends each post as JSON to Kafka `topic1`.
- Saves the same JSON line to a file (`--save-file`).

If the connection drops, it reconnects (trying the next Jetstream URL). It stops when `--max-messages` or `--max-minutes` is reached, or on Ctrl-C.

## Message format

```json
{"id": "<post key>", "type": "post", "time": 1700000000, "text": "..."}
```

No usernames are stored.

## Requirements

- Python 3
- A running Kafka broker with `topic1` created
- `pip install websocket-client kafka-python`

## How to run

Quick test (5 messages):

```
python3 producer_bluesky.py --save-file test_bsky.jsonl --max-messages 5
```

Longer collection (keeps the Mac awake):

```
caffeinate -i python3 producer_bluesky.py --save-file live_messages.jsonl --max-messages 1500
```

## Options

| Option | Default | Meaning |
|---|---|---|
| `--bootstrap-servers` | `localhost:9092` | Kafka broker address |
| `--topic` | `topic1` | Kafka topic to send to |
| `--save-file` | (required) | JSONL file that receives a copy of every message |
| `--rate` | `2.0` | Max messages per second |
| `--min-chars` | `20` | Drop posts shorter than this after cleaning |
| `--max-messages` | `0` (no limit) | Stop after N sent messages |
| `--max-minutes` | `0` (no limit) | Stop after N minutes |
| `--url` | built-in Jetstream list | Jetstream URL (repeatable); overrides the built-in list |

## How to check it worked

```
wc -l <save-file>
kafka-topics --list --bootstrap-server localhost:9092
```

`wc -l` should match the number of messages sent, and `topic1` should appear in the topic list.

## Notes

No paths or keys are hard-coded. All locations are command-line arguments.

## Data collected

The 1,500-message file `live_messages.jsonl` (collected Nov 11, 2026) is shared outside the repo and is included in the final submission ZIP.

## AI assistance

This code and README were produced with help from Claude (Anthropic), per the course AI-disclosure rule.
