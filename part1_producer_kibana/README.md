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

## How to use

Step-by-step setup for a teammate who has never run this (macOS, Apple Silicon).

1. **Prerequisites:** Java 17 or newer (Kafka 4.4 supports Java 17, 21 and 25), Python 3, and Homebrew.

2. **Install Kafka:**
   ```
   brew install kafka
   ```

3. **Install the Python packages:**
   ```
   pip install websocket-client kafka-python
   ```

4. **Set up Kafka's settings.** Copy Kafka's default settings file into the project folder:
   ```
   cp /opt/homebrew/etc/kafka/server.properties ./server.properties
   ```
   Then use the `server.properties` in this repo as the reference for the extra listener.
   - Replace `<your-tailscale-ip>` with your own Tailscale address. This is only needed if a teammate must connect over Tailscale.
   - The old default `listeners` and `advertised.listeners` lines must be commented out (put `#` at the start of each) so only the new ones are active.

5. **One-time storage setup:**
   ```
   KAFKA_CLUSTER_ID="$(kafka-storage random-uuid)"
   kafka-storage format --standalone -t $KAFKA_CLUSTER_ID -c ./server.properties
   ```
   Never run the format command again after the first time.

6. **Start Kafka** and leave that terminal open:
   ```
   kafka-server-start ./server.properties
   ```

7. **In a second terminal, create the topics:**
   ```
   kafka-topics --create --topic topic1 --bootstrap-server localhost:9092
   kafka-topics --create --topic topic2 --bootstrap-server localhost:9092
   kafka-topics --list --bootstrap-server localhost:9092
   ```
   The list should show `topic1` and `topic2`.

8. **Test the producer with 5 messages:**
   ```
   python3 producer_bluesky.py --save-file test_bsky.jsonl --max-messages 5
   cat test_bsky.jsonl
   ```
   A good result: the terminal prints `Done. 5 messages sent.`, and the file has 5 lines of readable English text with no links.

9. **Real run** (about 12 minutes):
   ```
   caffeinate -i python3 producer_bluesky.py --save-file live_messages.jsonl --max-messages 1500
   ```
   `caffeinate` keeps the Mac awake only while the producer runs. Optionally, you can use `--max-minutes` instead of `--max-messages` to stop after a set time.

10. **Check the result:**
    ```
    wc -l live_messages.jsonl
    ```
    It should print `1500`.

11. **Stop Kafka** with Ctrl-C in its window. To restart later, do step 6 only.

## Troubleshooting

- A `DeprecationWarning` about `value_serializer` is harmless. Ignore it.
- "Connection refused" means Kafka is not running. Start it (step 6).
- Kafka will not start if Tailscale is off while `server.properties` uses a Tailscale address. Turn Tailscale on, or remove that listener.

## Notes

No paths or keys are hard-coded. All locations are command-line arguments.

## Data collected

The 1,500-message file `live_messages.jsonl` (collected Nov 11, 2026) is shared outside the repo and is included in the final submission ZIP.

## AI assistance

This code and README were produced with help from Claude (Anthropic), per the course AI-disclosure rule.

Logstash, Elasticsearch and Kibana steps will be added once tested.
