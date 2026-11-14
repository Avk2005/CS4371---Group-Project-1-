# producer.py: Hacker News → Kafka topic1

Polls the free Hacker News API (https://github.com/HackerNews/API, no key needed) for new items.
It keeps stories and comments that have text. Each one is sent to Kafka as JSON
(`{"id","type","by","time","title","text"}`) and also appended as one line to the `--out` file.

## Run
```bash
source .venv/bin/activate
python producer/producer.py --out data/hn_messages.txt
```
Leave it running. Check progress with `wc -l data/hn_messages.txt` (you need at least 1,000). Stop with Ctrl+C.

| Flag | Default | Meaning |
|---|---|---|
| `--out` | required | File the messages are appended to |
| `--bootstrap` | localhost:9092 | Kafka servers |
| `--topic` | topic1 | Kafka topic |
| `--poll-seconds` | 10 | Seconds between checks for new items |
| `--backfill` | 0 | Also fetch this many of the most recent items when starting |
| `--types` | story,comment | Item types to keep |
| `--no-kafka` | off | Test mode: only write the file |

Comment text is HTML. The streaming app and training script remove the tags.
