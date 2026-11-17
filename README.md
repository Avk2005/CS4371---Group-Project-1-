# CS4371---Group-Project-1-Group #13
CS4371 Group Project 1 streams Hacker News posts with Kafka and PySpark, visualizes the top 10 named entities in Kibana, and classifies posts by topic in batch and in real time using a Spark MLlib model trained on AG News.

## How to run
Everything runs on one machine (WSL2 Ubuntu + Docker Desktop). Each folder has its own README with all options; no paths are hard coded.

| Folder | Contents |
|---|---|
| `infra/` | Docker Compose: Kafka, Elasticsearch, Kibana, Logstash |
| `producer/` | Hacker News API → Kafka `topic1` (+ saved copy) |
| `training/` | AG News counts, message cleaning, MLlib pipeline training (Part 2.1, 2.2) |
| `streaming/` | Spark Structured Streaming: named-entity counts → `topic2`, live classification (Part 1, 2.3c) |
| `scripts/` | Environment setup and checks, timed Kibana snapshots |
| `output/` | Results: Kibana snapshots, training and streaming logs |

1. Setup (once): `./scripts/setup_env.sh`, then `source .venv/bin/activate`
2. Infrastructure: `cd infra && docker compose up -d && ./create_topics.sh`
3. Producer (leave running): `python producer/producer.py --out data/hn_messages.txt`
4. Train: `python training/prep_and_train.py --agnews <AG News parquet> --messages data/hn_messages.txt --cleaned-out output/cleaned_messages --model-out models/agnews_lr`
5. Stream: `cd streaming && ./run_streaming.sh ../models/agnews_lr ../checkpoints`
6. Kibana chart and snapshots: see `infra/README.md` and `scripts/kibana_snapshots.py`
