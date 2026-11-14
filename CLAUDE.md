# Notes for Claude Code

## Project
CS4371 Group Project 1, due Nov 15 2026 at 11:59 PM. The assignment PDFs are in `docs/` if they were added.
Pipeline: Hacker News producer → Kafka topic1 → PySpark Structured Streaming (spaCy NER running counts → topic2 → Logstash → Elasticsearch → Kibana top-10 bar chart; plus live AG News classification counts in the console). Part 2: an MLlib pipeline (RegexTokenizer, StopWordsRemover, HashingTF, IDF, LogisticRegression) trained on AG News and applied to the streamed text.

I am Abhi. I own `streaming/` and am also doing `training/` (Chris's part). The whole live pipeline runs on MY machine (WSL2 Ubuntu + Docker Desktop).

## Environment
- WSL2 Ubuntu, Python venv at `.venv` (always `source .venv/bin/activate`), Java 17, PySpark 3.5.3, spaCy en_core_web_sm
- Kafka in Docker: apps on the host use `localhost:9092`; containers use `kafka:19092`
- Elasticsearch :9200 (security off), Kibana :5601, Logstash reads topic2 into index `hn-entities`
- Kafka CLI: `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 ...`
- Spark Kafka connector: `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3` (run_streaming.sh adds it)

## Rules (from the assignment)
- No hard-coded local paths: every path is a command-line argument.
- API keys only from environment variables, never in code. Hacker News needs none.
- `data/`, `models/`, `output/` and `checkpoints/` stay out of git.
- AI use must be disclosed, so do not remove the AI-tools notes.

## Checks
- `./scripts/check_env.sh`: environment and services
- Dry run without Kafka: `python streaming/streaming_app.py --model models/agnews_lr --checkpoint-dir /tmp/ck --input-dir streaming/sample_input`
- topic2 contents: `docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic2 --max-messages 3`
- Elasticsearch: `curl localhost:9200/hn-entities/_count`

## Working style
Ask before deleting data, running `docker compose down -v`, or pushing to git. Commit to my branch only, never to main.
