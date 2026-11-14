# Running the full GP1 pipeline on one machine

This folder starts Kafka, Elasticsearch, Kibana and Logstash in Docker.
`producer.py` and `streaming_app.py` run directly in WSL and connect to Kafka at `localhost:9092`.

```
producer.py ──> Kafka topic1 ──> streaming_app.py ──> Kafka topic2 ──> Logstash ──> Elasticsearch ──> Kibana
   (WSL)          (Docker)            (WSL)              (Docker)        (Docker)       (Docker)       browser
```

## One-time setup
1. Install **Docker Desktop for Windows**. In Settings → Resources → WSL Integration, turn on **Ubuntu**.
   Check from the Ubuntu terminal: `docker run hello-world`
2. Give Docker and WSL enough memory. The stack needs about 3–4 GB plus Spark. 16 GB of RAM is comfortable.
3. Your Python environment from earlier, plus the producer's Kafka library.
   Use whichever library `producer.py` imports, for example `pip install kafka-python`.

## Every run, in order
Open a few Ubuntu terminals. Each `cd` is the folder where that file lives.

**Terminal 1: infrastructure**
```bash
cd ~/gp1/infra
docker compose up -d          # first time downloads about 2 GB of images
./create_topics.sh            # should list topic1 and topic2
docker compose ps             # all 4 containers should say "running"
```
Wait about a minute, then open http://localhost:5601 in your Windows browser. Kibana should load.

**Terminal 2: producer** (keep it running the whole time)
```bash
cd ~/gp1/producer
python producer.py ...        # sends Hacker News posts to topic1 and saves a copy to a file
```
Check that messages are arriving:
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic1 --max-messages 3
```

**Terminal 3: streaming app**
```bash
cd ~/gp1/streaming
./run_streaming.sh ../models/agnews_lr ../checkpoints
```
Check that entity counts reach topic2:
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic2 --max-messages 3
```
Check that they reach Elasticsearch:
```bash
curl "http://localhost:9200/hn-entities/_count"
```

## Build the Kibana bar chart (once)
1. Go to Kibana, then ☰ → Stack Management → **Data Views** → Create data view.
   - Name and index pattern: `hn-entities`
   - Timestamp field: `@timestamp`
2. Go to ☰ → **Visualize Library** → Create → **Lens**, and pick the `hn-entities` data view.
3. Chart type: **Bar vertical**.
   - Horizontal axis: **Top values** of `entity.keyword`, **10** values, ranked by **Maximum of count**
   - Vertical axis: **Maximum** of `count`
4. Set the time range at the top right to **Last 5 minutes**, and turn on auto-refresh every 30 s.
   Each trigger re-sends the running totals, so the maximum in that window is the current total.
5. Save it, for example as "Top 10 named entities".
6. Take screenshots at **15, 30, 45 and 60 minutes** after starting the streaming app.

## Stopping and resetting
```bash
# stop streaming_app and producer with Ctrl+C
docker compose down           # stops containers, keeps Kafka and Elasticsearch data
docker compose down -v        # also deletes the Elasticsearch data (fresh index)
rm -rf ~/gp1/checkpoints # make Spark counts start from zero again
```

## If something goes wrong
| Problem | Fix |
|---|---|
| Elasticsearch keeps restarting | In PowerShell run `wsl -d docker-desktop sysctl -w vm.max_map_count=262144`, then `docker compose up -d` |
| Spark can't connect to Kafka | Check `docker compose ps`, and that `localhost:9092` is used (not `kafka:19092`) |
| Nothing in Kibana | Check `docker logs logstash` and that topic2 has messages. Widen the time range. |
| Out of memory or very slow | Close other apps and raise the memory in `.wslconfig`. Use `--trigger "60 seconds"` |
