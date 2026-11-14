# streaming_app.py — PySpark Structured Streaming (Part 1 Spark side + Part 2.3c)

Owner: Abhi

## What it does
The app runs two streaming queries that both read `topic1`:

1. **Named-entity counts.** The app cleans each message (JSON fields and HTML tags removed), extracts named entities with spaCy, and keeps a running count per entity. At every trigger it sends the top N entities to `topic2` as JSON and prints the top 10 in the console:
   ```json
   {"entity":"OpenAI","count":42,"rank":1,"batch_id":17,"batch_time":"2026-11-14T15:30:00.000-06:00"}
   ```
   Only these spaCy labels are counted: PERSON, ORG, GPE, NORP, PRODUCT, LOC, EVENT, WORK_OF_ART, FAC, LAW and LANGUAGE. Numbers and dates are left out so they don't fill the top 10.
2. **Live classification.** The app applies the saved AG News `PipelineModel` from `prep_and_train.py` to each incoming message. It keeps a running count of messages per predicted class (World, Sports, Business, Sci/Tech) and prints it in the console at every trigger.

## Requirements
- Python 3.9+, Java 11 or 17, PySpark 3.5
- spaCy and its English model:
  ```bash
  pip install spacy
  python -m spacy download en_core_web_sm
  ```
- Kafka running, with `topic1` and `topic2` created (Kareem's setup) and `producer.py` sending to `topic1`
- The model folder from Chris

## How to run
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:<your Spark version, e.g. 3.5.3> \
  streaming_app.py \
  --model          <path/to/agnews_lr model folder> \
  --checkpoint-dir <path/to/checkpoints>
```
Check your Spark version with `spark-submit --version`. The Kafka package version must match it.

### Optional parameters
| Flag | Default | Meaning |
|---|---|---|
| `--bootstrap` | localhost:9092 | Kafka bootstrap servers |
| `--in-topic` / `--out-topic` | topic1 / topic2 | Input and output topics |
| `--class-topic` | (none) | Also send the class counts to this Kafka topic |
| `--starting-offsets` | latest | Use `earliest` to also count messages already in topic1 |
| `--trigger` | "30 seconds" | Trigger interval |
| `--top-n` | 50 | Entities sent to topic2 per trigger |
| `--entity-labels` | see above | spaCy labels to count |
| `--input-dir` | (none) | Dry run without Kafka: reads text files from a folder and prints to the console only |

Delete the checkpoint folder if you want the counts to start from zero again.

## For the Kibana side (Kareem)
Each trigger sends the current running totals again with a new `batch_time`. In Kibana, build the bar chart with:
- **X-axis:** Terms of `entity.keyword`, top 10, ordered by the metric below
- **Y-axis:** Max of `count`
- Time filter: the last few minutes, or a filter on the latest `batch_id`

Minimal Logstash pipeline:
```
input  { kafka { bootstrap_servers => "localhost:9092" topics => ["topic2"] codec => json } }
filter { date { match => ["batch_time", "ISO8601"] target => "@timestamp" } }
output { elasticsearch { hosts => ["http://localhost:9200"] index => "entities" } }
```

## What to capture for the report and video
- Screenshots of the console at several points in the run. Each one should show the "top 10 named entities" table and the `predicted_class | count` table.
- Video (your part): show the start command, then explain the two queries. Query 1 runs topic1 through cleaning and NER into running counts sent to topic2. Query 2 runs topic1 through the saved model into running class counts. Then show the counts growing over a few triggers.
