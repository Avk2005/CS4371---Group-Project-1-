"""
CS4371 Group Project 1 - Part 1 (Spark side) + Part 2.3c
PySpark Structured Streaming app  (Abhi)

Two streaming queries run at the same time, both reading topic1:

  Query 1  named entities
     topic1 -> clean text -> spaCy NER -> running count per entity
            -> at every trigger, send {"entity","count","rank","batch_time"} JSON
               messages to topic2 (Logstash -> Elasticsearch -> Kibana bar chart)
            -> also prints the current top 10 in the console

  Query 2  topic classification
     topic1 -> clean text -> saved AG News PipelineModel (from prep_and_train.py)
            -> running count of messages per predicted class, printed in the console
               (and optionally sent to another Kafka topic with --class-topic)

No paths are hard coded. Example:
  spark-submit \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
     streaming_app.py --model models/agnews_lr --checkpoint-dir checkpoints

For a dry run without Kafka, --input-dir reads text files dropped into a folder
and only the console output is produced.
"""
import argparse
import json
import re

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType

CLASS_NAMES = {0.0: "World", 1.0: "Sports", 2.0: "Business", 3.0: "Sci/Tech"}

# spaCy labels kept as "named entities". Numbers, dates, money etc. are dropped
# because they would otherwise fill the top 10 with "one", "today", "2".
DEFAULT_LABELS = "PERSON,ORG,GPE,NORP,PRODUCT,LOC,EVENT,WORK_OF_ART,FAC,LAW,LANGUAGE"


def parse_args():
    p = argparse.ArgumentParser(description="GP1 streaming app: NER counts + live classification")
    p.add_argument("--model", required=True, help="Folder of the saved PipelineModel (from prep_and_train.py)")
    p.add_argument("--checkpoint-dir", required=True, help="Folder for streaming checkpoints")
    p.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    p.add_argument("--in-topic", default="topic1", help="Topic with raw messages (default topic1)")
    p.add_argument("--out-topic", default="topic2", help="Topic for entity counts (default topic2)")
    p.add_argument("--class-topic", default=None, help="Optional topic for class counts (console only if omitted)")
    p.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"],
                   help="Where to start reading topic1 (default latest)")
    p.add_argument("--trigger", default="30 seconds", help="Trigger interval (default '30 seconds')")
    p.add_argument("--top-n", type=int, default=50, help="Entities sent to topic2 per trigger (default 50)")
    p.add_argument("--spacy-model", default="en_core_web_sm", help="spaCy model name")
    p.add_argument("--entity-labels", default=DEFAULT_LABELS, help="Comma list of spaCy labels to count")
    p.add_argument("--input-dir", default=None, help="Dry run: read text files from this folder instead of Kafka")
    return p.parse_args()


# ---------------------------------------------------------------- text cleaning
_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")
_ENTITIES = [("&quot;", '"'), ("&#x27;", "'"), ("&#x2F;", "/"), ("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">")]


def clean_text(raw):
    """producer.py may send plain text or JSON ({"title":..., "text":...}). Return clean text."""
    if raw is None:
        return None
    s = raw.strip()
    if s.startswith("{"):
        try:
            obj = json.loads(s)
            parts = [obj.get(k) for k in ("title", "text") if obj.get(k)]
            s = ". ".join(str(x).strip() for x in parts)
        except (ValueError, AttributeError):
            pass
    s = _TAG.sub(" ", s)
    for ent, rep in _ENTITIES:
        s = s.replace(ent, rep)
    s = _WS.sub(" ", s).strip()
    return s or None


clean_udf = F.udf(clean_text, StringType())

# ---------------------------------------------------------------- NER (spaCy)
def make_ner_udf(model_name, labels):
    keep = set(labels)

    def ner(text):
        if not text:
            return []
        import spacy
        nlp = getattr(spacy, "_gp1_nlp", None)  # cache on the module: loaded once per Python worker
        if nlp is None:
            nlp = spacy.load(model_name, disable=["lemmatizer", "textcat"])
            spacy._gp1_nlp = nlp
        out = []
        for ent in nlp(text[:5000]).ents:
            if ent.label_ not in keep:
                continue
            name = ent.text.strip().strip("\"'()[]")
            name = re.sub(r"^(the|The)\s+", "", name)
            name = re.sub(r"['’]s$", "", name).strip()
            if len(name) > 1:
                out.append(name)
        return out

    return F.udf(ner, ArrayType(StringType()))


# ---------------------------------------------------------------- main
def main():
    args = parse_args()
    spark = SparkSession.builder.appName("GP1-StreamingApp").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "4")

    # ---- source: topic1 (or a folder for a dry run)
    if args.input_dir:
        raw = spark.readStream.format("text").load(args.input_dir).select(F.col("value").alias("raw"))
    else:
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", args.bootstrap)
               .option("subscribe", args.in_topic)
               .option("startingOffsets", args.starting_offsets)
               .option("failOnDataLoss", "false")
               .load()
               .select(F.col("value").cast("string").alias("raw")))

    messages = raw.select(clean_udf("raw").alias("text")).filter(F.col("text").isNotNull())

    # ================= Query 1: running count of named entities -> topic2
    ner_udf = make_ner_udf(args.spacy_model, [x.strip() for x in args.entity_labels.split(",") if x.strip()])
    entity_counts = (messages
                     .select(F.explode(ner_udf("text")).alias("entity"))
                     .groupBy("entity").count())

    def send_entities(batch_df, batch_id):
        now = F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        top = (batch_df.orderBy(F.desc("count"), F.asc("entity")).limit(args.top_n)
               .withColumn("rank", F.row_number().over(Window.orderBy(F.desc("count"), F.asc("entity"))))
               .withColumn("batch_id", F.lit(batch_id))
               .withColumn("batch_time", now)
               .cache())
        if top.isEmpty():
            top.unpersist()
            return
        print(f"\n===== Batch {batch_id}: top 10 named entities (running totals) =====")
        top.select("entity", "count").show(10, truncate=False)
        if not args.input_dir:
            (top.select(F.col("entity").alias("key"),
                        F.to_json(F.struct("entity", "count", "rank", "batch_id", "batch_time")).alias("value"))
             .write.format("kafka")
             .option("kafka.bootstrap.servers", args.bootstrap)
             .option("topic", args.out_topic)
             .save())
        top.unpersist()

    q1 = (entity_counts.writeStream
          .outputMode("complete")
          .foreachBatch(send_entities)
          .option("checkpointLocation", f"{args.checkpoint_dir.rstrip('/')}/entities")
          .trigger(processingTime=args.trigger)
          .queryName("entity_counts")
          .start())

    # ================= Query 2: live classification with the saved model
    model = PipelineModel.load(args.model)
    name_map = F.create_map(*[x for k, v in CLASS_NAMES.items() for x in (F.lit(k), F.lit(v))])
    class_counts = (model.transform(messages)
                    .select(name_map[F.col("prediction")].alias("predicted_class"))
                    .groupBy("predicted_class").count())

    if args.class_topic and not args.input_dir:
        q2 = (class_counts
              .select(F.col("predicted_class").alias("key"),
                      F.to_json(F.struct("predicted_class", "count")).alias("value"))
              .writeStream.outputMode("complete").format("kafka")
              .option("kafka.bootstrap.servers", args.bootstrap)
              .option("topic", args.class_topic)
              .option("checkpointLocation", f"{args.checkpoint_dir.rstrip('/')}/classes_kafka")
              .trigger(processingTime=args.trigger)
              .queryName("class_counts_kafka")
              .start())
    q3 = (class_counts.orderBy(F.desc("count"))
          .writeStream.outputMode("complete").format("console")
          .option("truncate", "false")
          .option("checkpointLocation", f"{args.checkpoint_dir.rstrip('/')}/classes_console")
          .trigger(processingTime=args.trigger)
          .queryName("class_counts_console")
          .start())

    print(f"Streaming started: entities -> {args.out_topic}, class counts -> console"
          + (f" and {args.class_topic}" if args.class_topic else "") + ". Ctrl+C to stop.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
