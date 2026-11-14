"""
CS4371 Group Project 1 - Part 2 (2.1 + 2.2)
Data prep and model training  (Chris)

What it does
  1. Loads the AG News training Parquet file and prints the number of articles per class.
  2. Loads the text streamed in Part 1, either from the file saved by producer.py
     (--messages) or with a batch Kafka read of topic1 (--kafka-bootstrap / --topic).
     Keeps only the message text, strips HTML, removes empty and duplicate messages,
     prints how many remain and saves the cleaned messages (--cleaned-out).
  3. Builds an MLlib Pipeline
        RegexTokenizer -> StopWordsRemover -> HashingTF -> IDF -> LogisticRegression
     fits it on AG News and saves the fitted PipelineModel (--model-out).

No paths are hard coded. Example:
  spark-submit prep_and_train.py \
      --agnews  data/train-00000-of-00001.parquet \
      --messages data/hn_messages.txt \
      --cleaned-out output/cleaned_messages \
      --model-out  models/agnews_lr
"""
import argparse
import json

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import IDF, HashingTF, RegexTokenizer, StopWordsRemover
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

CLASS_NAMES = {0: "World", 1: "Sports", 2: "Business", 3: "Sci/Tech"}


def parse_args():
    p = argparse.ArgumentParser(description="AG News prep + training (GP1 Part 2.1/2.2)")
    p.add_argument("--agnews", required=True, help="AG News training Parquet file (text, label)")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--messages", help="File of streamed messages saved by producer.py")
    src.add_argument("--kafka-bootstrap", help="Kafka bootstrap servers, e.g. localhost:9092 (batch read of topic1)")
    p.add_argument("--topic", default="topic1", help="Kafka topic with the raw messages (default topic1)")
    p.add_argument("--cleaned-out", required=True, help="Output folder for the cleaned messages (JSON lines)")
    p.add_argument("--model-out", required=True, help="Output folder for the fitted PipelineModel")
    p.add_argument("--num-features", type=int, default=1 << 18, help="HashingTF features (default 262144)")
    p.add_argument("--min-doc-freq", type=int, default=3, help="IDF minimum document frequency (default 3)")
    p.add_argument("--max-iter", type=int, default=50, help="LogisticRegression maxIter (default 50)")
    p.add_argument("--reg-param", type=float, default=0.01, help="LogisticRegression regParam (default 0.01)")
    p.add_argument("--holdout", type=float, default=0.1,
                   help="Fraction of AG News held out to report accuracy before the final fit (0 to skip)")
    return p.parse_args()


def extract_text(raw):
    """A producer line/message can be plain text or JSON. Return just the message text."""
    if raw is None:
        return None
    s = raw.strip()
    if s.startswith("{"):
        try:
            obj = json.loads(s)
            parts = [obj.get(k) for k in ("title", "text") if obj.get(k)]
            return ". ".join(str(x).strip() for x in parts) if parts else None
        except (ValueError, AttributeError):
            pass
    return s


extract_text_udf = F.udf(extract_text, StringType())


def load_streamed(spark, args):
    if args.messages:
        # wholetext=False -> one row per line, which is how producer.py saves messages
        raw = spark.read.text(args.messages).withColumnRenamed("value", "raw")
    else:
        raw = (spark.read.format("kafka")
               .option("kafka.bootstrap.servers", args.kafka_bootstrap)
               .option("subscribe", args.topic)
               .option("startingOffsets", "earliest")
               .option("endingOffsets", "latest")
               .load()
               .select(F.col("value").cast("string").alias("raw")))
    total = raw.count()

    text = extract_text_udf(F.col("raw"))
    text = F.regexp_replace(text, r"<[^>]+>", " ")                # strip HTML tags
    for ent, rep in [("&quot;", '"'), ("&#x27;", "'"), ("&#x2F;", "/"),
                     ("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">")]:
        text = F.regexp_replace(text, ent, rep)                     # common HN entities
    text = F.trim(F.regexp_replace(text, r"\s+", " "))

    cleaned = (raw.select(text.alias("text"))
               .filter(F.col("text").isNotNull() & (F.length("text") > 0))
               .dropDuplicates(["text"])
               .withColumn("id", F.monotonically_increasing_id()))
    return total, cleaned


def build_pipeline(args):
    tokenizer = RegexTokenizer(inputCol="text", outputCol="words",
                               pattern=r"[^a-z0-9']+", toLowercase=True, minTokenLength=2)
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    tf = HashingTF(inputCol="filtered", outputCol="tf", numFeatures=args.num_features)
    idf = IDF(inputCol="tf", outputCol="features", minDocFreq=args.min_doc_freq)
    lr = LogisticRegression(featuresCol="features", labelCol="label", family="multinomial",
                            maxIter=args.max_iter, regParam=args.reg_param, elasticNetParam=0.0)
    return Pipeline(stages=[tokenizer, remover, tf, idf, lr])


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("GP1-PrepAndTrain").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ---------- 2.1 AG News ----------
    ag = (spark.read.parquet(args.agnews)
          .select(F.col("text").cast("string"), F.col("label").cast("double"))
          .na.drop())
    ag.cache()
    print("\n=== AG News training data ===")
    print(f"Total articles: {ag.count()}")
    counts = ag.groupBy("label").count().orderBy("label").collect()
    for r in counts:
        print(f"  {int(r['label'])} {CLASS_NAMES[int(r['label'])]:<9} {r['count']}")

    # ---------- 2.1 Streamed messages ----------
    total, cleaned = load_streamed(spark, args)
    cleaned.cache()
    remaining = cleaned.count()
    print("\n=== Streamed messages (Part 1) ===")
    print(f"Messages read:                          {total}")
    print(f"After removing empty + duplicate:       {remaining}")
    print(f"Removed:                                {total - remaining}")
    if remaining < 1000:
        print("WARNING: fewer than 1,000 messages remain - keep producer.py running longer.")
    cleaned.select("id", "text").coalesce(1).write.mode("overwrite").json(args.cleaned_out)
    print(f"Cleaned messages saved to: {args.cleaned_out}")

    # ---------- 2.2 Pipeline ----------
    pipeline = build_pipeline(args)
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")

    if args.holdout > 0:
        train, test = ag.randomSplit([1 - args.holdout, args.holdout], seed=42)
        m = pipeline.fit(train)
        pred = m.transform(test)
        acc = evaluator.setMetricName("accuracy").evaluate(pred)
        f1 = evaluator.setMetricName("f1").evaluate(pred)
        print(f"\n=== Hold-out check ({args.holdout:.0%} of AG News) ===")
        print(f"Accuracy: {acc:.4f}   Weighted F1: {f1:.4f}")

    print("\nFitting final model on the full AG News training set ...")
    model = pipeline.fit(ag)
    acc_train = evaluator.setMetricName("accuracy").evaluate(model.transform(ag))
    print(f"Training accuracy: {acc_train:.4f}")
    model.write().overwrite().save(args.model_out)
    print(f"PipelineModel saved to: {args.model_out}")
    print("Stages:", [type(s).__name__ for s in model.stages])

    spark.stop()


if __name__ == "__main__":
    main()
