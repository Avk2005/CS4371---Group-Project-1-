# prep_and_train.py — Data prep and model training (Part 2.1 + 2.2)

Owner: Chris

## What it does
1. Loads the AG News training file and prints the number of articles in each class.
2. Loads the messages streamed in Part 1, keeps only the text, strips HTML tags, removes empty and duplicate messages, and prints how many remain.
3. Saves the cleaned messages as JSON lines (`id`, `text`) for the batch predictions step.
4. Builds an MLlib Pipeline: `RegexTokenizer → StopWordsRemover → HashingTF → IDF → LogisticRegression`.
5. Prints the accuracy on a 10% hold-out split of AG News, then fits the pipeline on the full training set and saves the fitted `PipelineModel`.

## Requirements
- Python 3.9+, Java 11 or 17, PySpark 3.5 (`pip install pyspark`)
- The AG News training file `train-00000-of-00001.parquet` from https://huggingface.co/datasets/fancyzhx/ag_news (Files → data). It has 120,000 rows with the columns `text` and `label`.
- The messages file saved by `producer.py`, one message per line. Plain text or JSON lines with `title`/`text` fields both work.

## How to run
All paths are command-line arguments. Nothing is hard coded.

```bash
spark-submit prep_and_train.py \
    --agnews      <path/to/train-00000-of-00001.parquet> \
    --messages    <path/to/producer_output.txt> \
    --cleaned-out <output/cleaned_messages> \
    --model-out   <models/agnews_lr>
```
`python prep_and_train.py ...` also works if PySpark was installed with pip.

To read topic1 directly from Kafka instead of the saved file, replace `--messages` with
`--kafka-bootstrap localhost:9092 --topic topic1` and add
`--packages org.apache.spark:spark-sql-kafka-0-10_2.12:<your Spark version>` to `spark-submit`.

### Optional parameters
| Flag | Default | Meaning |
|---|---|---|
| `--num-features` | 262144 | HashingTF feature count |
| `--min-doc-freq` | 3 | IDF minimum document frequency |
| `--max-iter` | 50 | LogisticRegression iterations |
| `--reg-param` | 0.01 | LogisticRegression regularization |
| `--holdout` | 0.1 | Share of AG News held out for the accuracy check (0 skips it) |

## Output
- Console: AG News count per class, messages read, messages after cleaning, hold-out accuracy and F1.
- `<cleaned-out>/part-*.json`: cleaned messages. Send this folder to Ashwin.
- `<model-out>/`: the fitted PipelineModel. Send this folder to Abhi and Ashwin.

Load the model with `PipelineModel.load(path)` and run `model.transform(df)` on a DataFrame with a `text` column. The output has `prediction` (0 World, 1 Sports, 2 Business, 3 Sci/Tech) and `probability`.
