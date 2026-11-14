#!/usr/bin/env bash
# Start the GP1 streaming app (Abhi).
# Usage: ./run_streaming.sh <model_dir> <checkpoint_dir> [extra streaming_app.py flags]
# Example: ./run_streaming.sh ../models/agnews_lr ./checkpoints --trigger "30 seconds"
set -euo pipefail
if [ $# -lt 2 ]; then
  echo "Usage: $0 <model_dir> <checkpoint_dir> [extra flags]"; exit 1
fi
MODEL="$1"; CKPT="$2"; shift 2
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# Kafka connector must match the installed Spark version
SPARK_VER="$(python3 -c 'import pyspark; print(pyspark.__version__)')"
PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VER}"
echo "Spark ${SPARK_VER} | model=${MODEL} | checkpoints=${CKPT}"
exec spark-submit --packages "$PKG" "$SCRIPT_DIR/streaming_app.py" \
  --model "$MODEL" --checkpoint-dir "$CKPT" "$@"
