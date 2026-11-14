#!/usr/bin/env bash
# Checks everything the pipeline needs. Run from the repo root with .venv activated.
cd "$(dirname "$0")/.."
ok(){ echo "  [OK]   $1"; }; bad(){ echo "  [FAIL] $1"; FAIL=1; }
FAIL=0
echo "Checking environment..."
java -version >/dev/null 2>&1 && ok "Java: $(java -version 2>&1 | head -1)" || bad "Java missing (sudo apt install openjdk-17-jdk)"
python -c "import pyspark" 2>/dev/null && ok "PySpark $(python -c 'import pyspark;print(pyspark.__version__)')" || bad "PySpark missing (activate .venv, pip install -r requirements.txt)"
python -c "import spacy; spacy.load('en_core_web_sm')" 2>/dev/null && ok "spaCy + en_core_web_sm" || bad "spaCy model missing (python -m spacy download en_core_web_sm)"
command -v docker >/dev/null && docker info >/dev/null 2>&1 && ok "Docker running" || bad "Docker not reachable (start Docker Desktop, enable WSL integration)"
if docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^kafka$'; then
  ok "Kafka container up"
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q topic1 && ok "topic1 exists" || bad "topic1 missing (./infra/create_topics.sh)"
else bad "Kafka container not running (cd infra && docker compose up -d)"; fi
curl -s localhost:9200 >/dev/null && ok "Elasticsearch on :9200" || bad "Elasticsearch not reachable on :9200"
curl -s -o /dev/null localhost:5601 && ok "Kibana on :5601" || bad "Kibana not reachable on :5601 (can take ~1 min to start)"
ls data/*.parquet >/dev/null 2>&1 && ok "AG News parquet in data/" || bad "AG News parquet not in data/"
[ -d models/agnews_lr ] && ok "Model at models/agnews_lr" || bad "Model not trained yet (see training/README.md)"
[ $FAIL -eq 0 ] && echo "All good." || echo "Fix the FAIL items above."
