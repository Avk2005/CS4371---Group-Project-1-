#!/usr/bin/env bash
# One-time Python environment setup (run from the repo root inside WSL Ubuntu).
set -euo pipefail
cd "$(dirname "$0")/.."
sudo apt update
sudo apt install -y openjdk-17-jdk python3-venv python3-pip zip unzip
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python -m spacy download en_core_web_sm
echo "Done. Run: source .venv/bin/activate && ./scripts/check_env.sh"
