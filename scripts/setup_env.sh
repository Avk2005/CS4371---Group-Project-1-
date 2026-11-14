#!/usr/bin/env bash
# One-time Python environment setup (run from the repo root inside WSL Ubuntu).
# Uses uv to get Python 3.11, because PySpark 3.5 does not work with Python 3.13+/3.14.
set -euo pipefail
cd "$(dirname "$0")/.."
sudo apt update
sudo apt install -y openjdk-17-jdk zip unzip curl
command -v uv >/dev/null || { curl -LsSf https://astral.sh/uv/install.sh | sh; export PATH="$HOME/.local/bin:$PATH"; }
uv venv --python 3.11 --seed .venv
source .venv/bin/activate
uv pip install -r requirements.txt
python -m spacy download en_core_web_sm
echo "Done. Run: source .venv/bin/activate && ./scripts/check_env.sh"
