#!/usr/bin/env bash

python -m venv venv--target-postgres
source /code/venv--target-postgres/bin/activate

pip install -e .

echo -e "\n\nINFO: Dev environment ready."

tail -f /dev/null
