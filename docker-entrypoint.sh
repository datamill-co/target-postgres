#!/usr/bin/env bash

python -m venv venv/target-postgres
tests/migrations/scripts/install_schema_versions.sh
source /code/venv/target-postgres/bin/activate

pip install -e .[tests]

echo "source /code/venv/target-postgres/bin/activate" >> ~/.bashrc
echo -e "\n\nINFO: Dev environment ready."

tail -f /dev/null
