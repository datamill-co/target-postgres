#!/usr/bin/env bash
set -e -x

cd /code

source venv/target-postgres/bin/activate
pip install -U pip
/opt/poetry/bin/poetry install

cat tests/migrations/data/tap | venv/target-postgres/bin/target-postgres --config ${1}
X="$?"

deactivate

exit ${X}
