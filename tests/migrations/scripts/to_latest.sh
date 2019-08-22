#!/usr/bin/env bash
set -e -x

cd /code

source venv/target-postgres/bin/activate
pip install .

cat tests/migrations/data/tap | target-postgres --config ${1}
X="$?"

deactivate

exit ${X}
