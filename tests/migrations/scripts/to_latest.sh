#!/usr/bin/env bash
set -e -x

source /code/venv--target-postgres/bin/activate

cat /code/tests/migrations/data/tap | target-postgres --config ${1}
X="$?"

deactivate

exit ${X}
