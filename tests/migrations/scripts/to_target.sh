#!/usr/bin/env bash
set -e -x

cat /code/tests/migrations/data/tap | /code/venv/target-postgres--${1}/bin/target-postgres --config ${2}
