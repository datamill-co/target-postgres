#!/usr/bin/env bash
set -e -x

python -m venv venv--target-postgres--schema0
source venv--target-postgres--schema0/bin/activate
pip install "singer-target-postgres==0.1.2"
deactivate

python -m venv venv--target-postgres--schema1
source venv--target-postgres--schema1/bin/activate
pip install "singer-target-postgres==0.1.9"
deactivate
