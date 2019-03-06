#!/usr/bin/env bash

python3-coverage run --append --include='blockchain_parser/*' --omit='*/tests/*' setup.py test
python3-coverage report
python3-coverage erase
