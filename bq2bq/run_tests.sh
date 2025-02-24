#!/bin/sh -e

pip install --no-deps -r requirements.txt
python -m unittest discover tests/
