#!/bin/sh -e

pip install --no-deps -r requirements.txt
pip install coverage
coverage run setup.py test
echo "coverage $(coverage report | awk '{print $6}' | tail -n 1)"
coverage report
