#!/bin/bash

set -e

test -f .env && source .env

echo "Sending celery task from Python"
echo "-------------------------------"
python examples/celery_app.py
echo ""
echo ""

echo "Consuming celery task from Rust"
echo "-------------------------------"
cargo run --example celery consume
