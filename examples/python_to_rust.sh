#!/bin/bash

set -e

echo "Sending celery task from Python"
echo "-------------------------------"
python examples/celery_app.py
echo ""
echo ""

echo "Consuming celery task from Rust"
echo "-------------------------------"
cargo run --example celery_app consume
