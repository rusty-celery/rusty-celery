#!/bin/bash

set -e

echo "Sending celery task from Rust"
echo "-----------------------------"
cargo run --example celery_app produce
echo ""
echo ""

echo "Consuming celery task from Python"
echo "---------------------------------"
celery \
    --app=celery_app.my_app worker \
    --workdir=examples \
    -Q celery \
    -Ofair \
    --loglevel=info
