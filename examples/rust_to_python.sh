#!/bin/bash

set -e

echo "Sending celery task from Rust"
echo "-----------------------------"
cargo run --example celery_app produce
echo ""
echo ""

echo "Consuming celery task from Python"
echo "---------------------------------"
cd examples && celery \
    --app=celery_app.my_app worker \
    -Q celery \
    -Ofair \
    --loglevel=info
