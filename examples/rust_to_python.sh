#!/bin/bash

set -e

test -f .env && source .env

echo "Sending celery task from Rust"
echo "-----------------------------"
cargo run --example celery produce
echo ""
echo ""

echo "Consuming celery task from Python"
echo "---------------------------------"
cd examples && celery \
    --app=celery_app.app worker \
    -Q celery \
    --without-heartbeat \
    -Ofair \
    --loglevel=info
