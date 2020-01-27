#!/bin/bash

set -e

test -f .env && source .env

cargo run --release --example runtime_benchmark spawn
