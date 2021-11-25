#!/bin/sh

set -e

docker run -p 127.0.0.1:6379:6379 --rm redis
