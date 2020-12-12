#!/bin/sh

set -e

docker run -p 6379:6379 --rm redis
