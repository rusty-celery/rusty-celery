#!/bin/sh

set -e

docker run -p 27017:27017 --rm mongo
