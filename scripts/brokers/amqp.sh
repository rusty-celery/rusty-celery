#!/bin/sh

set -e

docker run -p 127.0.0.1:5672:5672 --rm rabbitmq
