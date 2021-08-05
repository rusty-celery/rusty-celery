#!/bin/sh

set -e

docker run -p 5672:5672 --rm rabbitmq
