#!/bin/sh

set -e

docker run -p 5672:5672 --rm -e RABBITMQ_DEFAULT_VHOST=my_vhost rabbitmq:3
