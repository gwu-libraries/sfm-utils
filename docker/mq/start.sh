#!/bin/bash
set -e

# Setup directory
if [ ! -d "/sfm-mq-data/rabbitmq" ]; then
    echo "Creating rabbitmq directory"
    mkdir /sfm-mq-data/rabbitmq
fi
chown -R rabbitmq:rabbitmq /sfm-mq-data/rabbitmq

# Call original entrypoint
exec docker-entrypoint.sh rabbitmq-server $@
