#!/bin/bash
set -e

# Setup directory
if [ ! -d "/sfm-data/rabbitmq" ]; then
    echo "Creating rabbitmq directory"
    mkdir /sfm-data/rabbitmq
fi
chown -R rabbitmq:rabbitmq /sfm-data/rabbitmq

# Call original entrypoint
exec docker-entrypoint.sh rabbitmq-server $@
