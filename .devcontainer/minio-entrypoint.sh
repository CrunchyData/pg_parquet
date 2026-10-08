#!/bin/bash

trap "echo 'Caught termination signal. Exiting...'; exit 0" SIGINT SIGTERM

# not every image runs as root or ships a writable /data
data_dir="${MINIO_DATA_DIR:-/tmp/minio-data}"
mkdir -p "$data_dir"

minio server "$data_dir" &

minio_pid=$!

# give up instead of waiting forever if the server never becomes ready
ready=false
for _ in $(seq "${WAIT_FOR_ENDPOINT_TIMEOUT:-60}"); do
    if curl -s -o /dev/null --max-time 5 "$AWS_ENDPOINT_URL"; then
        ready=true
        break
    fi

    if ! kill -0 "$minio_pid" 2>/dev/null; then
        echo "minio server exited before it became ready"
        exit 1
    fi

    echo "Waiting for $AWS_ENDPOINT_URL..."
    sleep 1
done

if [ "$ready" = false ]; then
    echo "$AWS_ENDPOINT_URL is not ready after ${WAIT_FOR_ENDPOINT_TIMEOUT:-60} seconds"
    exit 1
fi

# set access key and secret key
mc alias set local $AWS_ENDPOINT_URL $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD

# create buckets
mc mb local/$AWS_S3_TEST_BUCKET
mc mb local/${AWS_S3_TEST_BUCKET}2

wait $minio_pid
