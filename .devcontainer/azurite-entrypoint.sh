#!/bin/bash

trap "echo 'Caught termination signal. Exiting...'; exit 0" SIGINT SIGTERM

minio server /data &

azurite_pid=$!

while ! curl $AWS_ENDPOINT_URL; do
    echo "Waiting for $AWS_ENDPOINT_URL..."
    sleep 1
done

# create container
az storage container create -n $AZURE_TEST_CONTAINER_NAME --connection-string $AZURE_STORAGE_CONNECTION_STRING

wait $azurite_pid
