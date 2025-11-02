#!/bin/sh

trap "echo 'Caught termination signal. Exiting...'; exit 0" SIGINT SIGTERM

# install curl (missing in the container)
apk add curl

azurite -l /data --blobHost azurite &

azurite_pid=$!

while ! curl $AZURE_STORAGE_ENDPOINT; do
    echo "Waiting for $AZURE_STORAGE_ENDPOINT..."
    sleep 1
done

wait $azurite_pid
