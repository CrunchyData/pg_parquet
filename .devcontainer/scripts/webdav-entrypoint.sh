#!/bin/sh

trap "echo 'Caught termination signal. Exiting...'; exit 0" SIGINT SIGTERM

# install curl (missing in the container)
apk add curl

rclone serve webdav /data --addr :8080 &

webdav_pid=$!

while ! curl $HTTP_ENDPOINT; do
    echo "Waiting for $HTTP_ENDPOINT..."
    sleep 1
done

wait $webdav_pid
