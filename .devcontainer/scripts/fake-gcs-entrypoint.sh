#!/bin/sh

trap "echo 'Caught termination signal. Exiting...'; exit 0" SIGINT SIGTERM

# install curl (missing in the container)
apk add curl

/bin/fake-gcs-server -data /data -scheme http -public-host fake-gcs-server:4443 &

fake_gcs_pid=$!

while ! curl $GOOGLE_SERVICE_ENDPOINT; do
    echo "Waiting for $GOOGLE_SERVICE_ENDPOINT..."
    sleep 1
done

# create fake-gcs bucket
curl -v -X POST --data-binary "{\"name\":\"$GOOGLE_TEST_BUCKET\"}" -H "Content-Type: application/json" "${GOOGLE_SERVICE_ENDPOINT}/storage/v1/b"
curl -v -X POST --data-binary "{\"name\":\"${GOOGLE_TEST_BUCKET}2\"}" -H "Content-Type: application/json" "${GOOGLE_SERVICE_ENDPOINT}/storage/v1/b"

wait $fake_gcs_pid
