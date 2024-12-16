#!/bin/sh

trap "echo 'Caught termination signal. Exiting...'; exit 0" SIGINT SIGTERM

/bin/fake-gcs-server -data /data -scheme http -public-host localhost:4443 &

gcs_pid=$!

while ! curl $GOOGLE_SERVICE_ENDPOINT; do
    echo "Waiting for $GOOGLE_SERVICE_ENDPOINT..."
    sleep 1
done

# create bucket
curl -v -X POST --data-binary "{\"name\":\"$GOOGLE_TEST_BUCKET\"}" -H "Content-Type: application/json" "$GOOGLE_SERVICE_ENDPOINT/storage/v1/b"

wait $gcs_pid
