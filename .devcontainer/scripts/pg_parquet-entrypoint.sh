#!/bin/bash

trap "echo 'Caught termination signal. Exiting...'; exit 0" SIGINT SIGTERM

# create azurite container (az cli not found in azurite container, so we create them here)
az storage container create -n $AZURE_TEST_CONTAINER_NAME --connection-string $AZURE_STORAGE_CONNECTION_STRING
az storage container create -n ${AZURE_TEST_CONTAINER_NAME}2 --connection-string $AZURE_STORAGE_CONNECTION_STRING

# fix volume permissions
sudo chown -R rust:rust /workspace

uvx --from mitmproxy mitmdump \
	-s patch_arn_xml.py \
	--mode reverse:${AWS_ENDPOINT_URL} \
	--set keep_host_header=true \
	--listen-port ${AWS_ENDPOINT_PROXY_PORT}
