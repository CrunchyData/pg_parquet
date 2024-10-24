#!/bin/bash

source setup_test_envs.sh

nohup azurite --location /tmp/azurite-storage > /dev/null 2>&1 &

az storage container create --name "${AZURE_TEST_CONTAINER_NAME}" --public off --connection-string "$AZURE_STORAGE_CONNECTION_STRING"
