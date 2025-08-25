.PHONY: init-pgrx build clean install uninstall package \
	check check-with-coverage check-minio check-azure check-gcs check-http check-format check-lint \
	start-containers stop-containers start-mitmdump stop-mitmdump start-minio stop-minio \
	start-azurite stop-azurite start-fake-gcs stop-fake-gcs start-web-dav stop-web-dav

ENVFILE ?= .devcontainer/.env

include $(ENVFILE)
export

IS_EL8 := $(shell grep -q 'PLATFORM_ID="platform:el8"' /etc/os-release 2>/dev/null && echo "true" || echo "false")

PG_CONFIG ?= pg_config

PG_MAJOR ?= $(shell $(PG_CONFIG) --version | cut -d '.' -f 1 | cut -d ' ' -f 2)

MINIO_IMAGE ?= minio/minio
AZURITE_IMAGE ?= mcr.microsoft.com/azure-storage/azurite
FAKE_GCS_IMAGE ?= tustvold/fake-gcs-server
WEBDAV_IMAGE ?= rclone/rclone

all: build

init-pgrx:
	cargo pgrx init --pg${PG_MAJOR} $(PG_CONFIG) --base-testing-port ${PGRX_TEST_PG_BASE_PORT}

build: init-pgrx
	cargo build --release --features pg$(PG_MAJOR) --no-default-features

clean:
	cargo clean

install: build
	cargo pgrx install --release --features pg$(PG_MAJOR) --no-default-features

uninstall:
	rm -f $(shell $(PG_CONFIG) --pkglibdir)/pg_parquet.so
	rm -f $(shell $(PG_CONFIG) --sharedir)/extension/pg_parquet*

package:
	cargo pgrx package --profile release --features pg$(PG_MAJOR) --no-default-features

check-format: init-pgrx
	cargo fmt --all -- --check

check-lint: init-pgrx
	cargo clippy --all-targets --features "pg$(PG_MAJOR), pg_test" --no-default-features -- -D warnings

check: build stop-containers start-containers
	cargo pgrx test pg$(PG_MAJOR) --no-default-features

check-with-coverage: init-pgrx stop-containers start-containers
	cargo llvm-cov show-env --export-prefix > llvm-cov.env && \
	. ./llvm-cov.env && \
	cargo llvm-cov clean && \
	cargo build --features "pg$(PG_MAJOR), pg_test" --no-default-features && \
	cargo pgrx test pg$(PG_MAJOR) --no-default-features && \
	cargo llvm-cov report --lcov > lcov.info

check-s3: build stop-minio start-minio
	cargo pgrx test pg$(PG_MAJOR) test_s3 --no-default-features

check-azure: build stop-azurite start-azurite
ifeq ($(IS_EL8),false)
	cargo pgrx test pg$(PG_MAJOR) test_azure --no-default-features
endif

check-gcs: build stop-fake-gcs start-fake-gcs
	cargo pgrx test pg$(PG_MAJOR) test_gcs --no-default-features

check-http: build stop-web-dav start-web-dav
	cargo pgrx test pg$(PG_MAJOR) test_http --no-default-features

start-containers: start-minio start-azurite start-fake-gcs start-web-dav

stop-containers: stop-minio stop-azurite stop-fake-gcs stop-web-dav

start-mitmdump:
	pipenv run mitmdump -s patch_arn_xml.py \
				--mode reverse:${AWS_ENDPOINT_URL} \
				--set keep_host_header=true \
				--listen-port ${AWS_ENDPOINT_PROXY_PORT} 2>&1 > /dev/null &

stop-mitmdump:
	pkill -9 mitmdump || true

start-minio: start-mitmdump
	docker run --rm -d \
	  --name minio \
	  --env-file $(ENVFILE) \
	  -p 9000:9000 \
	  --entrypoint "./entrypoint.sh" \
	  --volume ./.devcontainer/minio-entrypoint.sh:/entrypoint.sh \
	  $(MINIO_IMAGE)

	while ! curl ${AWS_ENDPOINT_URL}; do \
	  echo "Waiting for ${AWS_ENDPOINT_URL}..."; \
	  sleep 1; \
	done

stop-minio: stop-mitmdump
	docker stop minio || true

start-azurite:
ifeq ($(IS_EL8),false)
	docker run --rm -d \
	  --name azurite \
	  --env-file $(ENVFILE) \
	  -p 10000:10000 \
	  $(AZURITE_IMAGE)

	while ! curl ${AZURE_STORAGE_ENDPOINT}; do \
	  echo "Waiting for ${AZURE_STORAGE_ENDPOINT}..."; \
	  sleep 1; \
	done

	az storage container create -n ${AZURE_TEST_CONTAINER_NAME} --connection-string ${AZURE_STORAGE_CONNECTION_STRING}
	az storage container create -n ${AZURE_TEST_CONTAINER_NAME}2 --connection-string ${AZURE_STORAGE_CONNECTION_STRING}
endif

stop-azurite:
ifeq ($(IS_EL8),false)
	docker stop azurite || true
endif

start-fake-gcs:
	# tusvold/fake-gcs-server is not available for ARM64, so we build it from source
	if [ "$(shell uname -m)" != "x86_64" ]; then \
		rm -rf fake-gcs-server && \
		git clone https://github.com/tustvold/fake-gcs-server.git && \
		cd fake-gcs-server && git checkout support-xml-api && \
		docker build . -t $(FAKE_GCS_IMAGE) && \
		cd .. && rm -rf fake-gcs-server; \
	fi

	docker run --rm -d \
		--name fake-gcs-server \
		--env-file $(ENVFILE) \
		-p 4443:4443 \
		$(FAKE_GCS_IMAGE) -scheme http -public-host localhost:4443;

	while ! curl ${GOOGLE_SERVICE_ENDPOINT}; do \
	  echo "Waiting for ${GOOGLE_SERVICE_ENDPOINT}..."; \
	  sleep 1; \
	done

	curl -v -X POST --data-binary "{\"name\":\"${GOOGLE_TEST_BUCKET}\"}" -H "Content-Type: application/json" "${GOOGLE_SERVICE_ENDPOINT}/storage/v1/b"
	curl -v -X POST --data-binary "{\"name\":\"${GOOGLE_TEST_BUCKET}2\"}" -H "Content-Type: application/json" "${GOOGLE_SERVICE_ENDPOINT}/storage/v1/b"

stop-fake-gcs:
	docker stop fake-gcs-server || true

start-web-dav:
	docker run --rm -d \
	  --name rclone-webdav \
	  --env-file $(ENVFILE) \
	  -p 8080:80 \
	  $(WEBDAV_IMAGE) serve webdav /data --addr :80

	while ! curl ${HTTP_ENDPOINT}; do \
	  echo "Waiting for ${HTTP_ENDPOINT}..."; \
	  sleep 1; \
	done

stop-web-dav:
	docker stop rclone-webdav || true
