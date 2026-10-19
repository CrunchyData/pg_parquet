.PHONY: init-pgrx build clean install uninstall package \
	check check-libs check-with-coverage check-minio check-azure check-gcs check-http check-format check-lint \
	start-containers stop-containers start-mitmdump stop-mitmdump start-minio stop-minio \
	start-azurite stop-azurite start-fake-gcs stop-fake-gcs start-web-dav stop-web-dav

ENVFILE ?= .devcontainer/.env

include $(ENVFILE)
export

IS_EL8 := $(shell grep -q 'PLATFORM_ID="platform:el8"' /etc/os-release 2>/dev/null && echo "true" || echo "false")

PG_CONFIG ?= pg_config

# a beta reports e.g. "PostgreSQL 19beta4", so take the leading digits only
PG_MAJOR ?= $(shell $(PG_CONFIG) --version | sed -E 's/^PostgreSQL ([0-9]+).*/\1/')

# The extensions of this repo, in the order they install: each one's control file `requires`
# the one before it, so `CREATE EXTENSION pg_parquet` needs all of them present. `make install`
# installing only pg_parquet would leave a cluster that cannot create it.
EXTENSIONS ?= pg_object_store pg_arrow_base pg_arrow pg_parquet

# Tested from the top down. `cargo pgrx test` leaves a `pg_test` build of the package it tested
# installed, and a `pg_test` build owns the `tests` schema that pgrx's test functions go in --
# so `CREATE EXTENSION` of anything that requires it then fails with "schema tests is not a
# member of extension", and every later test reports "Could not obtain test mutex". Descending
# order means nothing is tested after a tier below it was replaced.
TEST_ORDER ?= pg_parquet pg_arrow pg_arrow_base pg_object_store

# The crates with no Postgres in them, linted and unit tested without a pg feature.
# pg-arrow-convert is not among them: it speaks pg_sys and so builds per major.
LIBS ?= object-store-client object-store-core format-registry arrow-ipc-format parquet-format

# MinIO gated its community images: minio/minio and quay.io/minio/minio no longer
# allow anonymous pulls, and dl.min.io returns 410. This is an archived MinIO build.
MINIO_IMAGE ?= bitnamilegacy/minio:2025.7.23-debian-12-r5
AZURITE_IMAGE ?= mcr.microsoft.com/azure-storage/azurite
FAKE_GCS_IMAGE ?= tustvold/fake-gcs-server
WEBDAV_IMAGE ?= rclone/rclone

# Containers do not always come up, e.g. when an image pull is rate limited, so
# the readiness checks below give up instead of waiting forever. Seconds.
WAIT_FOR_ENDPOINT_TIMEOUT ?= 60

# $(call wait_for_endpoint,<url>). curl is only asked to reach the endpoint, not
# to get a 2xx out of it: a bare GET on the root of these services answers with
# 400 or 403 once they are up.
define wait_for_endpoint
	for _ in $$(seq ${WAIT_FOR_ENDPOINT_TIMEOUT}); do \
	  curl -s -o /dev/null --max-time 5 $(1) && exit 0; \
	  echo "Waiting for $(1)..."; \
	  sleep 1; \
	done; \
	echo "$(1) is not ready after ${WAIT_FOR_ENDPOINT_TIMEOUT} seconds"; \
	exit 1
endef

all: build

init-pgrx:
	cargo pgrx init --pg${PG_MAJOR} $(PG_CONFIG) --base-testing-port ${PGRX_TEST_PG_BASE_PORT}

build: init-pgrx
	for ext in $(EXTENSIONS); do \
	  cargo build --package $$ext --release --features pg$(PG_MAJOR) --no-default-features || exit 1; \
	done

clean:
	cargo clean

install: build
	for ext in $(EXTENSIONS); do \
	  cargo pgrx install --package $$ext --release --features pg$(PG_MAJOR) --no-default-features || exit 1; \
	done

uninstall:
	for ext in $(EXTENSIONS); do \
	  rm -f $(shell $(PG_CONFIG) --pkglibdir)/$$ext.so; \
	  rm -f $(shell $(PG_CONFIG) --sharedir)/extension/$$ext*; \
	done

package:
	for ext in $(EXTENSIONS); do \
	  cargo pgrx package --package $$ext --profile release --features pg$(PG_MAJOR) --no-default-features || exit 1; \
	done

check-format: init-pgrx
	cargo fmt --all -- --check

check-lint: init-pgrx
	for ext in $(EXTENSIONS); do \
	  cargo clippy --package $$ext --all-targets --features "pg$(PG_MAJOR), pg_test" --no-default-features -- -D warnings || exit 1; \
	done
	cargo clippy --package pg-arrow-convert --all-targets --features pg$(PG_MAJOR) --no-default-features -- -D warnings
	cargo clippy $(addprefix --package ,$(LIBS)) --all-targets -- -D warnings

# The unit tests of the crates that need no Postgres at all: the formats themselves, uri
# parsing, the registry. Seconds, and no cluster.
check-libs:
	cargo test $(addprefix --package ,$(LIBS))

# `install` rather than `build`: every tier has to be in the cluster, built *without* pg_test,
# before the tier above it can be created. See TEST_ORDER for why the order is what it is.
check: install check-libs stop-containers start-containers
	for ext in $(TEST_ORDER); do \
	  cargo pgrx test --package $$ext pg$(PG_MAJOR) --no-default-features || exit 1; \
	done

check-with-coverage: install stop-containers start-containers
	cargo llvm-cov show-env --export-prefix > llvm-cov.env && \
	. ./llvm-cov.env && \
	cargo llvm-cov clean && \
	for ext in $(TEST_ORDER); do \
	  cargo build --package $$ext --features "pg$(PG_MAJOR), pg_test" --no-default-features || exit 1; \
	  cargo pgrx test --package $$ext pg$(PG_MAJOR) --no-default-features || exit 1; \
	done && \
	cargo llvm-cov report --lcov > lcov.info

# A name filter that matches nothing still exits 0, and the ~50 cloud tests these four targets
# select have not been ported out of 0.6.0 yet: they run zero tests and pass. Whichever tier
# ends up owning them, these need `--package` and a filter that matches.
check-s3: install stop-minio start-minio
	cargo pgrx test --package pg_parquet pg$(PG_MAJOR) test_s3 --no-default-features

check-azure: install stop-azurite start-azurite
ifeq ($(IS_EL8),false)
	cargo pgrx test --package pg_parquet pg$(PG_MAJOR) test_azure --no-default-features
endif

check-gcs: install stop-fake-gcs start-fake-gcs
	cargo pgrx test --package pg_parquet pg$(PG_MAJOR) test_gcs --no-default-features

check-http: install stop-web-dav start-web-dav
	cargo pgrx test --package pg_parquet pg$(PG_MAJOR) test_http --no-default-features

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
	  --entrypoint /entrypoint.sh \
	  --volume ./.devcontainer/minio-entrypoint.sh:/entrypoint.sh \
	  $(MINIO_IMAGE)

	$(call wait_for_endpoint,${AWS_ENDPOINT_URL})

stop-minio: stop-mitmdump
	docker stop minio || true

start-azurite:
ifeq ($(IS_EL8),false)
	docker run --rm -d \
	  --name azurite \
	  --env-file $(ENVFILE) \
	  -p 10000:10000 \
	  $(AZURITE_IMAGE)

	$(call wait_for_endpoint,${AZURE_STORAGE_ENDPOINT})

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

	$(call wait_for_endpoint,${GOOGLE_SERVICE_ENDPOINT})

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

	$(call wait_for_endpoint,${HTTP_ENDPOINT})

stop-web-dav:
	docker stop rclone-webdav || true
