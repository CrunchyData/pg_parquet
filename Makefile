.PHONY: build clean install uninstall package set-pg_config-path \
	check check-with-coverage check-minio check-azure check-gcs check-http check-format check-lint \
	start-containers stop-containers

ENVFILE ?= .devcontainer/.env

include $(ENVFILE)
export

PG_MAJOR=$(shell docker compose -f .devcontainer/docker-compose.yml exec pg_parquet \
					bash -lc "pg_config --version | cut -d ' ' -f 2 | cut -d '.' -f 1")

all: build

set-pg_config-path: start-containers
	@if [ -z "$(PG_CONFIG_DIR)" ]; then \
		echo "Error: PG_CONFIG_DIR must be provided (e.g. make set-pg_config-path PG_CONFIG_DIR=/usr/lib/postgresql/17/bin)"; \
		exit 1; \
	fi; \
	docker compose -f .devcontainer/docker-compose.yml exec pg_parquet \
		bash -lc "echo 'export PATH=${PG_CONFIG_DIR}:${PATH}' >> /home/rust/.bashrc"

build: start-containers
	docker compose -f .devcontainer/docker-compose.yml exec pg_parquet \
		cargo build --release --features pg$(PG_MAJOR) --no-default-features

clean: start-containers
	docker compose -f .devcontainer/docker-compose.yml exec pg_parquet \
		cargo clean

install: start-containers
	docker compose -f .devcontainer/docker-compose.yml exec pg_parquet \
		cargo pgrx install --release --features pg$(PG_MAJOR) --no-default-features

uninstall: start-containers
	docker compose -f .devcontainer/docker-compose.yml exec pg_parquet \
		bash -lc "\
			rm -f $(pg_config --pkglibdir)/pg_parquet.so && \
			rm -f $(pg_config --sharedir)/extension/pg_parquet* \
		"

package: start-containers
	docker compose -f .devcontainer/docker-compose.yml exec pg_parquet \
		cargo pgrx package --profile release --features pg$(PG_MAJOR) --no-default-features

check-format: start-containers
	docker compose -f .devcontainer/docker-compose.yml exec pg_parquet \
		cargo fmt --all -- --check

check-lint: start-containers
	docker compose -f .devcontainer/docker-compose.yml exec pg_parquet \
		cargo clippy --all-targets --features "pg$(PG_MAJOR), pg_test" --no-default-features -- -D warnings

check: start-containers
	docker compose -f .devcontainer/docker-compose.yml exec pg_parquet \
		bash -lc "\
			export PG_MAJOR=$(PG_MAJOR) && \
			cargo pgrx test pg$(PG_MAJOR) --no-default-features \
		"

check-with-coverage: start-containers
	docker compose -f .devcontainer/docker-compose.yml exec pg_parquet \
		bash -lc "\
			export PG_MAJOR=$(PG_MAJOR) && \
			cargo llvm-cov show-env --export-prefix > llvm-cov.env && \
			. ./llvm-cov.env && \
			cargo llvm-cov clean && \
			cargo build --features 'pg$(PG_MAJOR), pg_test' --no-default-features && \
			cargo pgrx test pg$(PG_MAJOR) --no-default-features && \
			cargo llvm-cov report --lcov > lcov.info \
		"

check-s3: start-containers
	docker compose -f .devcontainer/docker-compose.yml exec pg_parquet \
		cargo pgrx test pg$(PG_MAJOR) test_s3 --no-default-features

check-azure: start-containers
	docker compose -f .devcontainer/docker-compose.yml exec pg_parquet \
		cargo pgrx test pg$(PG_MAJOR) test_azure --no-default-features

check-gcs: start-containers
	docker compose -f .devcontainer/docker-compose.yml exec pg_parquet \
		cargo pgrx test pg$(PG_MAJOR) test_gcs --no-default-features

check-http: start-containers
	docker compose -f .devcontainer/docker-compose.yml exec pg_parquet \
		cargo pgrx test pg$(PG_MAJOR) test_http --no-default-features

start-containers:
ifneq ($(shell uname -m),x86_64)
ifeq ($(FAKE_GCS_IMAGE),docker.io/tustvold/fake-gcs-server)
	# tusvold/fake-gcs-server is not available for ARM64, so we build it from source
	rm -rf fake-gcs-server && \
	git clone https://github.com/tustvold/fake-gcs-server.git && \
	cd fake-gcs-server && git checkout support-xml-api && \
	docker build . -t tustvold/fake-gcs-server && \
	cd .. && rm -rf fake-gcs-server;
endif
endif

	# start containers in detached mode
	docker compose -f .devcontainer/docker-compose.yml up -d --no-build

	is_healthy() { \
		docker compose -f .devcontainer/docker-compose.yml ps --format '{{.Service}} {{.Status}}' "$$1" 2>/dev/null \
			| grep -q 'healthy' || return 1; \
	}; \
	MAX_RETRIES=120; \
	TRIES=0; \
	until is_healthy pg_parquet && is_healthy minio && is_healthy azurite && is_healthy webdav && is_healthy fake-gcs-server; do \
		sleep 1; \
		TRIES=$$((TRIES + 1)); \
		if [ $$TRIES -ge $$MAX_RETRIES ]; then \
			echo "Containers failed to become healthy after $$MAX_RETRIES attempts."; \
			docker compose -f .devcontainer/docker-compose.yml ps; \
			exit 1; \
		fi; \
		echo "Waiting for containers to become healthy... (attempt $$TRIES/$$MAX_RETRIES)"; \
	done; \
	echo "All services healthy."

stop-containers:
	docker compose -f .devcontainer/docker-compose.yml stop
