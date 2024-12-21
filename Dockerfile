FROM postgres:17

RUN apt-get -y update && apt-get -y upgrade && apt-get -y install curl build-essential openssl libssl-dev pkg-config
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="$PATH:/root/.cargo/bin/"
RUN cargo install cargo-pgrx
RUN cargo pgrx init --pg17 $(which pg_config)
# RUN echo "shared_preload_libraries = 'pg_parquet'" >> ~/.pgrx/data-17/postgresql.conf
RUN echo "shared_preload_libraries = 'pg_parquet'" >> ~/.pgrx/postgresql.conf
COPY . /root/.pgrx/src
RUN cd /root/.pgrx/src && cargo pgrx run
