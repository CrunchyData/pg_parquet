ARG BASE_IMAGE_OS=almalinux
ARG BASE_IMAGE_OS_VERSION=9
FROM ${BASE_IMAGE_OS}:${BASE_IMAGE_OS_VERSION}
ARG BASE_IMAGE_OS
ARG BASE_IMAGE_OS_VERSION

ENV DEBIAN_FRONTEND="noninteractive"
ENV TZ="Europe/Istanbul"

ARG DEFAULT_PG_MAJOR=18

SHELL ["/bin/bash", "-c"]

# install deps
RUN if [ "${BASE_IMAGE_OS}" = "debian" ]; then \
      apt-get update && \
      apt-get -y install build-essential libreadline-dev zlib1g-dev flex bison         \
                         libxml2-dev libxslt-dev libssl-dev git libxml2-utils xsltproc \
                         pkg-config wget curl sudo lsb-release ca-certificates gnupg   \
                         nano net-tools libkrb5-dev python3 python3-pip python3-venv   \
                         python3-dev &&                                                \
      apt install --only-upgrade python3-pip && \
      pip3 install pipx --break-system-packages; \
    fi
RUN if [ "${BASE_IMAGE_OS}" = "almalinux" ]; then \
      dnf -y update && \
      dnf -y install epel-release && \
      (dnf config-manager --set-enabled powertools || true) && \
      (dnf config-manager --set-enabled crb || true) && \
      dnf -y install gcc gcc-c++ make readline-devel zlib-devel flex bison libxml2-devel    \
                     libxslt-devel openssl-devel pkgconfig wget ca-certificates sudo git    \
                     which gnupg nano net-tools krb5-devel python3 python3-pip              \
                     platform-python-devel &&                                               \
      pip3 install --upgrade pip && \
      pip3 install pipx; \
    fi

# install Postgres
RUN if [ "${BASE_IMAGE_OS}" = "debian" ]; then \
      wget -O- https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor > postgresql-keyring.gpg && \
      cat postgresql-keyring.gpg | sudo tee -a /usr/share/keyrings/postgresql-keyring.gpg > /dev/null && \
      sh -c 'echo "deb [signed-by=/usr/share/keyrings/postgresql-keyring.gpg] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && \
      apt-get update && apt-get -y install postgresql-{14,15,16,17,18}-postgis-3  \
                                           postgresql-{14,15,16,17,18}-pgaudit    \
                                           postgresql-server-dev-{14,15,16,17,18} \
                                           postgresql-client-{14,15,16,17,18}     \
                                           libpq-dev;                             \
    fi
RUN if [ "${BASE_IMAGE_OS}" = "almalinux" ]; then \
      ARCH=$(uname -m) && \
      dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-$(rpm -E %{rhel})-${ARCH}/pgdg-redhat-repo-latest.noarch.rpm && \
      (dnf -qy module disable postgresql || true) && \
      dnf install -y --nogpgcheck \
                     postgis36_{14,15,16,17,18}        \
                     pgaudit{16_14,17_15,_16,_17,_18}  \
                     postgresql{14,15,16,17,18}-server \
                     postgresql{14,15,16,17,18}-libs   \
                     postgresql{14,15,16,17,18}-devel; \
    fi

# default pg version for debian
ENV PATH="/usr/lib/postgresql/${DEFAULT_PG_MAJOR}/bin:${PATH}"

# default pg version for almalinux
ENV PATH="/usr/pgsql-${DEFAULT_PG_MAJOR}/bin/:${PATH}"

# set up permissions so that rust user can create extensions
RUN if [ "${BASE_IMAGE_OS}" = "debian" ]; then \
        chmod a+rwx `/usr/lib/postgresql/14/bin/pg_config --pkglibdir`          \
                    `/usr/lib/postgresql/14/bin/pg_config --pkglibdir`/bitcode  \
                    `/usr/lib/postgresql/14/bin/pg_config --sharedir`/extension \
                    /var/run/postgresql/ && \
        chmod a+rwx `/usr/lib/postgresql/15/bin/pg_config --pkglibdir`          \
                    `/usr/lib/postgresql/15/bin/pg_config --pkglibdir`/bitcode  \
                    `/usr/lib/postgresql/15/bin/pg_config --sharedir`/extension \
                    /var/run/postgresql/ && \
        chmod a+rwx `/usr/lib/postgresql/16/bin/pg_config --pkglibdir`          \
                    `/usr/lib/postgresql/16/bin/pg_config --pkglibdir`/bitcode  \
                    `/usr/lib/postgresql/16/bin/pg_config --sharedir`/extension \
                    /var/run/postgresql/ && \
        chmod a+rwx `/usr/lib/postgresql/17/bin/pg_config --pkglibdir`          \
                    `/usr/lib/postgresql/17/bin/pg_config --pkglibdir`/bitcode  \
                    `/usr/lib/postgresql/17/bin/pg_config --sharedir`/extension \
                    /var/run/postgresql/ && \
        chmod a+rwx `/usr/lib/postgresql/18/bin/pg_config --pkglibdir`          \
                    `/usr/lib/postgresql/18/bin/pg_config --pkglibdir`/bitcode  \
                    `/usr/lib/postgresql/18/bin/pg_config --sharedir`/extension \
                    /var/run/postgresql/; \
    fi
RUN if [ "${BASE_IMAGE_OS}" = "almalinux" ]; then \
        chmod a+rwx `/usr/pgsql-14/bin/pg_config --pkglibdir`          \
                    `/usr/pgsql-14/bin/pg_config --pkglibdir`/bitcode  \
                    `/usr/pgsql-14/bin/pg_config --sharedir`/extension \
                    /var/run/postgresql/ && \
        chmod a+rwx `/usr/pgsql-15/bin/pg_config --pkglibdir`          \
                    `/usr/pgsql-15/bin/pg_config --pkglibdir`/bitcode  \
                    `/usr/pgsql-15/bin/pg_config --sharedir`/extension \
                    /var/run/postgresql/ && \
        chmod a+rwx `/usr/pgsql-16/bin/pg_config --pkglibdir`          \
                    `/usr/pgsql-16/bin/pg_config --pkglibdir`/bitcode  \
                    `/usr/pgsql-16/bin/pg_config --sharedir`/extension \
                    /var/run/postgresql/ && \
        chmod a+rwx `/usr/pgsql-17/bin/pg_config --pkglibdir`          \
                    `/usr/pgsql-17/bin/pg_config --pkglibdir`/bitcode  \
                    `/usr/pgsql-17/bin/pg_config --sharedir`/extension \
                    /var/run/postgresql/ && \
        chmod a+rwx `/usr/pgsql-18/bin/pg_config --pkglibdir`          \
                    `/usr/pgsql-18/bin/pg_config --pkglibdir`/bitcode  \
                    `/usr/pgsql-18/bin/pg_config --sharedir`/extension \
                    /var/run/postgresql/; \
    fi

# initdb requires non-root user. This will also be the user that runs the container.
ARG USERNAME=rust
ARG USER_UID=1001
ARG USER_GID=1001
RUN groupadd --gid $USER_GID $USERNAME
RUN useradd --uid $USER_UID --gid $USER_GID -s /bin/bash -m $USERNAME

RUN echo "$USERNAME ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/$USERNAME

USER $USERNAME

# Install awscli, azure-cli and mitmdump with rust user
RUN pipx install mitmproxy && pipx install awscli && pipx install azure-cli
ENV PATH="~/.local/bin:${PATH}"

# install Rust environment
ARG RUSTC_VERSION=1.90.0
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y

ENV PATH="/home/rust/.cargo/bin:${PATH}"

RUN rustup install ${RUSTC_VERSION} && \
    rustup default ${RUSTC_VERSION} && \
    rustup component add rustfmt clippy llvm-tools-preview

# install llvm-cov for coverage reports
RUN cargo install --locked cargo-llvm-cov@0.6.12

# install and configure pgrx
ARG PGRX_VERSION=0.16.1
RUN cargo install --locked cargo-pgrx@${PGRX_VERSION}

ENV PGRX_TEST_PG_BASE_PORT=5454
RUN if [ "${BASE_IMAGE_OS}" = "debian" ]; then \
        cargo pgrx init --pg14 /usr/lib/postgresql/14/bin/pg_config \
                        --pg15 /usr/lib/postgresql/15/bin/pg_config \
                        --pg16 /usr/lib/postgresql/16/bin/pg_config \
                        --pg17 /usr/lib/postgresql/17/bin/pg_config \
                        --pg18 /usr/lib/postgresql/18/bin/pg_config \
                        --base-testing-port ${PGRX_TEST_PG_BASE_PORT}; \
    fi
RUN if [ "${BASE_IMAGE_OS}" = "almalinux" ]; then \
        cargo pgrx init --pg14 /usr/pgsql-14/bin/pg_config \
                        --pg15 /usr/pgsql-15/bin/pg_config \
                        --pg16 /usr/pgsql-16/bin/pg_config \
                        --pg17 /usr/pgsql-17/bin/pg_config \
                        --pg18 /usr/pgsql-18/bin/pg_config \
                        --base-testing-port ${PGRX_TEST_PG_BASE_PORT}; \
    fi

RUN echo "shared_preload_libraries = 'pgaudit,pg_parquet'" >> $HOME/.pgrx/data-14/postgresql.conf && \
    echo "shared_preload_libraries = 'pgaudit,pg_parquet'" >> $HOME/.pgrx/data-15/postgresql.conf && \
    echo "shared_preload_libraries = 'pgaudit,pg_parquet'" >> $HOME/.pgrx/data-16/postgresql.conf && \
    echo "shared_preload_libraries = 'pgaudit,pg_parquet'" >> $HOME/.pgrx/data-17/postgresql.conf && \
    echo "shared_preload_libraries = 'pgaudit,pg_parquet'" >> $HOME/.pgrx/data-18/postgresql.conf

# required for pgrx to work
ENV USER=$USERNAME

# git completion
RUN curl -o ~/.git-completion.bash https://raw.githubusercontent.com/git/git/master/contrib/completion/git-completion.bash
RUN echo "source ~/.git-completion.bash" >> ~/.bashrc
