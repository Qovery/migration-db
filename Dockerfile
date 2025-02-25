FROM debian:bookworm-slim

# Set default PostgreSQL client version if not specified
ARG POSTGRESQL_CLIENT_VERSION=17
ENV POSTGRESQL_CLIENT_VERSION=${POSTGRESQL_CLIENT_VERSION}

# Install all packages in a single RUN command
RUN apt update && apt install -y \
    wget \
    curl \
    gnupg \
    unzip \
    lsb-release \
    ca-certificates \
    default-mysql-client \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql-archive-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/postgresql-archive-keyring.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && apt update \
    && apt install -y postgresql-client-${POSTGRESQL_CLIENT_VERSION} \
    && wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian12-x86_64-100.10.0.tgz \
    && tar -xvf mongodb-database-tools-debian12-x86_64-100.10.0.tgz \
    && mv mongodb-database-tools-debian12-x86_64-100.10.0/bin/* /usr/local/bin/ \
    && rm -rf mongodb-database-tools-debian12-x86_64-100.10.0* \
    && ARCH=$(uname -m) \
    && case ${ARCH} in \
           x86_64) ARCH_NAME="amd64" ;; \
           aarch64) ARCH_NAME="arm64" ;; \
           *) echo "Unsupported architecture: ${ARCH}" && exit 1 ;; \
       esac \
    && wget -O migrationdb.zip https://github.com/Qovery/migration-db/releases/download/v0.2/migrationdb-linux-${ARCH_NAME}.zip \
    && unzip migrationdb.zip \
    && mv migrationdb-linux-${ARCH_NAME} /usr/local/bin/migrationdb \
    && chmod +x /usr/local/bin/migrationdb \
    && rm migrationdb.zip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["migrationdb"]