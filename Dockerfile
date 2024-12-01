FROM debian:bookworm-slim

# Install necessary packages
RUN apt update && apt install -y \
    wget \
    curl \
    gnupg \
    unzip \
    lsb-release \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Add PostgreSQL repository and install postgresql-client
RUN curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql-archive-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/postgresql-archive-keyring.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && apt update \
    && apt install -y postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Download and install MongoDB database tools
RUN wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian12-x86_64-100.10.0.tgz \
    && tar -xvf mongodb-database-tools-debian12-x86_64-100.10.0.tgz \
    && mv mongodb-database-tools-debian12-x86_64-100.10.0/bin/* /usr/local/bin/ \
    && rm -rf mongodb-database-tools-debian12-x86_64-100.10.0*

# Add MySQL repository and install default-mysql-client
RUN apt update \
    && apt install -y default-mysql-client \
    && rm -rf /var/lib/apt/lists/*

# Download and install MigrationDB
RUN wget -O migrationdb.zip https://github.com/Qovery/migration-db/releases/download/v0.2/migrationdb-linux-arm64.zip \
    && unzip migrationdb.zip \
    && mv migrationdb-linux-arm64 /usr/local/bin/migrationdb \
    && chmod +x /usr/local/bin/migrationdb \
    && rm migrationdb.zip

# Set the entrypoint to migrationdb
ENTRYPOINT ["migrationdb"]