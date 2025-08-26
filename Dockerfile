FROM golang:1.23-bookworm AS builder

WORKDIR /app
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o migrationdb main.go

FROM debian:bookworm-slim

# Add version argument
ARG VERSION=v0.2

# Set default PostgreSQL client version if not specified
ENV POSTGRESQL_CLIENT_VERSION=15

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
    # Install all PostgreSQL client versions
    && apt install -y \
       postgresql-client-15 \
       postgresql-client-16 \
       postgresql-client-17 \
    && wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian12-x86_64-100.10.0.tgz \
    && tar -xvf mongodb-database-tools-debian12-x86_64-100.10.0.tgz \
    && mv mongodb-database-tools-debian12-x86_64-100.10.0/bin/* /usr/local/bin/ \
    && rm -rf mongodb-database-tools-debian12-x86_64-100.10.0* \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from builder stage
COPY --from=builder /app/migrationdb /usr/local/bin/migrationdb
RUN chmod +x /usr/local/bin/migrationdb

# Create an entrypoint script to handle version selection
RUN echo '#!/bin/sh\n\
if [ "$POSTGRESQL_CLIENT_VERSION" != "15" ] && [ "$POSTGRESQL_CLIENT_VERSION" != "16" ] && [ "$POSTGRESQL_CLIENT_VERSION" != "17" ]; then\n\
    echo "Error: POSTGRESQL_CLIENT_VERSION must be 15, 16, or 17"\n\
    exit 1\n\
fi\n\
export PATH="/usr/lib/postgresql/${POSTGRESQL_CLIENT_VERSION}/bin:$PATH"\n\
# Verify the versions\n\
pg_dump --version\n\
pg_restore --version\n\
# Execute the main command with all arguments\n\
exec migrationdb "$@"' > /entrypoint.sh \
    && chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
