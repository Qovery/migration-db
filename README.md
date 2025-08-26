# MigrationDB

MigrationDB is a robust database migration tool that enables streaming data transfers between databases of the same type. It provides seamless migration capabilities for popular database systems, with built-in verification and flexible output options.

> [!NOTE]  
> If your database is large (around 50 GB or more), the migration process may take significant time, and MigrationDB might not be the most suitable tool. In such cases, consider using dedicated solutions provided by cloud providers, such as [AWS DMS](https://docs.aws.amazon.com/dms/latest/userguide/Welcome.html) 

## Supported Databases

- PostgreSQL
- MySQL
- MongoDB

## Features

- [x] Direct streaming migration between same-type databases
- [x] Data verification after migration (with checksums)
- [x] Support for stdout streaming
- [x] Connection validation and testing
- [x] Configurable buffer sizes and timeouts
- [x] TLS/SSL support with optional verification skip
- [x] Signal handling (graceful shutdown on SIGINT/SIGTERM)
- [x] Detailed logging with configurable levels
- [x] Progress monitoring

## Installation

### Using Docker

Clone the repository and build the Docker image:

```bash
git clone https://github.com/Qovery/migration-db.git
cd migration-db
docker build -f Dockerfile . --tag migrationdb
```

### Using Docker for Migrations

```bash
# Basic migration
docker run -it --rm migrationdb \
    --source postgresql://user:pass@source:5432/dbname \
    --target postgresql://user:pass@target:5432/dbname

# Stream to local file
docker run -it --rm migrationdb \
    --source postgresql://user:pass@source:5432/dbname \
    --stdout > dump.sql

# Validate connections
docker run -it --rm migrationdb validate \
    --source postgresql://user:pass@source:5432/dbname \
    --target postgresql://user:pass@target:5432/dbname

# Using custom network for database access
docker run -it --rm --network=my-network migrationdb \
    --source postgresql://user:pass@source-db:5432/dbname \
    --target postgresql://user:pass@target-db:5432/dbname
```

## Usage

### Basic Migration

```bash
migrationdb --source postgresql://user:pass@source:5432/dbname \
           --target postgresql://user:pass@target:5432/dbname
```

### Stream to Stdout

```bash
migrationdb --source postgresql://user:pass@source:5432/dbname --stdout > dump.sql
```

### Stream and Compress

```bash
migrationdb --source postgresql://user:pass@source:5432/dbname --stdout | gzip > dump.sql.gz
```

### Skip Verification

```bash
migrationdb --source postgresql://user:pass@source:5432/dbname \
           --target postgresql://user:pass@target:5432/dbname \
           --skip-verify
```

## Connection String Formats

- PostgreSQL: `postgresql://user:pass@host:5432/dbname`
- MySQL: `mysql://user:pass@host:3306/dbname`
- MongoDB: `mongodb://user:pass@host:27017/dbname`

## Configuration Options

| Flag                  | Description                                  | Default |
|-----------------------|----------------------------------------------|---------|
| `--source`            | Source database connection string (required) | -       |
| `--target`            | Target database connection string            | -       |
| `--stdout`            | Stream to stdout instead of target database  | false   |
| `--log-level`         | Log level (debug, info, warn, error)         | info    |
| `--buffer-size`       | Buffer size in bytes for streaming           | 10MB    |
| `--timeout`           | Migration timeout duration                   | 24h     |
| `--skip-verify`       | Skip verification after migration            | false   |
| `--verify-chunk-size` | Chunk size in bytes for verification         | 10MB    |
| `--skip-tls-verify`   | Skip TLS certificate verification            | false   |

## Commands

### Migrate (Default)

The default command performs the migration:

```bash
migrationdb --source <source-conn> --target <target-conn>
```

### Validate

Validates the connection strings and tests connectivity:

```bash
migrationdb validate --source <source-conn> --target <target-conn>
```

### Version

Displays the version information:

```bash
migrationdb version
```

## Features in Detail

### Data Verification

After migration, MigrationDB automatically verifies the transferred data by:

1. Comparing content between source and target databases
2. Calculating and comparing checksums
3. Providing detailed verification reports

This can be skipped using the `--skip-verify` flag.

### Security

- Sensitive information in connection strings is automatically masked in logs
- TLS/SSL support for secure connections
- Optional TLS verification skip for development environments

### Performance

- Configurable buffer sizes for optimal performance
- Streaming-based transfers for memory efficiency
- Chunked verification for large datasets

## Why MigrationDB?

MigrationDB was developed at Qovery to address the need for reliable database migrations across cloud providers and regions. It offers:

- Simple, intuitive command-line interface
- Robust error handling and validation
- Built-in verification capabilities
- Memory-efficient streaming transfers
- Support for multiple database types

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the [MIT License](LICENSE.md) - see the LICENSE file for details.
