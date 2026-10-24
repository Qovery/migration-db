package migration

import (
	"context"
	"fmt"
	"io"
)

type DatabaseType string

const (
	Postgres DatabaseType = "postgres"
	MySQL    DatabaseType = "mysql"
	MongoDB  DatabaseType = "mongodb"
)

// Dumper defines the interface for database dump operations
type Dumper interface {
	Dump(ctx context.Context, w io.Writer) error
	GetType() DatabaseType
}

// Restorer defines the interface for database restore operations
type Restorer interface {
	Restore(ctx context.Context, r io.Reader) error
	GetType() DatabaseType
}

func CreateDumper(dbType string, connStr string, useStdOut bool, extraArgs []string) (Dumper, error) {
	switch DatabaseType(dbType) {
	case Postgres:
		// Use custom format when not in stdout mode -- better for large databases
		// Use plain format when in stdout mode -- better for readability
		useCustomFormat := !useStdOut
		return NewPostgresDumper(connStr, useCustomFormat, extraArgs), nil
	case MySQL:
		return NewMySQLDumper(connStr, extraArgs), nil
	case MongoDB:
		return NewMongoDBDumper(connStr, extraArgs), nil
	default:
		return nil, fmt.Errorf("unsupported source database type: %s", dbType)
	}
}

func CreateRestorer(dbType string, connStr string, extraArgs []string) (Restorer, error) {
	switch DatabaseType(dbType) {
	case Postgres:
		return NewPostgresRestorer(connStr, extraArgs), nil
	case MySQL:
		return NewMySQLRestorer(connStr, extraArgs), nil
	case MongoDB:
		return NewMongoDBRestorer(connStr, extraArgs), nil
	default:
		return nil, fmt.Errorf("unsupported target database type: %s", dbType)
	}
}
