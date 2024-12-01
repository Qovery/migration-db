package migration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
)

type PostgresDumper struct {
	connString      string
	useCustomFormat bool
}

func NewPostgresDumper(connString string, useCustomFormat bool) *PostgresDumper {
	return &PostgresDumper{
		connString:      connString,
		useCustomFormat: useCustomFormat,
	}
}

func (d *PostgresDumper) GetType() DatabaseType {
	return Postgres
}

func (d *PostgresDumper) Dump(ctx context.Context, w io.Writer) error {
	var stderr bytes.Buffer

	args := []string{
		"--verbose",
		"--no-owner",      // Add this to avoid permission issues
		"--no-privileges", // Add this to avoid permission issues
	}

	if d.useCustomFormat {
		args = append(args, "--format=custom")
	} else {
		args = append(args, "--format=plain")
	}

	args = append(args, d.connString)

	cmd := exec.CommandContext(ctx, "pg_dump", args...)

	cmd.Stdout = w
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("pg_dump failed: %v, stderr: %s", err, stderr.String())
	}

	return nil
}

type PostgresRestorer struct {
	connString string
}

func NewPostgresRestorer(connString string) *PostgresRestorer {
	return &PostgresRestorer{connString: connString}
}

func (r *PostgresRestorer) GetType() DatabaseType {
	return Postgres
}

func (r *PostgresRestorer) Restore(ctx context.Context, reader io.Reader) error {
	var stderr bytes.Buffer

	cmd := exec.CommandContext(ctx, "pg_restore",
		"--verbose",
		"--no-owner",      // Add this to avoid permission issues
		"--no-privileges", // Add this to avoid permission issues
		"--clean",
		"--if-exists",
		"--no-comments",        // Optional: skip restoring comments
		"--no-security-labels", // Optional: skip security labels
		fmt.Sprintf("--dbname=%s", r.connString),
	)

	cmd.Stdin = reader
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("pg_restore failed: %v, stderr: %s", err, stderr.String())
	}

	return nil
}
