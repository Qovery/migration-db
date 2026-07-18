package migration

import (
	"context"
	"fmt"
	"io"
	"os/exec"
)

type MongoDBDumper struct {
	connString string
	extraArgs  []string
}

func NewMongoDBDumper(connString string, extraArgs []string) *MongoDBDumper {
	return &MongoDBDumper{connString: connString, extraArgs: extraArgs}
}

func (d *MongoDBDumper) GetType() DatabaseType {
	return MongoDB
}

func (d *MongoDBDumper) Dump(ctx context.Context, w io.Writer) error {
	args := []string{
		"--uri=" + d.connString,
		"--archive",
	}
	if len(d.extraArgs) > 0 {
		args = append(args, d.extraArgs...)
	}
	cmd := exec.CommandContext(ctx, "mongodump", args...)
	cmd.Stdout = w
	cmd.Stderr = io.Discard

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mongodump failed: %w", err)
	}

	return nil
}

type MongoDBRestorer struct {
	connString string
	extraArgs  []string
}

func NewMongoDBRestorer(connString string, extraArgs []string) *MongoDBRestorer {
	return &MongoDBRestorer{connString: connString, extraArgs: extraArgs}
}

func (r *MongoDBRestorer) GetType() DatabaseType {
	return MongoDB
}

func (r *MongoDBRestorer) Restore(ctx context.Context, reader io.Reader) error {
	args := []string{
		"--uri=" + r.connString,
		"--archive",
	}
	if len(r.extraArgs) > 0 {
		args = append(args, r.extraArgs...)
	}
	cmd := exec.CommandContext(ctx, "mongorestore", args...)
	cmd.Stdin = reader
	cmd.Stderr = io.Discard

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mongorestore failed: %w", err)
	}

	return nil
}
