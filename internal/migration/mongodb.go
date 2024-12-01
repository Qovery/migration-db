package migration

import (
	"context"
	"fmt"
	"io"
	"os/exec"
)

type MongoDBDumper struct {
	connString string
}

func NewMongoDBDumper(connString string) *MongoDBDumper {
	return &MongoDBDumper{connString: connString}
}

func (d *MongoDBDumper) GetType() DatabaseType {
	return MongoDB
}

func (d *MongoDBDumper) Dump(ctx context.Context, w io.Writer) error {
	cmd := exec.CommandContext(ctx, "mongodump",
		"--uri="+d.connString,
		"--archive",
	)
	cmd.Stdout = w
	cmd.Stderr = io.Discard

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mongodump failed: %w", err)
	}

	return nil
}

type MongoDBRestorer struct {
	connString string
}

func NewMongoDBRestorer(connString string) *MongoDBRestorer {
	return &MongoDBRestorer{connString: connString}
}

func (r *MongoDBRestorer) GetType() DatabaseType {
	return MongoDB
}

func (r *MongoDBRestorer) Restore(ctx context.Context, reader io.Reader) error {
	cmd := exec.CommandContext(ctx, "mongorestore",
		"--uri="+r.connString,
		"--archive",
	)
	cmd.Stdin = reader
	cmd.Stderr = io.Discard

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mongorestore failed: %w", err)
	}

	return nil
}
