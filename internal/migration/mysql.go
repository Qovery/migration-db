package migration

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
)

type MySQLDumper struct {
	connString string
	extraArgs  []string
}

func NewMySQLDumper(connString string, extraArgs []string) *MySQLDumper {
	return &MySQLDumper{connString: connString, extraArgs: extraArgs}
}

func (d *MySQLDumper) GetType() DatabaseType {
	return MySQL
}

func (d *MySQLDumper) Dump(ctx context.Context, w io.Writer) error {
	args := []string{
		"--defaults-extra-file=" + createMySQLConfigFile(d.connString),
		"--single-transaction",
		"--quick",
		"--compress",
	}
	if len(d.extraArgs) > 0 {
		args = append(args, d.extraArgs...)
	}
	cmd := exec.CommandContext(ctx, "mysqldump", args...)
	cmd.Stdout = w
	cmd.Stderr = io.Discard

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mysqldump failed: %w", err)
	}

	return nil
}

type MySQLRestorer struct {
	connString string
	extraArgs  []string
}

func NewMySQLRestorer(connString string, extraArgs []string) *MySQLRestorer {
	return &MySQLRestorer{connString: connString, extraArgs: extraArgs}
}

func (r *MySQLRestorer) GetType() DatabaseType {
	return MySQL
}

func (r *MySQLRestorer) Restore(ctx context.Context, reader io.Reader) error {
	args := []string{
		"--defaults-extra-file=" + createMySQLConfigFile(r.connString),
	}
	if len(r.extraArgs) > 0 {
		args = append(args, r.extraArgs...)
	}
	cmd := exec.CommandContext(ctx, "mysql", args...)
	cmd.Stdin = reader
	cmd.Stderr = io.Discard

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mysql restore failed: %w", err)
	}

	return nil
}

// Helper function to create MySQL config file from connection string
func createMySQLConfigFile(connString string) string {
	// Parse MySQL connection string
	// Expected format: user:pass@tcp(host:port)/dbname

	// Create a temporary file
	tempFile, err := os.CreateTemp("", "mysql-config-*.cnf")
	if err != nil {
		log.Printf("Failed to create temp file: %v", err)
		return ""
	}

	// Extract credentials and connection info from connection string
	user := ""
	password := ""
	host := ""
	port := "3306" // default MySQL port

	// Basic parsing of MySQL connection string
	if parts := strings.Split(connString, "@"); len(parts) == 2 {
		// Extract credentials
		credentials := strings.Split(parts[0], ":")
		if len(credentials) == 2 {
			user = credentials[0]
			password = credentials[1]
		}

		// Extract host and port
		hostPart := parts[1]
		if strings.HasPrefix(hostPart, "tcp(") {
			hostPart = strings.TrimPrefix(hostPart, "tcp(")
			hostPart = strings.Split(hostPart, ")/")[0]
			hostDetails := strings.Split(hostPart, ":")
			if len(hostDetails) == 2 {
				host = hostDetails[0]
				port = hostDetails[1]
			} else {
				host = hostPart
			}
		}
	}

	// Write MySQL configuration
	configContent := fmt.Sprintf(`[client]
user=%s
password=%s
host=%s
port=%s
`, user, password, host, port)

	if err := os.WriteFile(tempFile.Name(), []byte(configContent), 0600); err != nil {
		log.Printf("Failed to write config file: %v", err)
		return ""
	}

	return tempFile.Name()
}
