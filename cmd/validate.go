package cmd

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"net/url"
	"strings"
	"time"
)

var (
	skipTLSVerify bool
)

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate the connection strings and configurations",
	RunE:  runValidate,
}

func init() {
	validateCmd.Flags().BoolVar(&skipTLSVerify, "skip-tls-verify", false, "Skip TLS certificate verification when testing connections")
}

func runValidate(cmd *cobra.Command, args []string) error {
	// First validate the configuration
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("configuration validation failed: %v", err)
	}

	// Validate and get database types
	sourceType, err := ValidateConnections(cfg.SourceConn, cfg.TargetConn, cfg.StdoutMode)
	if err != nil {
		return fmt.Errorf("connection validation failed: %v", err)
	}

	// Test source connection
	fmt.Println("Validate: Testing source connection...")
	if err := TestConnection(sourceType, cfg.SourceConn, skipTLSVerify); err != nil {
		return fmt.Errorf("source connection test failed: %v", err)
	}
	fmt.Println("Source connection test successful!")

	// Test target connection if not in stdout mode
	if !cfg.StdoutMode {
		fmt.Println("\nValidate: Testing target connection...")
		if err := TestConnection(sourceType, cfg.TargetConn, skipTLSVerify); err != nil {
			return fmt.Errorf("target connection test failed: %v", err)
		}
		fmt.Println("Target connection test successful!")
	}

	// Print configuration summary
	fmt.Println("\nConfiguration Summary:")
	fmt.Println("Source configuration:")
	fmt.Printf("  Type: %s\n", sourceType)
	fmt.Printf("  Connection: %s\n", MaskConnectionString(cfg.SourceConn))

	if !cfg.StdoutMode {
		fmt.Println("\nTarget configuration:")
		fmt.Printf("  Type: %s\n", sourceType)
		fmt.Printf("  Connection: %s\n", MaskConnectionString(cfg.TargetConn))
	}

	return nil
}

// TestConnection attempts to establish a connection to the database
func TestConnection(dbType, connString string, skipTLSVerify bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch dbType {
	case "postgres", "mysql":
		finalConnString := connString
		if skipTLSVerify {
			// Add SSL/TLS disable parameters if skipTLSVerify is true
			if dbType == "postgres" && !strings.Contains(connString, "sslmode=") {
				if strings.Contains(connString, "?") {
					finalConnString += "&sslmode=disable"
				} else {
					finalConnString += "?sslmode=disable"
				}
			}

			if dbType == "mysql" && !strings.Contains(connString, "tls=") {
				if strings.Contains(connString, "?") {
					finalConnString += "&tls=false"
				} else {
					finalConnString += "?tls=false"
				}
			}
		}

		db, err := sql.Open(dbType, finalConnString)
		if err != nil {
			return fmt.Errorf("failed to create database connection: %v", err)
		}
		defer db.Close()

		// Test the connection
		err = db.PingContext(ctx)
		if err != nil {
			return fmt.Errorf("failed to connect to database: %v", err)
		}

	case "mongodb":
		clientOptions := options.Client().ApplyURI(connString)

		if skipTLSVerify {
			clientOptions.SetTLSConfig(nil).
				SetCompressors([]string{"none"}).
				SetDirect(true)
		}

		client, err := mongo.Connect(ctx, clientOptions)
		if err != nil {
			return fmt.Errorf("failed to create MongoDB client: %v", err)
		}
		defer client.Disconnect(ctx)

		// Test the connection
		err = client.Ping(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to connect to MongoDB: %v", err)
		}

	default:
		return fmt.Errorf("unsupported database type: %s", dbType)
	}

	return nil
}

// MaskConnectionString masks sensitive information in connection strings
func MaskConnectionString(conn string) string {
	if conn == "" {
		return ""
	}

	// Handle MongoDB connection strings
	if strings.HasPrefix(conn, "mongodb://") || strings.HasPrefix(conn, "mongodb+srv://") {
		// Replace password in MongoDB connection string
		parts := strings.Split(conn, "@")
		if len(parts) != 2 {
			return conn // Return original if we can't parse it
		}
		auth := strings.Split(parts[0], "://")
		if len(auth) != 2 {
			return conn
		}
		userPass := strings.Split(auth[1], ":")
		if len(userPass) != 2 {
			return conn
		}
		return fmt.Sprintf("%s://%s:****@%s", auth[0], userPass[0], parts[1])
	}

	// Handle SQL connection strings
	u, err := url.Parse(conn)
	if err != nil {
		return conn // Return original if we can't parse it
	}

	if u.User != nil {
		_, hasPassword := u.User.Password()
		if hasPassword {
			u.User = url.UserPassword(u.User.Username(), "****")
			// Convert to string and manually unescape the asterisks
			maskedStr := u.String()
			return strings.ReplaceAll(maskedStr, "%2A", "*")
		}
	}

	return u.String()
}

// ValidateConnections checks that both connections are valid and of the same type
func ValidateConnections(sourceConn, targetConn string, stdoutMode bool) (string, error) {
	sourceType, err := inferDatabaseType(sourceConn)
	if err != nil {
		return "", fmt.Errorf("invalid source database: %v", err)
	}

	if !stdoutMode {
		if targetConn == "" {
			return "", fmt.Errorf("target connection string is required when not using stdout mode")
		}

		targetType, err := inferDatabaseType(targetConn)
		if err != nil {
			return "", fmt.Errorf("invalid target database: %v", err)
		}

		if sourceType != targetType {
			return "", fmt.Errorf("source and target must be the same database type (got source: %s, target: %s)",
				sourceType, targetType)
		}
	}

	return sourceType, nil
}
