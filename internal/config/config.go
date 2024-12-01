package config

import (
	"fmt"
	"time"
)

type Config struct {
	SourceConn       string
	TargetConn       string
	SourceType       string
	TargetType       string
	StdoutMode       bool
	LogLevel         string
	BufferSize       int
	Timeout          time.Duration
	SkipVerification bool
	VerifyChunkSize  int
	SkipTLSVerify    bool
}

func (c *Config) Validate() error {
	if c.SourceType == "" || c.SourceConn == "" {
		return fmt.Errorf("source type and connection string are required")
	}

	if !c.StdoutMode && (c.TargetType == "" || c.TargetConn == "") {
		return fmt.Errorf("target type and connection string are required when not using stdout")
	}

	return nil
}
