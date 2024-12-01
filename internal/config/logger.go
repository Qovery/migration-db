package config

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

type LoggerOptions struct {
	ForceStderr bool
}

func NewLogger(level string, opts LoggerOptions) *Logger {
	// Parse log level
	var zapLevel zapcore.Level
	if err := zapLevel.UnmarshalText([]byte(level)); err != nil {
		zapLevel = zapcore.InfoLevel
	}

	// Create encoder config
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	// Create core
	var core zapcore.Core
	if opts.ForceStderr {
		// Force all output to stderr when in stdout mode
		core = zapcore.NewCore(
			zapcore.NewConsoleEncoder(encoderConfig),
			zapcore.Lock(os.Stderr),
			zapLevel,
		)
	} else {
		// Normal configuration with both stdout and stderr
		core = zapcore.NewCore(
			zapcore.NewConsoleEncoder(encoderConfig),
			zapcore.NewMultiWriteSyncer(
				zapcore.Lock(os.Stdout),
				zapcore.Lock(os.Stderr),
			),
			zapLevel,
		)
	}

	// Create logger
	logger := zap.New(core)
	return &Logger{
		logger: logger.Sugar(),
	}
}

// Logger wraps zap.SugaredLogger
type Logger struct {
	logger *zap.SugaredLogger
}

func (l *Logger) Info(args ...interface{})                    { l.logger.Info(args...) }
func (l *Logger) Infof(template string, args ...interface{})  { l.logger.Infof(template, args...) }
func (l *Logger) Warn(args ...interface{})                    { l.logger.Warn(args...) }
func (l *Logger) Warnf(template string, args ...interface{})  { l.logger.Warnf(template, args...) }
func (l *Logger) Error(args ...interface{})                   { l.logger.Error(args...) }
func (l *Logger) Errorf(template string, args ...interface{}) { l.logger.Errorf(template, args...) }
