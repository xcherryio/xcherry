package config

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type (
	// Logger contains the config items for logger
	Logger struct {
		// Stdout is true then the output needs to goto standard out
		// By default this is false and output will go to standard error
		Stdout bool `yaml:"stdout"`
		// Level is the desired log level
		Level string `yaml:"level"`
		// OutputFile is the path to the log output file
		// Stdout must be false, otherwise Stdout will take precedence
		OutputFile string `yaml:"outputFile"`
		// LevelKey is the desired log level, defaults to "level"
		LevelKey string `yaml:"levelKey"`
		// Encoding decides the format, supports "console" and "json".
		// "json" will print the log in JSON format(better for machine), while "console" will print in plain-text format(more human friendly)
		// Default is "json"
		Encoding string `yaml:"encoding"`
	}
)

// NewZapLogger builds and returns a new
// Zap logger for this logging configuration
func (cfg *Logger) NewZapLogger() (*zap.Logger, error) {
	levelKey := cfg.LevelKey
	if levelKey == "" {
		levelKey = "level"
	}

	encodeConfig := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       levelKey,
		NameKey:        "logger",
		CallerKey:      "",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   nil,
	}

	outputPath := "stderr"
	if len(cfg.OutputFile) > 0 {
		outputPath = cfg.OutputFile
		if cfg.Stdout {
			outputPath = "stdout"
		}
	}

	encoding := "json"
	if cfg.Encoding != "" {
		if cfg.Encoding == "json" || cfg.Encoding == "console" {
			encoding = cfg.Encoding
		} else {
			return nil, fmt.Errorf("invalid encoding for log, only supporting json or console")
		}
	}

	config := zap.Config{
		Level:            zap.NewAtomicLevelAt(parseZapLevel(cfg.Level)),
		Development:      false,
		Sampling:         nil, // consider exposing this to config for our external customer
		Encoding:         encoding,
		EncoderConfig:    encodeConfig,
		OutputPaths:      []string{outputPath},
		ErrorOutputPaths: []string{outputPath},
	}
	return config.Build()
}

func parseZapLevel(level string) zapcore.Level {
	switch strings.ToLower(level) {
	case "debug":
		return zap.DebugLevel
	case "info":
		return zap.InfoLevel
	case "warn":
		return zap.WarnLevel
	case "error":
		return zap.ErrorLevel
	case "fatal":
		return zap.FatalLevel
	default:
		return zap.InfoLevel
	}
}
