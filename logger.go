// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package s3tar

import (
	"context"
	"fmt"
	"log"
	"os"
)

const (
	contextKeyLogger      = contextKey("logger")
	contextKeyLoggerLevel = contextKey("logger-level")
)

type logWriter struct {
}

func (writer logWriter) Write(bytes []byte) (int, error) {
	return fmt.Print(string(bytes))
}

func SetLogLevel(ctx context.Context, level int) context.Context {
	return context.WithValue(ctx, contextKeyLoggerLevel, level)
}

func SetupLogger(incoming context.Context) context.Context {
	logger := log.New(os.Stdout, "", 0)
	logger.SetOutput(new(logWriter))
	return context.WithValue(incoming, contextKeyLogger, logger)
}

func Debugf(ctx context.Context, format string, v ...interface{}) {
	logger, level := getValues(ctx)
	if level > 2 && level <= 3 {
		logger.Printf(format, v...)
	}
}

func Warnf(ctx context.Context, format string, v ...interface{}) {
	logger, level := getValues(ctx)
	if level > 1 && level <= 3 {
		logger.Printf(format, v...)
	}
}

// Errorf, always log regardless of log level, but don't stop the application
func Errorf(ctx context.Context, format string, v ...interface{}) {
	logger, _ := getValues(ctx)
	logger.Printf(format, v...)
}
func Fatalf(ctx context.Context, format string, v ...interface{}) {
	log.Fatalf(format, v...)
}

func Infof(ctx context.Context, format string, v ...interface{}) {
	logger, level := getValues(ctx)
	if level >= 1 {
		logger.Printf(format, v...)
	}
}

func getValues(ctx context.Context) (*log.Logger, int) {
	var logger *log.Logger
	var level int
	if _logger, ok := ctx.Value(contextKeyLogger).(*log.Logger); ok {
		logger = _logger
	} else {
		log.Printf("default logger")
		logger = log.Default()
	}
	if _level, ok := ctx.Value(contextKeyLoggerLevel).(int); ok {
		level = _level
	} else {
		level = 0
	}
	return logger, level
}
