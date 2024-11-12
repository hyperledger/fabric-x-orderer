package testutil

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func CreateLogger(t *testing.T, i int) *zap.SugaredLogger {
	logConfig := zap.NewDevelopmentConfig()
	logConfig.Level.SetLevel(zapcore.InfoLevel)
	logger, _ := logConfig.Build()
	logger = logger.With(zap.String("t", t.Name())).With(zap.Int64("id", int64(i)))
	sugaredLogger := logger.Sugar()
	return sugaredLogger
}

func CreateBenchmarkLogger(b *testing.B, i int) *zap.SugaredLogger {
	logConfig := zap.NewDevelopmentConfig()
	logConfig.Level.SetLevel(zapcore.InfoLevel)
	logger, _ := logConfig.Build()
	logger = logger.With(zap.String("b", b.Name())).With(zap.Int64("id", int64(i)))
	sugaredLogger := logger.Sugar()
	return sugaredLogger
}
