/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testutil

import (
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func CreateLogger(t *testing.T, i int) *flogging.FabricLogger {
	logConfig := zap.NewDevelopmentConfig()
	logConfig.Level.SetLevel(zapcore.InfoLevel)
	logger, _ := logConfig.Build()
	logger = logger.With(zap.String("t", t.Name())).With(zap.Int64("id", int64(i)))
	fabricLogger := flogging.NewFabricLogger(logger)
	return fabricLogger
}

func CreateLoggerForModule(t *testing.T, name string, level zapcore.Level) *flogging.FabricLogger {
	logConfig := zap.NewDevelopmentConfig()
	logConfig.Level.SetLevel(level)
	logger, _ := logConfig.Build()
	logger = logger.With(zap.String("t", t.Name())).With(zap.String("m", name))
	fabricLogger := flogging.NewFabricLogger(logger)
	return fabricLogger
}

func CreateBenchmarkLogger(b *testing.B, i int) *flogging.FabricLogger {
	logConfig := zap.NewDevelopmentConfig()
	logConfig.Level.SetLevel(zapcore.InfoLevel)
	logger, _ := logConfig.Build()
	logger = logger.With(zap.String("b", b.Name())).With(zap.Int64("id", int64(i)))
	fabricLogger := flogging.NewFabricLogger(logger)
	return fabricLogger
}
