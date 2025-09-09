/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package monitoring

import (
	"context"
	"fmt"
	"net"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/types"
)

type Monitor struct {
	Provider *Provider
	logger   types.Logger
	endpoint Endpoint
	// stop is used to stop the monitoring service
	stop     context.CancelFunc
	listener net.Listener
}

func NewMonitor(endpoint Endpoint, prefix string) *Monitor {
	logger := flogging.MustGetLogger(fmt.Sprintf("%s.monitoring", prefix))
	return &Monitor{Provider: NewProvider(logger), endpoint: endpoint, logger: logger}
}

func (m *Monitor) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	m.stop = cancel

	var err error
	serverConfig := ServerConfig{endpoint: &m.endpoint, logger: m.logger}
	m.listener, err = serverConfig.Listener()
	if err != nil {
		m.logger.Panicf("%v", err)
	}
	m.endpoint.Port = serverConfig.endpoint.Port

	go func() {
		m.Provider.StartPrometheusServer(ctx, m.listener)
	}()
}

func (m *Monitor) Stop() {
	if m.stop != nil {
		m.stop()
	}
	if m.listener != nil {
		m.listener.Close()
	}
}

func (m *Monitor) Address() string {
	return fmt.Sprintf("http://%s/metrics", m.endpoint.Address())
}
