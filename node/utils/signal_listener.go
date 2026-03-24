/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
)

type NodeStopper interface {
	Stop()
}

// TODO: unit test StopSignalListen
func StopSignalListen(stopChan chan struct{}, node NodeStopper, logger *flogging.FabricLogger, nodeAddr string) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM)

	go func() {
		defer signal.Stop(signalChan)

		for {
			select {
			case <-signalChan:
				logger.Infof("SIGTERM signal caught, the node listening on %s is about to shutdown:", nodeAddr)
				node.Stop()
				return
			case <-stopChan:
				logger.Infof("Exit StopSignalListen routine")
				return
			}
		}
	}()
}
