/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/operations"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
)

// CreateTrackedRequest creates a tracked request whose responses are queued on
// the given channel, to submit a request without a live client stream.
func CreateTrackedRequest(request *protos.Request, responses chan Response, reqID []byte, trace []byte) *TrackedRequest {
	logger := flogging.MustGetLogger("router")
	metrics := NewRouterMetrics(&config.RouterNodeConfig{Metrics: &operations.Metrics{Provider: "disabled"}}, logger)

	feedback := &clientFeedback{responses: responses, metrics: metrics, logger: logger}
	return &TrackedRequest{request: request, feedback: feedback, reqID: reqID, trace: trace}
}
