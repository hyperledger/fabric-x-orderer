/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
)

type TrackedRequest struct {
	request   *protos.Request // the request to be forward to the batcher
	responses chan Response   // the feedback channel where the response will be sent to
	reqID     []byte          // identifier used to disseminate requests across shards in the router
	trace     []byte          // used to trace the request in the router. If nil, the request is untraced, and a response is sent no later than after forwarding to the batcher.
}

func CreateTrackedRequest(request *protos.Request, responses chan Response, reqID []byte, trace []byte) *TrackedRequest {
	return &TrackedRequest{request: request, responses: responses, reqID: reqID, trace: trace}
}

type Response struct {
	err   error
	reqID []byte
	*protos.SubmitResponse
}

func (resp *Response) GetResponseError() error {
	return resp.err
}

func responseToSubmitResponse(response *Response) *protos.SubmitResponse {
	resp := &protos.SubmitResponse{
		ReqID: response.reqID,
	}
	if response.SubmitResponse != nil {
		resp = response.SubmitResponse
	} else { // It's an error
		resp.Error = response.err.Error()
	}
	return resp
}

func responseToBroadcastResponse(response *Response) *orderer.BroadcastResponse {
	br := &orderer.BroadcastResponse{}
	if response.err != nil {
		br.Status = common.Status_INTERNAL_SERVER_ERROR
		br.Info = response.err.Error()
	} else {
		br.Status = common.Status_SUCCESS
	}
	return br
}
