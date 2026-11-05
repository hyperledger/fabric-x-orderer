/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"errors"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
)

// ErrThrottled is the sentinel error carried by a Response when a request is
// rejected by the router's rate limiter. On the Broadcast API it maps to
// common.Status_SERVICE_UNAVAILABLE (there is no RESOURCE_EXHAUSTED in
// common.Status); on the Submit API it is surfaced via SubmitResponse.Error.
var ErrThrottled = errors.New("service unavailable: request throttled by router rate limiter")

// errContextDone is carried by a Response when the client gave up on a Submit before the answer arrived.
// It is not counted in router_requests_rejected: the router refused nothing.
var errContextDone = errors.New("context done before receiving response")

// badRequestError is an error for a request that is itself wrong. It is counted under code "400" in
// router_requests_rejected; every other error the router answers with is counted under "500".
type badRequestError struct {
	err error
}

func (e *badRequestError) Error() string {
	return e.err.Error()
}

func (e *badRequestError) Unwrap() error {
	return e.err
}

func badRequest(err error) error {
	return &badRequestError{err: err}
}

type TrackedRequest struct {
	request  *protos.Request // the request to be forward to the batcher
	feedback *clientFeedback // the feedback path of the submitting client
	reqID    []byte          // identifier used to disseminate requests across shards in the router
	trace    []byte          // used to trace the request in the router. If nil, the request is untraced, and a response is sent no later than after forwarding to the batcher.
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
	switch {
	case response.err == nil:
		br.Status = common.Status_SUCCESS
	case errors.Is(response.err, ErrThrottled):
		br.Status = common.Status_SERVICE_UNAVAILABLE
		br.Info = response.err.Error()
	default:
		br.Status = common.Status_INTERNAL_SERVER_ERROR
		br.Info = response.err.Error()
	}
	return br
}
