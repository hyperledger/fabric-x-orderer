/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter

import (
	"fmt"

	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
)

type MaxSizeFilter struct {
	requestMaxBytes uint64
}

func NewMaxSizeFilter(config FilterConfig) *MaxSizeFilter {
	return &MaxSizeFilter{requestMaxBytes: config.RequestMaxBytes()}
}

// Verify checks that the size of the request does not exceeds the maximal size in bytes.
func (ms *MaxSizeFilter) Verify(request *comm.Request) error {
	requestSize := uint64(len(request.Payload) + len(request.Signature))
	if requestSize > ms.requestMaxBytes {
		return fmt.Errorf("the request's size exceeds the maximum size: actual = %d, limit = %d", requestSize, ms.requestMaxBytes)
	}
	return nil
}

func (ms *MaxSizeFilter) Update(config FilterConfig) error {
	ms.requestMaxBytes = config.RequestMaxBytes()
	return nil
}

// PayloadNotEmptyRule - checks that the payload in the request is not nil.
type PayloadNotEmptyRule struct{}

func (r PayloadNotEmptyRule) Verify(request *comm.Request) error {
	if request.Payload == nil {
		return fmt.Errorf("empty payload field")
	}
	return nil
}

func (r PayloadNotEmptyRule) Update(config FilterConfig) error {
	return nil
}
