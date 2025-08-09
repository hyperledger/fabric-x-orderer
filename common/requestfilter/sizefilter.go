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
	maxSizeBytes uint64
}

func NewMaxSizeFilter(config FilterConfig) *MaxSizeFilter {
	maxSize, err := config.GetMaxSizeBytes()
	if err != nil {
		return nil
	}
	return &MaxSizeFilter{maxSizeBytes: maxSize}
}

// Verify checks that the size of the request does not exceeds the maximal size in bytes.
func (ms *MaxSizeFilter) Verify(request *comm.Request) error {
	requestSize := uint64(len(request.Payload) + len(request.Signature))
	if requestSize > ms.maxSizeBytes {
		return fmt.Errorf("the request's size exceeds the maximum size: actual = %d, limit = %d", requestSize, ms.maxSizeBytes)
	}
	return nil
}

func (ms *MaxSizeFilter) Update(config FilterConfig) error {
	maxSize, err := config.GetMaxSizeBytes()
	if err != nil {
		return err
	}
	ms.maxSizeBytes = maxSize
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
