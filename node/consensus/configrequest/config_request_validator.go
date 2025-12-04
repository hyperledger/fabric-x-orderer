/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configrequest

import "github.com/hyperledger/fabric-protos-go-apiv2/common"

type ConfigRequestValidator interface {
	ValidateConfigRequest(envelope *common.Envelope) error
}

// TODO: implement validate config
type DefaultValidateConfigRequest struct{}

//go:generate counterfeiter -o mocks/config_request_validator.go . ConfigRequestValidator
func (vc *DefaultValidateConfigRequest) ValidateConfigRequest(envelope *common.Envelope) error {
	return nil
}
