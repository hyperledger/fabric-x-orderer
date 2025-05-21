/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"errors"
)

var (
	ErrBatchAlreadyExists = errors.New("batch already exists")
	ErrBatchDoesNotExist  = errors.New("batch does not exist")
	ErrBatchTooLarge      = errors.New("batch too large")
)
