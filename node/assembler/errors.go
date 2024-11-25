package assembler

import (
	"errors"
)

var (
	ErrBatchAlreadyExists = errors.New("batch already exists")
	ErrBatchDoesNotExist  = errors.New("batch does not exist")
)
