package utils

import (
	"encoding/pem"
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
)

func ReadPem(path string) ([]byte, error) {
	if path == "" {
		return nil, fmt.Errorf("failed reading pem file, path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed reading a pem file from %s, err: %v", path, err)
	}
	pbl, _ := pem.Decode(data)
	if pbl == nil {
		return nil, errors.Errorf("no pem content for file %s", path)
	}
	if pbl.Type != "CERTIFICATE" && pbl.Type != "PRIVATE KEY" {
		return nil, errors.Errorf("unexpected pem type, got a %s", strings.ToLower(pbl.Type))
	}
	certRaw := pbl.Bytes

	return certRaw, nil
}
