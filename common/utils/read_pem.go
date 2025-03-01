package utils

import (
	"fmt"
	"os"
)

func ReadPem(path string) ([]byte, error) {
	if path == "" {
		return nil, fmt.Errorf("failed reading pem file, path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed reading a pem file from %s, err: %v", path, err)
	}

	return data, nil
}
