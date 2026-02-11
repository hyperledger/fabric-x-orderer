/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"os"

	"gopkg.in/yaml.v3"
)

func WriteToYAML(config interface{}, path string) error {
	c, err := yaml.Marshal(&config)
	if err != nil {
		return err
	}

	err = os.WriteFile(path, c, 0o644)
	if err != nil {
		return err
	}

	return nil
}

func ReadFromYAML(config any, path string) error {
	yamlFile, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(yamlFile, config)
	if err != nil {
		return err
	}
	return nil
}
