/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sampleconfig

import _ "embed"

// DefaultYaml contains the sample config.
//
//go:embed configtx.yaml
var DefaultYaml string

// DefaultCryptoConfig contains the default crypto config.
//
//go:embed config_crypto.yaml
var DefaultCryptoConfig string
