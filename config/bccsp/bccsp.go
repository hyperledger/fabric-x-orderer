/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package bccsp

// BCCSP configures the blockchain crypto service providers.
type BCCSP struct {
	// Default specifies the preferred blockchain crypto service provider to use.
	// If the preferred provider is not available, the software based provider ("SW") will be used.
	// Valid providers are:
	// - SW: a software based crypto provider
	// - PKCS11: a CA hardware security module crypto provider.
	Default string `yaml:"Default,omitempty"`
	// SW configures the software based blockchain crypto provider.
	SW *SwOpts `yaml:"SW,omitempty"`
	// PKCS11 is the settings for the PKCS#11 crypto provider (i.e. when DEFAULT: PKCS11)
	PKCS11 *PKCS11 `yaml:"PKCS11,omitempty"`
}

type SwOpts struct {
	Security     int               `json:"security" yaml:"Security"`
	Hash         string            `json:"hash" yaml:"Hash"`
	FileKeystore *FileKeystoreOpts `json:"filekeystore,omitempty" yaml:"FileKeyStore,omitempty"`
}

type FileKeystoreOpts struct {
	KeyStorePath string `yaml:"KeyStore"`
}

type PKCS11 struct {
	Hash     string `yaml:"Hash,omitempty"`
	Security int    `yaml:"Security,omitempty"`
	Pin      string `yaml:"Pin,omitempty"`
	Label    string `yaml:"Label,omitempty"`
	Library  string `yaml:"Library,omitempty"`

	AltID  string         `yaml:"AltID,omitempty"`
	KeyIDs []KeyIDMapping `yaml:"KeyIDs,omitempty"`
}

type KeyIDMapping struct {
	SKI string `yaml:"SKI,omitempty"`
	ID  string `yaml:"ID,omitempty"`
}
