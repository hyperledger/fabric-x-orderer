/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"time"

	"github.ibm.com/decentralized-trust-research/arma/node/comm"
	"github.ibm.com/decentralized-trust-research/arma/node/config"
)

func clientConfig(TLSPrivateKeyFile config.RawBytes, TLSCertificateFile config.RawBytes, TlsCACert []config.RawBytes) comm.ClientConfig {
	var tlsCAs [][]byte
	for _, cert := range TlsCACert {
		tlsCAs = append(tlsCAs, cert)
	}

	cc := comm.ClientConfig{
		AsyncConnect: true,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               TLSPrivateKeyFile,
			Certificate:       TLSCertificateFile,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     tlsCAs,
		},
		DialTimeout: time.Second * 5,
	}
	return cc
}
