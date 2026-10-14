/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tlsutil

import (
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// NewMutualTLSServer creates a gRPC server at addr that presents keyPair, and accepts only a client
// certificate that ca issued.
func NewMutualTLSServer(addr string, ca tlsgen.CA, keyPair *tlsgen.CertKeyPair) (*comm.GRPCServer, error) {
	return comm.NewGRPCServer(addr, comm.ServerConfig{
		SecOpts: comm.SecureOptions{
			ClientRootCAs:     [][]byte{ca.CertBytes()},
			Key:               keyPair.Key,
			Certificate:       keyPair.Cert,
			RequireClientCert: true,
			UseTLS:            true,
		},
	})
}

// Client holds the material a test client needs for a mutual TLS connection and its request binding.
type Client struct {
	ca      tlsgen.CA
	keyPair *tlsgen.CertKeyPair
	// TLSCertHash is the hash of the client TLS certificate, which binds a request to the connection.
	TLSCertHash []byte
}

// NewClient creates a client with a TLS certificate that ca issued.
func NewClient(t *testing.T, ca tlsgen.CA) *Client {
	keyPair, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)

	tlsCertHash, err := protoutil.HashTLSCertificate(keyPair.Cert)
	require.NoError(t, err)

	return &Client{ca: ca, keyPair: keyPair, TLSCertHash: tlsCertHash}
}

// Dial opens a mutual TLS connection to endpoint.
func (c *Client) Dial(endpoint string) (*grpc.ClientConn, error) {
	clientConfig := comm.ClientConfig{
		SecOpts: comm.SecureOptions{
			Key:               c.keyPair.Key,
			Certificate:       c.keyPair.Cert,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     [][]byte{c.ca.CertBytes()},
		},
		DialTimeout: 10 * time.Second,
	}

	return clientConfig.Dial(endpoint)
}
