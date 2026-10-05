/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"sync"
	"testing"

	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	configMocks "github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

// TestVerifyCEConcurrentWithReconfig checks that verifyCE and VerificationSequence do not race with a
// dynamic reconfiguration, which replaces c.Config and c.SigVerifier under c.lock (configureConsensus)
// while requests are being verified from gRPC handlers and SmartBFT goroutines. The race is reported
// only under -race.
func TestVerifyCEConcurrentWithReconfig(t *testing.T) {
	sk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	signer := crypto.ECDSASigner(*sk)

	newConfig := func(seq uint64) (*nodeconfig.ConsenterNodeConfig, crypto.ECDSAVerifier) {
		configtxValidator := &policyMocks.FakeConfigtxValidator{}
		configtxValidator.SequenceReturns(seq)
		bundle := &configMocks.FakeConfigResources{}
		bundle.ConfigtxValidatorReturns(configtxValidator)
		verifier := crypto.ECDSAVerifier{
			types.NewBatcherIdentity(1, 1): signer.PublicKey,
		}
		return &nodeconfig.ConsenterNodeConfig{Bundle: bundle}, verifier
	}

	config0, verifier0 := newConfig(0)
	config1, verifier1 := newConfig(1)

	c := &Consensus{
		Logger:      testutil.CreateLogger(t, 1),
		Config:      config0,
		SigVerifier: verifier0,
	}

	// A BAF at config sequence 0 is valid under both configs: current under config0, and one behind
	// (accepted for revival) under config1.
	baf, err := batcher.CreateBAF(signer, 1, 1, make([]byte, 32), 1, 1, 0, 0, nil)
	require.NoError(t, err)
	req := (&state.ControlEvent{BAF: baf}).Bytes()

	const iterations = 100
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range iterations {
			c.lock.Lock()
			if i%2 == 0 {
				c.Config, c.SigVerifier = config1, verifier1
			} else {
				c.Config, c.SigVerifier = config0, verifier0
			}
			c.lock.Unlock()
		}
	}()

	for range iterations {
		_, _, err := c.verifyCE(req)
		require.NoError(t, err)
		require.LessOrEqual(t, c.VerificationSequence(), uint64(1))
	}
	wg.Wait()
}
