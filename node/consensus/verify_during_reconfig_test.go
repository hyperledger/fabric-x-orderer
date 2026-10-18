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

	smartbft_types "github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/hyperledger/SmartBFT/smartbftprotos"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	configMocks "github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// heightStorage is a Storage that only reports its height.
type heightStorage struct {
	Storage
	height uint64
}

func (s heightStorage) Height() uint64 {
	return s.height
}

// TestVerifyCEConcurrentWithReconfig checks that verifyCE and VerificationSequence do not race with a
// dynamic reconfiguration, which replaces c.Config, c.SigVerifier and c.Storage under c.lock
// (configureConsensus) while requests are being verified from gRPC handlers and SmartBFT goroutines.
// The race is reported only under -race.
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
			types.NewAssemblerIdentity(1):  signer.PublicKey,
		}
		return &nodeconfig.ConsenterNodeConfig{Bundle: bundle}, verifier
	}

	config0, verifier0 := newConfig(0)
	config1, verifier1 := newConfig(1)

	storage0, storage1 := heightStorage{height: 1}, heightStorage{height: 2}

	c := &Consensus{
		Logger:      testutil.CreateLogger(t, 1),
		Config:      config0,
		SigVerifier: verifier0,
		Storage:     storage0,
	}

	// A BAF at config sequence 0 is valid under both configs: current under config0, and one behind
	// (accepted for revival) under config1.
	baf, err := batcher.CreateBAF(signer, 1, 1, make([]byte, 32), 1, 1, 0, 0, nil)
	require.NoError(t, err)
	bafReq := (&state.ControlEvent{BAF: baf}).Bytes()

	// An assembler decision report of the genesis decision is committed in both ledgers, and its config
	// sequence matches config0 only.
	report := &state.AssemblerDecisionReport{Party: 1, DecisionNum: 0, ConfigSeq: 0}
	report.Signature, err = signer.Sign(report.ToBeSigned())
	require.NoError(t, err)
	reportReq := (&state.ControlEvent{AssemblerReport: report}).Bytes()

	// The reconfigurations keep running until all the verifications are done, so that they interleave.
	const iterations = 100
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
			}
			c.lock.Lock()
			if i%2 == 0 {
				c.Config, c.SigVerifier, c.Storage = config1, verifier1, storage1
			} else {
				c.Config, c.SigVerifier, c.Storage = config0, verifier0, storage0
			}
			c.lock.Unlock()
		}
	}()

	for range iterations {
		_, _, err := c.verifyCE(bafReq)
		require.NoError(t, err)
		// Under config0 the report reaches the ledger height check, which reads the snapshot of c.Storage;
		// under config1 its config sequence mismatches.
		_, _, err = c.verifyCE(reportReq)
		if err != nil {
			require.ErrorContains(t, err, "mismatch config sequence")
		}
		require.LessOrEqual(t, c.VerificationSequence(), uint64(1))
	}
	close(done)
	wg.Wait()
}

// reconfiguringVerifier verifies a signature and then replaces c.Config under c.lock, as a dynamic
// reconfiguration (configureConsensus) would while VerifyProposal verifies the proposal's requests.
type reconfiguringVerifier struct {
	crypto.ECDSAVerifier
	c         *Consensus
	newConfig *nodeconfig.ConsenterNodeConfig
}

func (v *reconfiguringVerifier) VerifySignature(id types.NodeIdentity, msg, sig []byte) error {
	err := v.ECDSAVerifier.VerifySignature(id, msg, sig)
	v.c.lock.Lock()
	v.c.Config = v.newConfig
	v.c.lock.Unlock()
	return err
}

// TestVerifyProposalRejectsReconfigDuringRequestVerification checks that VerifyProposal rejects a proposal
// when a reconfiguration lands while its requests are verified outside the lock, instead of simulating the
// state transition with the new state against the old verification sequence.
func TestVerifyProposalRejectsReconfigDuringRequestVerification(t *testing.T) {
	sk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	signer := crypto.ECDSASigner(*sk)

	newConfig := func(seq uint64) *nodeconfig.ConsenterNodeConfig {
		configtxValidator := &policyMocks.FakeConfigtxValidator{}
		configtxValidator.SequenceReturns(seq)
		bundle := &configMocks.FakeConfigResources{}
		bundle.ConfigtxValidatorReturns(configtxValidator)
		return &nodeconfig.ConsenterNodeConfig{Bundle: bundle}
	}

	c := &Consensus{
		Logger: testutil.CreateLogger(t, 1),
		Config: newConfig(0),
	}
	c.SigVerifier = &reconfiguringVerifier{
		ECDSAVerifier: crypto.ECDSAVerifier{types.NewBatcherIdentity(1, 1): signer.PublicKey},
		c:             c,
		newConfig:     newConfig(1),
	}

	baf, err := batcher.CreateBAF(signer, 1, 1, make([]byte, 32), 1, 1, 0, 0, nil)
	require.NoError(t, err)
	requests := types.BatchedRequests{(&state.ControlEvent{BAF: baf}).Bytes()}
	hdr := &state.Header{Num: 1, State: &state.State{N: 1}}
	md, err := proto.Marshal(&smartbftprotos.ViewMetadata{LatestSequence: 1})
	require.NoError(t, err)

	_, err = c.VerifyProposal(smartbft_types.Proposal{
		Header:               hdr.Serialize(),
		Payload:              requests.Serialize(),
		Metadata:             md,
		VerificationSequence: 0,
	})
	require.EqualError(t, err, "verification sequence changed from 0 to 1 while verifying the proposal")
}
