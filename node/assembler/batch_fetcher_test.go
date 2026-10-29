/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"testing"
	"time"

	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	configMocks "github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
	"github.com/stretchr/testify/require"
)

// Scenario:
//  1. Start a stub batcher that holds one batch and serves only a request signed with the
//     certificate of the assembler.
//  2. Get the batch with a fetcher that signs with the identity of the assembler, and expect the
//     batch.
//  3. Get the batch with a fetcher that signs with another identity, and expect no batch while the
//     fetcher runs.
func TestBatchFetcherSignsTheRequestsItPullsWith(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	shardID := types.ShardID(1)
	partyID := types.PartyID(1)

	batchers, batcherInfos, cleanup := createStubBatchersAndInfos(t, 1, shardID, ca)
	t.Cleanup(cleanup)

	assemblerSigner, assemblerCert := signutil.NewSelfSignedSigner(t, "org")
	foreignSigner, _ := signutil.NewSelfSignedSigner(t, "org")
	batchers[0].AdmitOnly(t, assemblerCert)

	batch := types.NewSimpleBatch(shardID, partyID, 0, types.BatchedRequests{[]byte{1}}, 0, nil)
	batchers[0].SetNextBatch(batch)

	nodeConfig := fetcherConfig(t, ca, partyID, []config.ShardInfo{{ShardId: shardID, Batchers: batcherInfos}})
	logger := testutil.CreateLogger(t, int(partyID))

	t.Run("signed with the admitted identity", func(t *testing.T) {
		fetcher := assembler.NewBatchFetcher(nil, nodeConfig, assemblerSigner, logger)
		t.Cleanup(fetcher.Stop)

		fetched, err := fetcher.GetBatch(batch)
		require.NoError(t, err)
		require.True(t, types.BatchIDEqual(batch, fetched))
	})

	t.Run("signed with another identity", func(t *testing.T) {
		fetcher := assembler.NewBatchFetcher(nil, nodeConfig, foreignSigner, logger)

		fetched := make(chan types.Batch, 1)
		go func() {
			batch, _ := fetcher.GetBatch(batch)
			fetched <- batch
		}()

		require.Never(t, func() bool { return len(fetched) > 0 }, 2*time.Second, 100*time.Millisecond)

		fetcher.Stop()
		require.Nil(t, <-fetched)
	})
}

// Scenario:
//  1. Start a stub batcher that holds batches 0 to 4 of primary 1.
//  2. Replicate the shard from the start, and consume batches 0 and 1, leaving the rest unconsumed.
//  3. Restart the batcher, add batches 5 to 7, and consume the rest of primary 1.
//  4. Expect every batch from 2 to 7 exactly once and in order.
func TestBatchFetcherResumesAfterTheLastBatchOnReconnect(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	shardID := types.ShardID(1)
	partyID := types.PartyID(1)

	batchers, batcherInfos, cleanup := createStubBatchersAndInfos(t, 1, shardID, ca)
	t.Cleanup(cleanup)
	for seq := range types.BatchSequence(5) {
		batchers[0].SetNextBatch(types.NewSimpleBatch(shardID, partyID, seq, types.BatchedRequests{[]byte{byte(seq)}}, 0, nil))
	}

	nodeConfig := fetcherConfig(t, ca, partyID, []config.ShardInfo{{ShardId: shardID, Batchers: batcherInfos}})
	nodeConfig.ReplicationChannelSize = 100
	signer, _ := signutil.NewSelfSignedSigner(t, "org")
	fetcher := assembler.NewBatchFetcher(nil, nodeConfig, signer, testutil.CreateLogger(t, int(partyID)))
	t.Cleanup(fetcher.Stop)
	batches := fetcher.Replicate(shardID)

	require.Equal(t, types.BatchSequence(0), nextBatch(t, batches).Seq())
	require.Equal(t, types.BatchSequence(1), nextBatch(t, batches).Seq())

	batchers[0].Stop()
	batchers[0].Restart()
	for seq := types.BatchSequence(5); seq < 8; seq++ {
		batchers[0].SetNextBatch(types.NewSimpleBatch(shardID, partyID, seq, types.BatchedRequests{[]byte{byte(seq)}}, 0, nil))
	}

	for expected := types.BatchSequence(2); expected < 8; expected++ {
		require.Equal(t, expected, nextBatch(t, batches).Seq())
	}
	require.Never(t, func() bool { return len(batches) > 0 }, time.Second, 100*time.Millisecond)
}

func nextBatch(t *testing.T, batches <-chan types.Batch) types.Batch {
	select {
	case batch := <-batches:
		return batch
	case <-time.After(10 * time.Second):
		t.Fatal("no batch was replicated")
		return nil
	}
}

// fetcherConfig is the configuration of the assembler of a party that fetches batches from the shards.
func fetcherConfig(
	t *testing.T, ca tlsgen.CA, partyID types.PartyID, shards []config.ShardInfo,
) *config.AssemblerNodeConfig {
	keyPair, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)

	configtxValidator := &policyMocks.FakeConfigtxValidator{}
	configtxValidator.ChannelIDReturns("arma")
	bundle := &configMocks.FakeConfigResources{}
	bundle.ConfigtxValidatorReturns(configtxValidator)

	return &config.AssemblerNodeConfig{
		PartyId:            partyID,
		Shards:             shards,
		TLSPrivateKeyFile:  keyPair.Key,
		TLSCertificateFile: keyPair.Cert,
		Bundle:             bundle,
	}
}
