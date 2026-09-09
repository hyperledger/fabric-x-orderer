/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"math"
	"os"
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	flogging.ActivateSpec("blkstorage=debug")
	os.Exit(m.Run())
}

type testEnv struct {
	t        testing.TB
	provider *BlockStoreProvider
}

func newTestEnv(t testing.TB, conf *Conf) *testEnv {
	return newTestEnvWithMetricsProvider(t, conf, &disabled.Provider{})
}

func newTestEnvWithMetricsProvider(t testing.TB, conf *Conf, metricsProvider metrics.Provider) *testEnv {
	p, err := NewProvider(conf, metricsProvider)
	require.NoError(t, err)
	return &testEnv{t, p}
}

func (env *testEnv) Cleanup() {
	env.provider.Close()
}

type testBlockfileMgrWrapper struct {
	t            testing.TB
	blockfileMgr *blockfileMgr
}

func newTestBlockfileWrapper(env *testEnv, ledgerid string) *testBlockfileMgrWrapper {
	blkStore, err := env.provider.Open(ledgerid)
	require.NoError(env.t, err)
	return &testBlockfileMgrWrapper{env.t, blkStore.fileMgr}
}

func (w *testBlockfileMgrWrapper) addBlocks(blocks []*common.Block) {
	for _, blk := range blocks {
		err := w.blockfileMgr.addBlock(blk)
		require.NoError(w.t, err, "Error while adding block to blockfileMgr")
	}
}

func (w *testBlockfileMgrWrapper) testGetBlockByNumber(blocks []*common.Block) {
	for i := 0; i < len(blocks); i++ {
		b, err := w.blockfileMgr.retrieveBlockByNumber(blocks[0].Header.Number + uint64(i))
		require.NoError(w.t, err, "Error while retrieving [%d]th block from blockfileMgr", i)
		require.Equal(w.t, blocks[i], b)
	}
	// test getting the last block
	b, err := w.blockfileMgr.retrieveBlockByNumber(math.MaxUint64)
	iLastBlock := len(blocks) - 1
	require.NoError(w.t, err, "Error while retrieving last block from blockfileMgr")
	require.Equal(w.t, blocks[iLastBlock], b)
}

func (w *testBlockfileMgrWrapper) close() {
	w.blockfileMgr.close()
}
