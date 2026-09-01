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
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestMain(m *testing.M) {
	flogging.ActivateSpec("blkstorage=debug")
	os.Exit(m.Run())
}

type testEnv struct {
	t        testing.TB
	provider *BlockStoreProvider
}

var attrsToIndex = []IndexableAttr{
	IndexableAttrBlockHash,
	IndexableAttrBlockNum,
	IndexableAttrTxID,
	IndexableAttrBlockNumTranNum,
}

func newTestEnv(t testing.TB, conf *Conf) *testEnv {
	return newTestEnvWithMetricsProvider(t, conf, &disabled.Provider{})
}

func newTestEnvWithMetricsProvider(t testing.TB, conf *Conf, metricsProvider metrics.Provider) *testEnv {
	return newTestEnvSelectiveIndexing(t, conf, attrsToIndex, metricsProvider)
}

func newTestEnvSelectiveIndexing(t testing.TB, conf *Conf, attrsToIndex []IndexableAttr, metricsProvider metrics.Provider) *testEnv {
	indexConfig := &IndexConfig{AttrsToIndex: attrsToIndex}
	p, err := NewProvider(conf, indexConfig, metricsProvider)
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

func (w *testBlockfileMgrWrapper) testGetBlockByHash(blocks []*common.Block) {
	for i, block := range blocks {
		hash := protoutil.BlockHeaderHash(block.Header)
		b, err := w.blockfileMgr.retrieveBlockByHash(hash)
		require.NoError(w.t, err, "Error while retrieving [%d]th block from blockfileMgr", i)
		require.Equal(w.t, block, b)
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

func (w *testBlockfileMgrWrapper) testGetMultipleDataByTxID(
	txID string,
	expectedData []*expectedBlkTxValidationCode,
) {
	rangescan := constructTxIDRangeScan(txID)
	itr, err := w.blockfileMgr.db.GetIterator(rangescan.startKey, rangescan.stopKey)
	require := require.New(w.t)
	require.NoError(err)
	defer itr.Release()

	fetchedData := []*expectedBlkTxValidationCode{}
	for itr.Next() {
		v := &TxIDIndexValue{}
		require.NoError(proto.Unmarshal(itr.Value(), v))

		blkFLP := &fileLocPointer{}
		require.NoError(blkFLP.unmarshal(v.BlkLocation))
		blk, err := w.blockfileMgr.fetchBlock(blkFLP)
		require.NoError(err)

		txFLP := &fileLocPointer{}
		require.NoError(txFLP.unmarshal(v.TxLocation))
		txEnv, err := w.blockfileMgr.fetchTransactionEnvelope(txFLP)
		require.NoError(err)

		fetchedData = append(fetchedData, &expectedBlkTxValidationCode{
			blk:            blk,
			txEnv:          txEnv,
			validationCode: peer.TxValidationCode(v.TxValidationCode),
		})
	}
	require.Equal(expectedData, fetchedData)
}

func (w *testBlockfileMgrWrapper) close() {
	w.blockfileMgr.close()
}

type expectedBlkTxValidationCode struct {
	blk            *common.Block
	txEnv          *common.Envelope
	validationCode peer.TxValidationCode
}
