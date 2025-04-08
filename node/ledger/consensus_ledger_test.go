package ledger_test

import (
	"testing"

	"github.ibm.com/decentralized-trust-research/arma/node/consensus/state"
	"github.ibm.com/decentralized-trust-research/arma/node/ledger"
	"github.ibm.com/decentralized-trust-research/arma/node/ledger/mocks"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConsensusLedger(t *testing.T) {
	dir := t.TempDir()
	l, err := ledger.NewConsensusLedger(dir)
	require.NoError(t, err)
	require.NotNil(t, l)
	require.Zero(t, l.Height())
	_, err = l.RetrieveBlockByNumber(0)
	require.Error(t, err)

	h1 := &state.Header{Num: 0}
	bytes := state.DecisionToBytes(smartbft_types.Proposal{
		Header: h1.Serialize(),
	}, make([]smartbft_types.Signature, 1))
	require.NotNil(t, bytes)
	l.Append(bytes)
	require.Equal(t, uint64(1), l.Height())
	_, err = l.RetrieveBlockByNumber(0)
	require.NoError(t, err)

	listener := &mocks.FakeAppendListener{}
	l.RegisterAppendListener(listener)

	h2 := &state.Header{Num: 1}
	bytes = state.DecisionToBytes(smartbft_types.Proposal{
		Header: h2.Serialize(),
	}, make([]smartbft_types.Signature, 1))
	require.NotNil(t, bytes)
	l.Append(bytes)
	require.Equal(t, uint64(2), l.Height())
	_, err = l.RetrieveBlockByNumber(1)
	require.NoError(t, err)

	require.Equal(t, 1, listener.OnAppendCallCount())

	l.Close()
	l.Close() // make sure there is no issue closing again

	l, err = ledger.NewConsensusLedger(dir)
	require.NoError(t, err)
	require.NotNil(t, l)
	require.Equal(t, uint64(2), l.Height())
	_, err = l.RetrieveBlockByNumber(0)
	require.NoError(t, err)
	_, err = l.RetrieveBlockByNumber(1)
	require.NoError(t, err)

	require.Equal(t, 1, listener.OnAppendCallCount())

	listener = &mocks.FakeAppendListener{}
	l.RegisterAppendListener(listener)

	h3 := &state.Header{Num: 2}
	bytes = state.DecisionToBytes(smartbft_types.Proposal{
		Header: h3.Serialize(),
	}, make([]smartbft_types.Signature, 1))
	require.NotNil(t, bytes)
	l.Append(bytes)
	require.Equal(t, uint64(3), l.Height())
	_, err = l.RetrieveBlockByNumber(2)
	require.NoError(t, err)

	require.Equal(t, 1, listener.OnAppendCallCount())

	require.PanicsWithError(t, "block number should have been 3 but was 2", func() { l.Append(bytes) })
	require.Equal(t, uint64(3), l.Height())
	_, err = l.RetrieveBlockByNumber(2)
	require.NoError(t, err)
	_, err = l.RetrieveBlockByNumber(1)
	require.NoError(t, err)
	_, err = l.RetrieveBlockByNumber(0)
	require.NoError(t, err)
}
