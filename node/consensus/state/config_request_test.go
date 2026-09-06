/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state_test

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	consensus_state "github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/stretchr/testify/require"
)

// configRequestWithSequence builds a ConfigRequest whose envelope carries a config
// envelope with the given sequence, mirroring how a real config transaction is encoded.
func configRequestWithSequence(seq uint64) consensus_state.ConfigRequest {
	configEnv := &common.ConfigEnvelope{
		Config: &common.Config{
			Sequence:     seq,
			ChannelGroup: &common.ConfigGroup{},
		},
	}
	payload := &common.Payload{
		Data: protoutil.MarshalOrPanic(configEnv),
	}
	return consensus_state.ConfigRequest{
		Envelope: &common.Envelope{
			Payload: protoutil.MarshalOrPanic(payload),
		},
	}
}

// configRequestWithSequenceAndID builds a ConfigRequest whose config envelope carries the given
// sequence and a LastUpdate (CONFIG_UPDATE) envelope whose channel header carries the given tx id,
// mirroring how a real config transaction is encoded.
func configRequestWithSequenceAndID(seq uint64, txID string) consensus_state.ConfigRequest {
	channelHeader := &common.ChannelHeader{
		Type: int32(common.HeaderType_CONFIG_UPDATE),
		TxId: txID,
	}
	lastUpdatePayload := &common.Payload{
		Header: &common.Header{
			ChannelHeader: protoutil.MarshalOrPanic(channelHeader),
		},
	}
	configEnv := &common.ConfigEnvelope{
		Config: &common.Config{
			Sequence:     seq,
			ChannelGroup: &common.ConfigGroup{},
		},
		LastUpdate: &common.Envelope{
			Payload: protoutil.MarshalOrPanic(lastUpdatePayload),
		},
	}
	payload := &common.Payload{
		Data: protoutil.MarshalOrPanic(configEnv),
	}
	return consensus_state.ConfigRequest{
		Envelope: &common.Envelope{
			Payload: protoutil.MarshalOrPanic(payload),
		},
	}
}

func TestConfigRequestSerialization(t *testing.T) {
	cr := consensus_state.ConfigRequest{
		Envelope: &common.Envelope{
			Payload:   []byte("config-payload"),
			Signature: []byte("config-signature"),
		},
	}

	var cr2 consensus_state.ConfigRequest

	err := cr2.FromBytes(cr.Bytes())
	require.NoError(t, err)

	require.Equal(t, cr.Envelope.Payload, cr2.Envelope.Payload)
	require.Equal(t, cr.Envelope.Signature, cr2.Envelope.Signature)
}

func TestConfigRequestFromBytesInvalid(t *testing.T) {
	var cr consensus_state.ConfigRequest

	// A malformed byte slice is not a valid proto-encoded config request.
	err := cr.FromBytes([]byte{0xff, 0xff, 0xff})
	require.Error(t, err)
}

func TestConfigRequestConfigSequence(t *testing.T) {
	// The sequence encoded in the config envelope is returned verbatim.
	cr := configRequestWithSequence(42)
	seq, err := cr.ConfigSequence()
	require.NoError(t, err)
	require.Equal(t, types.ConfigSequence(42), seq)

	// A config request survives a Bytes/FromBytes round trip and still yields
	// the same sequence.
	var cr2 consensus_state.ConfigRequest
	require.NoError(t, cr2.FromBytes(cr.Bytes()))
	seq2, err := cr2.ConfigSequence()
	require.NoError(t, err)
	require.Equal(t, types.ConfigSequence(42), seq2)
}

func TestConfigRequestConfigSequenceErrors(t *testing.T) {
	// Payload that is not a valid Payload proto.
	crBadPayload := consensus_state.ConfigRequest{
		Envelope: &common.Envelope{Payload: []byte{0xff, 0xff, 0xff}},
	}
	_, err := crBadPayload.ConfigSequence()
	require.Error(t, err)

	// An empty payload unmarshals to an empty Payload, whose Data is not a valid
	// config envelope, so the config is nil and ConfigSequence must error.
	crEmpty := consensus_state.ConfigRequest{
		Envelope: &common.Envelope{Payload: []byte{}},
	}
	_, err = crEmpty.ConfigSequence()
	require.Error(t, err)
}

func TestConfigRequestID(t *testing.T) {
	// The tx id encoded in the channel header of the LastUpdate envelope is returned verbatim.
	cr := configRequestWithSequenceAndID(42, "tx-123")
	id, err := cr.ID()
	require.NoError(t, err)
	require.Equal(t, "tx-123", id)

	// A config request survives a Bytes/FromBytes round trip and still yields the same id.
	var cr2 consensus_state.ConfigRequest
	require.NoError(t, cr2.FromBytes(cr.Bytes()))
	id2, err := cr2.ID()
	require.NoError(t, err)
	require.Equal(t, "tx-123", id2)
}

func TestConfigRequestIDErrors(t *testing.T) {
	// Payload that is not a valid Payload proto.
	crBadPayload := consensus_state.ConfigRequest{
		Envelope: &common.Envelope{Payload: []byte{0xff, 0xff, 0xff}},
	}
	_, err := crBadPayload.ID()
	require.Error(t, err)

	// A config envelope without a LastUpdate has no id to extract.
	crNoLastUpdate := configRequestWithSequence(7)
	_, err = crNoLastUpdate.ID()
	require.Error(t, err)
}

func TestConfigRequestString(t *testing.T) {
	// Valid config request: both the tx id and the sequence appear in the description.
	cr := configRequestWithSequenceAndID(7, "tx-abc")
	require.Contains(t, cr.String(), "tx-abc")
	require.Contains(t, cr.String(), "config sequence 7")

	// A config request whose envelope cannot be parsed reports the error rather
	// than a sequence.
	crBad := consensus_state.ConfigRequest{
		Envelope: &common.Envelope{Payload: []byte{0xff, 0xff, 0xff}},
	}
	require.Contains(t, crBad.String(), "error")

	// A valid config envelope without a LastUpdate still reports the sequence,
	// along with the error extracting the id.
	crNoID := configRequestWithSequence(7)
	require.Contains(t, crNoID.String(), "config sequence 7")
	require.Contains(t, crNoID.String(), "error extracting id")
}
