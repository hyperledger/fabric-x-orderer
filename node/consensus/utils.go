package consensus

import (
	"encoding/binary"

	arma_types "arma/common/types"
	"arma/core"
)

func toBeSignedBAF(baf core.BatchAttestationFragment) []byte {
	simpleBAF, ok := baf.(*arma_types.SimpleBatchAttestationFragment)
	if !ok {
		return nil
	}
	return simpleBAF.ToBeSigned()
}

func toBeSignedComplaint(c *core.Complaint) []byte {
	buff := make([]byte, 12)
	var pos int
	binary.BigEndian.PutUint16(buff, uint16(c.Shard))
	pos += 2
	binary.BigEndian.PutUint64(buff[pos:], c.Term)
	pos += 8
	binary.BigEndian.PutUint16(buff[pos:], uint16(c.Signer))

	return buff
}
