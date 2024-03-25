package arma

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type Router struct {
	Logger         Logger
	RequestToShard func([]byte) (uint32, uint16)
	Forward        func(shard uint16, request []byte) (BackendError, error)
}

type BackendError error

func (r *Router) Submit(request []byte) error {
	reqID, shardID := r.RequestToShard(request)
	r.Logger.Debugf("Forwarding request %d to shard %d", reqID, shardID)
	backendErr, err := r.Forward(shardID, request)
	if err != nil {
		r.Logger.Warnf("Failed forwarding request %d to shard %d: %v", reqID, shardID, err)
		return err
	}

	if backendErr != nil {
		bckErrStr := backendErr.Error()
		if bckErrStr != "" {
			r.Logger.Warnf("Backend of shard %d could not enqueue request: %v", shardID, bckErrStr)
			return fmt.Errorf("%s", backendErr)
		}
	}

	return nil
}

func CRC32RequestToShard(shardCount uint16) func([]byte) (uint32, uint16) {
	return func(request []byte) (uint32, uint16) {
		reqID := crc32.Checksum(request, crc32.IEEETable)
		return reqID, uint16(reqID) % shardCount
	}
}

func SumBasedRequestToShard(shardCount uint16) func([]byte) (uint32, uint16) {
	return func(request []byte) (uint32, uint16) {
		var i int
		var requestID uint32
		var shardID uint16

		for i = 0; i+4 < len(request); i += 4 {
			requestID += binary.BigEndian.Uint32(request[i:])
			shardID += binary.BigEndian.Uint16(request[i:])
		}

		// If i = |request| we stop.
		//Otherwise, i+j = |request| for some 0<j<4.
		if i != len(request) {
			buff := make([]byte, 4)
			copy(buff, request[i:])
			requestID += binary.BigEndian.Uint32(buff)
			shardID += binary.BigEndian.Uint16(buff)
		}

		return requestID, shardID % shardCount
	}
}
