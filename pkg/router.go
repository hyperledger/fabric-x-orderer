package arma

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
)

type Router struct {
	Logger         Logger
	RequestToShard func([]byte) ([]byte, uint16)
	Forward        func(shard uint16, request []byte) (BackendError, error)
}

type BackendError error

type RoutingError struct {
	BackendErr string
	ForwardErr string
	ReqID      string
}

func (f RoutingError) Error() string {
	return fmt.Sprintf(`{"BackendErr": "%s", "ForwardErr": "%s", "ReqID": "%s"}`, f.BackendErr, f.ForwardErr, f.ReqID)
}

func (r *Router) Submit(request []byte) error {
	reqID, shardID := r.RequestToShard(request)
	r.Logger.Debugf("Forwarding request %d to shard %d", reqID, shardID)
	backendErr, err := r.Forward(shardID, request)
	if err != nil {
		r.Logger.Warnf("Failed forwarding request %d to shard %d: %v", reqID, shardID, err)
		return RoutingError{
			ReqID:      hex.EncodeToString(reqID),
			ForwardErr: err.Error(),
		}
	}

	if backendErr != nil {
		r.Logger.Warnf("Backend of shard %d could not enqueue request: %v", shardID, backendErr)
		return RoutingError{
			ReqID:      hex.EncodeToString(reqID),
			BackendErr: backendErr.Error(),
		}
	}

	return nil
}

func CRC32RequestToShard(shardCount uint16) func([]byte) ([]byte, uint16) {
	return func(request []byte) ([]byte, uint16) {
		reqID := crc32.Checksum(request, crc32.IEEETable)
		buff := make([]byte, 4)
		binary.BigEndian.PutUint32(buff, reqID)

		return buff, uint16(reqID) % shardCount
	}
}

func SumBasedRequestToShard(shardCount uint16) func([]byte) ([]byte, uint16) {
	return func(request []byte) ([]byte, uint16) {
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

		buff := make([]byte, 4)
		binary.BigEndian.PutUint32(buff, requestID)

		return buff, shardID % shardCount
	}
}
