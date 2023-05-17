package arma

import "fmt"

type Router struct {
	ShardCount     uint16
	Logger         Logger
	RequestToShard func([]byte) (string, uint16)
	Forward        func(shard uint16, request []byte) (BackendError, error)
}

type BackendError error

func (r *Router) Submit(request []byte) error {
	reqID, shardID := r.RequestToShard(request)
	r.Logger.Debugf("Forwarding request %s to shard %d", reqID[:8], shardID)
	backendErr, err := r.Forward(shardID, request)
	if err != nil {
		r.Logger.Warnf("Failed forwarding request %s to shard %d: %v", reqID[:8], shardID, err)
		return err
	}

	bckErrStr := backendErr.Error()
	if bckErrStr != "" {
		r.Logger.Warnf("Backend of shard %s could not enqueue request: %v", shardID, bckErrStr)
		return fmt.Errorf("%s", backendErr)
	}

	return nil
}
