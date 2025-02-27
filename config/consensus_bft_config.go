package config

import "time"

// ConsensusConfig includes the consensus configuration,
type ConsensusConfig struct {
	BFTConfig SmartBFTConfig `yaml:"SmartBFT,omitempty"`
}

type SmartBFTConfig struct {
	// RequestBatchMaxInterval is the maximal time interval a request batch can wait before it is proposed.
	// A request batch is accumulating requests until `RequestBatchMaxInterval` had elapsed from the time the batch was first created
	// (i.e. the time the first request was added to it), or until it is of count `RequestBatchMaxCount`,
	// or it reaches `RequestBatchMaxBytes`, whichever occurs first.
	RequestBatchMaxInterval time.Duration `yaml:"RequestBatchMaxInterval,omitempty"`
	// RequestForwardTimeout is started from the moment a request is submitted,
	// and defines the interval after which a request is forwarded to the leader.
	RequestForwardTimeout time.Duration `yaml:"RequestForwardTimeout,omitempty"`
	// RequestComplainTimeout is started when `RequestForwardTimeout` expires,
	// and defines the interval after which the node complains about the view leader.
	RequestComplainTimeout time.Duration `yaml:"RequestComplainTimeout,omitempty"`
	// RequestAutoRemoveTimeout is started when `RequestComplainTimeout` expires,
	// and defines the interval after which a request is removed (dropped) from the request pool.
	RequestAutoRemoveTimeout time.Duration `yaml:"RequestAutoRemoveTimeout,omitempty"`
	// ViewChangeResendInterval defines the interval after which the ViewChange message is resent.
	ViewChangeResendInterval time.Duration `yaml:"ViewChangeResendInterval,omitempty"`
	// ViewChangeTimeout is started when a node first receives a quorum of `ViewChange` messages,
	// and defines the interval after which the node will try to initiate a view change with a higher view number.
	ViewChangeTimeout time.Duration `yaml:"ViewChangeTimeout,omitempty"`
	// LeaderHeartbeatTimeout is the interval after which, if nodes do not receive a "sign of life" from the leader,
	// they complain about the current leader and try to initiate a view change. A sign of life is either a heartbeat or a message from the leader.
	LeaderHeartbeatTimeout time.Duration `yaml:"LeaderHeartbeatTimeout,omitempty"`
	// CollectTimeout is the interval after which the node stops listening to `StateTransferResponse` messages,
	// stops collecting information about view metadata from remote nodes.
	CollectTimeout time.Duration `yaml:"CollectTimeout,omitempty"`
	// IncomingMessageBufferSize is the size of the buffer holding incoming messages before they are processed (maximal number of messages).
	IncomingMessageBufferSize uint64 `yaml:"IncomingMessageBufferSize,omitempty"`
	// RequestPoolSize is the number of pending requests retained by the node.
	// The `RequestPoolSize` is recommended to be at least double (x2) the `RequestBatchMaxCount`.
	// This cannot be changed dynamically and the node must be restarted to pick up the change.
	RequestPoolSize uint64 `yaml:"RequestPoolSize,omitempty"`
	// LeaderHeartbeatCount is the number of heartbeats per `LeaderHeartbeatTimeout` that the leader should emit.
	// The heartbeat-interval is equal to: `LeaderHeartbeatTimeout/LeaderHeartbeatCount`.
	LeaderHeartbeatCount uint64 `yaml:"LeaderHeartbeatCount,omitempty"`
}
