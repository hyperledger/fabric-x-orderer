package generate

import "time"

type BatchingConfig struct {
	// BatchTimeout is the amount of time to wait before creating a batch.
	BatchTimeout time.Duration `yaml:"BatchTimeout,omitempty"`
	// BatchSize controls the number of messages batched into a block and defines limits on a batch size.
	BatchSize BatchSize `yaml:"BatchSize,omitempty"`
}

type BatchSize struct {
	// MaxMessageCount is the maximum number of messages to permit in a batch. No block will contain more than this number of messages.
	MaxMessageCount uint32 `yaml:"MaxMessageCount,omitempty"`
	// AbsoluteMaxBytes is the absolute maximum number of bytes allowed for the serialized messages in a batch.
	// The maximum block size is this value plus the size of the associated metadata (usually a few KB depending upon
	// the size of the signing identities). Any transaction larger than this value will be rejected by ordering.
	// It is recommended not to exceed 49 MB, given the default grpc max message size of 100 MB
	AbsoluteMaxBytes uint32 `yaml:"AbsoluteMaxBytes,omitempty"`
	// PreferredMaxBytes is the preferred maximum number of bytes allowed for the serialized messages in a batch.
	// Roughly, this field may be considered the best effort maximum size of a batch.
	// A batch will fill with messages until this size is reached (or the max message count, or batch timeout is exceeded).
	// If adding a new message to the batch would cause the batch to exceed the preferred max bytes,
	// then the current batch is closed and written to a block, and a new batch containing the new message is created.
	// If a message larger than the preferred max bytes is received, then its batch will contain only that message.
	// Because messages may be larger than preferred max bytes (up to AbsoluteMaxBytes),
	// some batches may exceed the preferred max bytes, but will always contain exactly one transaction.
	PreferredMaxBytes uint32 `yaml:"PreferredMaxBytes,omitempty"`
}
