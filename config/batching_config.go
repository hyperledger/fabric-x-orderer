/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import "time"

type BatchingConfig struct {
	// BatchTimeouts controls the timeouts of a batch.
	BatchTimeouts BatchTimeouts `yaml:"BatchTimeouts,omitempty"`
	// BatchSize controls the number of messages batched into a block and defines limits on a batch size.
	BatchSize BatchSize `yaml:"BatchSize,omitempty"`
	// RequestMaxBytes is the maximal request size in bytes.
	RequestMaxBytes uint64 `yaml:"RequestMaxBytes,omitempty"`
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
	// *** NOTE: This field is not in use. ***
	PreferredMaxBytes uint32 `yaml:"PreferredMaxBytes,omitempty"`
}

type BatchTimeouts struct {
	// BatchCreationTimeout is the time a batch can wait before it is created.
	BatchCreationTimeout time.Duration `yaml:"BatchCreationTimeout,omitempty"`
	// FirstStrikeThreshold defines the maximum time a request can remain in the memory pool without being batched.
	// After this duration, the request is forwarded to the primary batcher by the secondary batcher.
	FirstStrikeThreshold time.Duration `yaml:"FirstStrikeThreshold,omitempty"`
	// SecondStrikeThreshold defines the maximum duration that can pass following the FirstStrikeThreshold.
	// If this timeout is reached, a complaint is sent to the consensus layer, suspecting primary censorship of the request.
	SecondStrikeThreshold time.Duration `yaml:"SecondStrikeThreshold,omitempty"`
	// AutoRemoveTimeout defines the maximum time a request can stay in the memory pool after the second threshold is reached.
	// If this timeout is reached, the request is removed from the pool.
	// NOTE: This timeout's intended purpose is not implemented and is used for GC by the memory pool.
	AutoRemoveTimeout time.Duration `yaml:"AutoRemoveTimeout,omitempty"`
}
