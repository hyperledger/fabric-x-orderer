/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package orderers

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"math/rand"
	"sync"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/pkg/errors"
)

// Endpoint represents a source of replication items, e.g. batches or decisions.
// The refreshed channel is closed when the ConnectionSource is updated.
type Endpoint struct {
	Address   string
	RootCerts [][]byte
	Refreshed chan struct{}
}

func (e *Endpoint) String() string {
	if e == nil {
		return "<nil>"
	}

	certHashStr := "<nil>"

	if e.RootCerts != nil {
		hasher := md5.New()
		for _, cert := range e.RootCerts {
			hasher.Write(cert)
		}
		hash := hasher.Sum(nil)
		certHashStr = fmt.Sprintf("%X", hash)
	}

	return fmt.Sprintf("Address: %s, CertHash: %s", e.Address, certHashStr)
}

type SourceEndpoint struct {
	Address   string
	RootCerts [][]byte
}

type Party2SourceEndpoint map[types.PartyID]*SourceEndpoint

type ConnectionSource struct {
	party        types.PartyID // The party holding the object
	targetShard  types.ShardID // The target shard or consensus cluster
	partOfTarget bool          // Whether the holder of the object is part of the target shard

	mutex            sync.RWMutex
	allEndpoints     []*Endpoint                 // All endpoints, excluding the self-endpoint.
	partyToEndpoints map[types.PartyID]*Endpoint // All endpoints, including self party, used to detect changes
	logger           *flogging.FabricLogger
}

func NewConnectionSource(logger *flogging.FabricLogger, selfParty types.PartyID, targetShard types.ShardID, partOfTarget bool) *ConnectionSource {
	return &ConnectionSource{
		partyToEndpoints: make(map[types.PartyID]*Endpoint),
		logger:           logger,
		party:            selfParty,
		targetShard:      targetShard,
		partOfTarget:     partOfTarget,
	}
}

// PartyEndpoint returns a random endpoint.
func (cs *ConnectionSource) PartyEndpoint(party types.PartyID) (*Endpoint, error) {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	if len(cs.partyToEndpoints) == 0 {
		return nil, errors.Errorf("no endpoints currently defined")
	}

	ep, ok := cs.partyToEndpoints[party]
	if !ok {
		return nil, errors.Errorf("not found")
	}

	return ep, nil
}

// RandomEndpoint returns a random endpoint.
func (cs *ConnectionSource) RandomEndpoint() (*Endpoint, error) {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	if len(cs.allEndpoints) == 0 {
		return nil, errors.Errorf("no endpoints currently defined")
	}
	return cs.allEndpoints[rand.Intn(len(cs.allEndpoints))], nil
}

func (cs *ConnectionSource) Endpoints() []*Endpoint {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	return cs.allEndpoints
}

// ShuffledEndpoints returns a shuffled array of endpoints in a new slice.
func (cs *ConnectionSource) ShuffledEndpoints() []*Endpoint {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	n := len(cs.allEndpoints)
	returnedSlice := make([]*Endpoint, n)
	indices := rand.Perm(n)
	for i, idx := range indices {
		returnedSlice[i] = cs.allEndpoints[idx]
	}
	return returnedSlice
}

// Update calculates whether there was a change in the endpoints or certificates, and updates the endpoints if there was
// a change. When endpoints are updated, all the 'refreshed' channels of the old endpoints are closed and a new set of
// endpoints is prepared.
//
// Update skips the self-endpoint (if part od the target cluster) when preparing the endpoint array. However, changes to the
// self-endpoint do trigger the refresh of all the endpoints.
func (cs *ConnectionSource) Update(party2SourceEndpoint map[types.PartyID]*SourceEndpoint) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.logger.Infof("Processing updates for shard %d endpoints", cs.targetShard)

	anyChange := cs.detectChange(party2SourceEndpoint)

	if !anyChange {
		cs.logger.Debugf("No sourceEndpoint addresses or TLS certs were changed")
		// No TLS certs changed, no org specified endpoints changed,
		// and if we are using global endpoints, they are the same
		// as our last set.  No need to update anything.
		return
	}

	for _, endpoint := range cs.allEndpoints {
		// Alert any existing consumers that have a reference to the old endpoints
		// that their reference is now stale, and they should get a new one.
		// This is done even for endpoints which have the same TLS certs and address.
		close(endpoint.Refreshed)
	}

	cs.allEndpoints = nil
	cs.partyToEndpoints = make(map[types.PartyID]*Endpoint)
	for party, sourceEndpoint := range party2SourceEndpoint {
		endpoint := &Endpoint{
			Address:   sourceEndpoint.Address,
			RootCerts: sourceEndpoint.RootCerts,
			Refreshed: make(chan struct{}),
		}
		cs.partyToEndpoints[party] = endpoint

		if cs.partOfTarget && cs.party == party {
			cs.logger.Debugf("Skipping self sourceEndpoint [%s] of party %d ", sourceEndpoint.Address, party)
			continue
		}
		cs.allEndpoints = append(cs.allEndpoints, endpoint)
	}

	cs.logger.Infof("Processed updates for shard %d endpoints: %v", cs.targetShard, cs.partyToEndpoints)
}

func (cs *ConnectionSource) detectChange(party2SourceEndpoint map[types.PartyID]*SourceEndpoint) bool {
	if len(party2SourceEndpoint) != len(cs.partyToEndpoints) {
		return true
	}

	for party, sourceEP := range party2SourceEndpoint {
		ep, ok := cs.partyToEndpoints[party]
		if !ok {
			return true
		}

		if ep.Address != sourceEP.Address {
			return true
		}

		if len(ep.RootCerts) != len(sourceEP.RootCerts) {
			return true
		}

		for i, cert := range ep.RootCerts {
			if !bytes.Equal(cert, sourceEP.RootCerts[i]) {
				return true
			}
		}
	}

	return false
}
