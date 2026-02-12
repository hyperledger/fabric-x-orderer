/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package orderers

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"sync"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/pkg/errors"
)

// Endpoint represents an orderer endpoint, which includes the endpoint address, TLS root certs, and a channel that is closed when the endpoint is refreshed.
type Endpoint struct {
	Address   string
	RootCerts [][]byte
	Refreshed chan struct{}
}

// String returns a string representation of the Endpoint, including the address and a hash of the TLS root certs. If the Endpoint is nil, it returns "<nil>".
func (e *Endpoint) String() string {
	if e == nil {
		return "<nil>"
	}

	certHashStr := "<nil>"

	if e.RootCerts != nil {
		hash := flattenRootCerts(e.RootCerts)
		certHashStr = fmt.Sprintf("%X", hash)
	}

	return fmt.Sprintf("Address: %s, RootCertHash: %s", e.Address, certHashStr)
}

type Party2Endpoint map[types.PartyID]*Endpoint

type ConnectionSource struct {
	mutex            sync.RWMutex
	allEndpoints     []*Endpoint                 // All endpoints, excluding the self-endpoint.
	partyToEndpoints map[types.PartyID]*Endpoint // All endpoints, including self party, used to detect changes.
	logger           *flogging.FabricLogger
	selfParty        types.PartyID // The party ID of the entity using this connection source. If PartyID is zero, it means the user of this connection source is not part of the oredering organizations.
}

type OrdererOrg struct {
	Addresses []string
	RootCerts [][]byte
}

func NewConnectionSource(logger *flogging.FabricLogger, selfParty types.PartyID) *ConnectionSource {
	return &ConnectionSource{
		partyToEndpoints: map[types.PartyID]*Endpoint{},
		logger:           logger,
		selfParty:        selfParty,
	}
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

// Update2 calculates whether there was a change in the endpoints or certificates, and updates the endpoint if there was
// a change. When endpoints are updated, all the 'refreshed' channels of the old endpoints are closed and a new set of
// endpoints is prepared.
//
// Update skips the self-endpoint (if not empty) when preparing the endpoint array. However, changes to the
// self-endpoint do trigger the refresh of all the endpoints.
func (cs *ConnectionSource) Update2(party2Endpoints Party2Endpoint) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	cs.logger.Infof("Processing updates for orderer endpoints: %+v", party2Endpoints)

	if anyChange := cs.detectChanges(party2Endpoints); !anyChange {
		cs.logger.Debugf("No orderer endpoint addresses or TLS certs were changed")
		// No TLS certs changed, no org specified endpoints changed,
		// and if we are using global endpoints, they are the same
		// as our last set.  No need to update anything.
		return
	}

	for _, endpoint := range cs.partyToEndpoints {
		// Alert any existing consumers that have a reference to the old endpoints
		// that their reference is now stale and they should get a new one.
		// This is done even for endpoints which have the same TLS certs and address
		// but this is desirable to help load balance.  For instance if only
		// one orderer were defined, and the config is updated to include 4 more, we
		// want the peers to disconnect from that original orderer and reconnect
		// evenly across the now five.
		close(endpoint.Refreshed)
	}

	cs.allEndpoints = nil
	clear(cs.partyToEndpoints)

	for partyID, endpoint := range party2Endpoints {
		freshEndpoint := &Endpoint{
			Address:   endpoint.Address,
			RootCerts: endpoint.RootCerts,
			Refreshed: make(chan struct{}),
		}

		cs.partyToEndpoints[partyID] = freshEndpoint

		if partyID == cs.selfParty {
			cs.logger.Debugf("Skipping self party [%d] with endpoint [%s] from allEndpoints", partyID, endpoint.Address)
			continue
		}

		cs.allEndpoints = append(cs.allEndpoints, freshEndpoint)
	}

	cs.logger.Debugf("Returning an orderer connection pool source with org specific endpoints: %+v", cs.allEndpoints)
}

func (cs *ConnectionSource) detectChanges(party2Endpoints Party2Endpoint) bool {
	if len(party2Endpoints) != len(cs.partyToEndpoints) {
		cs.logger.Debugf("Number of orderer endpoints has changed. Previous: %d, new: %d", len(cs.partyToEndpoints), len(party2Endpoints))
		return true
	}

	for partyID, endpoint := range party2Endpoints {
		existingEndpoint, ok := cs.partyToEndpoints[partyID]
		if !ok {
			cs.logger.Debugf("Found new orderer party [%d] with endpoint [%s]", partyID, endpoint.Address)
			return true
		}

		if endpoint.Address != existingEndpoint.Address {
			cs.logger.Debugf("Orderer party [%d] has a different endpoint address. Previous: [%s], new: [%s]", partyID, existingEndpoint.Address, endpoint.Address)
			return true
		}

		if !bytes.Equal(flattenRootCerts(endpoint.RootCerts), flattenRootCerts(existingEndpoint.RootCerts)) {
			cs.logger.Debugf("Orderer party [%d] has different TLS root certs", partyID)
			return true
		}
	}

	return false
}

func flattenRootCerts(rootCerts [][]byte) []byte {
	hasher := sha256.New()
	for _, cert := range rootCerts {
		hasher.Write(cert)
	}
	return hasher.Sum(nil)
}
