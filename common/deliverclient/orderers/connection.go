/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package orderers

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"sync"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/pkg/errors"
)

type ConnectionSource struct {
	mutex              sync.RWMutex
	allEndpoints       []*Endpoint       // All endpoints, excluding the self-endpoint.
	orgToEndpointsHash map[string][]byte // Used to detect whether the endpoints or certificates has changed.
	logger             *flogging.FabricLogger
	overrides          map[string]*Endpoint // In the peer, it is used to override an orderer endpoint.
	selfEndpoint       string               // Empty when used by a peer, or the self-endpoint when used by an orderer.
}

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

type OrdererOrg struct {
	Addresses []string
	RootCerts [][]byte
}

func NewConnectionSource(logger *flogging.FabricLogger, overrides map[string]*Endpoint, selfEndpoint string) *ConnectionSource {
	return &ConnectionSource{
		orgToEndpointsHash: map[string][]byte{},
		logger:             logger,
		overrides:          overrides,
		selfEndpoint:       selfEndpoint,
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

// Update calculates whether there was a change in the endpoints or certificates, and updates the endpoint if there was
// a change. When endpoints are updated, all the 'refreshed' channels of the old endpoints are closed and a new set of
// endpoints is prepared.
//
// Update skips the self-endpoint (if not empty) when preparing the endpoint array. However, changes to the
// self-endpoint do trigger the refresh of all the endpoints.
func (cs *ConnectionSource) Update(orgs map[string]OrdererOrg) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	cs.logger.Infof("Processing updates for orderer endpoints: %+v", orgs)

	newOrgToEndpointsHash := map[string][]byte{}

	anyChange := false
	hasOrgEndpoints := false
	for orgName, org := range orgs {
		hasher := sha256.New()
		for _, cert := range org.RootCerts {
			hasher.Write(cert)
		}
		for _, address := range org.Addresses {
			hasOrgEndpoints = true
			hasher.Write([]byte(address))
		}
		hash := hasher.Sum(nil)

		newOrgToEndpointsHash[orgName] = hash

		lastHash, ok := cs.orgToEndpointsHash[orgName]
		if ok && bytes.Equal(hash, lastHash) {
			continue
		}

		cs.logger.Debugf("Found orderer org '%s' has updates", orgName)
		anyChange = true
	}

	for orgName := range cs.orgToEndpointsHash {
		if _, ok := orgs[orgName]; !ok {
			// An org that used to exist has been removed
			cs.logger.Debugf("Found orderer org '%s' has been removed", orgName)
			anyChange = true
		}
	}

	if !anyChange {
		cs.logger.Debugf("No orderer endpoint addresses or TLS certs were changed")
		// No TLS certs changed, no org specified endpoints changed,
		// and if we are using global endpoints, they are the same
		// as our last set.  No need to update anything.
		return
	}

	cs.orgToEndpointsHash = newOrgToEndpointsHash

	for _, endpoint := range cs.allEndpoints {
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

	for _, org := range orgs {
		var rootCerts [][]byte
		for _, rootCert := range org.RootCerts {
			if hasOrgEndpoints {
				rootCerts = append(rootCerts, rootCert)
			}
		}

		// Note, if !hasOrgEndpoints, this for loop is a no-op
		for _, address := range org.Addresses {
			if address == cs.selfEndpoint {
				cs.logger.Debugf("Skipping self endpoint [%s] from org specific endpoints", address)
				continue
			}
			overrideEndpoint, ok := cs.overrides[address]
			if ok {
				cs.allEndpoints = append(cs.allEndpoints, &Endpoint{
					Address:   overrideEndpoint.Address,
					RootCerts: overrideEndpoint.RootCerts,
					Refreshed: make(chan struct{}),
				})
				continue
			}

			cs.allEndpoints = append(cs.allEndpoints, &Endpoint{
				Address:   address,
				RootCerts: rootCerts,
				Refreshed: make(chan struct{}),
			})
		}
	}

	cs.logger.Debugf("Returning an orderer connection pool source with org specific endpoints: %+v", cs.allEndpoints)
}
