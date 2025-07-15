/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package orderers_test

import (
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/node/delivery/client/orderers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateConnectionSource(t *testing.T) {
	factory := &orderers.ConnectionSourceFactory{}
	require.NotNil(t, factory)
	lg := flogging.MustGetLogger("test")
	connSource := factory.CreateConnectionSource(lg, 1, 2, false)
	require.NotNil(t, connSource)
	assert.IsType(t, &orderers.ConnectionSource{}, connSource)
}
