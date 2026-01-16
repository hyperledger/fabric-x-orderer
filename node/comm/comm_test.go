/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package comm_test

import (
	"fmt"
	"time"

	"github.com/hyperledger/fabric-x-common/common/crypto/tlsgen"
)

const (
	timeout = time.Second * 10
)

// CA that generates TLS key-pairs.
// We use only one CA because the authentication
// is based on TLS pinning
var ca = createCAOrPanic()

func createCAOrPanic() tlsgen.CA {
	ca, err := tlsgen.NewCA()
	if err != nil {
		panic(fmt.Sprintf("failed creating CA: %+v", err))
	}
	return ca
}

// TODO bring the rest of the tests from fabric/.../cluster/comm_test.go
