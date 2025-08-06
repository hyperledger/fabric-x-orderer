/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"
)

func BatchIDToString(id BatchID) string {
	if id == nil || reflect.ValueOf(id).IsNil() {
		return "<nil>"
	}
	s := fmt.Sprintf("Sh,Pr,Sq,Dg: <%d,%d,%d,%s>", id.Shard(), id.Primary(), id.Seq(), hex.EncodeToString(id.Digest()))
	return s
}

func BatchIDEqual(a BatchID, b BatchID) bool {
	if a.Shard() == b.Shard() && a.Primary() == b.Primary() && a.Seq() == b.Seq() && bytes.Equal(a.Digest(), b.Digest()) {
		return true
	}
	return false
}
