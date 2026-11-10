/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package leveldbhelper

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

// newTestProvider returns a provider on a new directory, closed when the test ends.
func newTestProvider(t *testing.T) *Provider {
	p, err := NewProvider(&Conf{DBPath: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(p.Close)
	return p
}

// checkHandleEntries expects the handle to hold exactly the given keys and values, in order.
func checkHandleEntries(t *testing.T, h *DBHandle, expectedKeys []string, expectedValues []string) {
	lower, upper := h.keyRange()
	itr, err := h.db.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	require.NoError(t, err)
	var actualKeys []string
	var actualValues []string
	for itr.First(); itr.Valid(); itr.Next() {
		value, err := itr.ValueAndErr()
		require.NoError(t, err)
		actualKeys = append(actualKeys, string(retrieveAppKey(itr.Key())))
		actualValues = append(actualValues, string(value))
	}
	require.NoError(t, itr.Close())
	require.Equal(t, expectedKeys, actualKeys)
	require.Equal(t, expectedValues, actualValues)
}

// retrieveAppKey returns the key of a handle's iterator entry without the handle's prefix.
func retrieveAppKey(levelKey []byte) []byte {
	return bytes.SplitN(levelKey, dbNameKeySep, 2)[1]
}

func createTestKey(i int) string {
	return fmt.Sprintf("key_%06d", i)
}

func createTestValue(dbname string, i int) string {
	return fmt.Sprintf("value_%s_%06d", dbname, i)
}

func createTestKeys(start int, end int) []string {
	var keys []string
	for i := start; i <= end; i++ {
		keys = append(keys, createTestKey(i))
	}
	return keys
}

func createTestValues(dbname string, start int, end int) []string {
	var values []string
	for i := start; i <= end; i++ {
		values = append(values, createTestValue(dbname, i))
	}
	return values
}
