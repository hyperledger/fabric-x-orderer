/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package ledger offers the exact same interfaces as the `hyperledger/fabric/common/ledger`, only more performant.
// Interface aliasing (type interface-A = interface-B) is used so that a user of the `fabric` package would be able to
// use this package and vice-versa.
package ledger

import (
	fabric_ledger "github.com/hyperledger/fabric/common/ledger"
)

// Ledger captures the methods that are common across the 'PeerLedger', 'OrdererLedger', and 'ValidatedLedger'.
type Ledger = fabric_ledger.Ledger

// ResultsIterator - an iterator for query result set.
type ResultsIterator = fabric_ledger.ResultsIterator

// QueryResultsIterator - an iterator for query result set.
type QueryResultsIterator = fabric_ledger.QueryResultsIterator

// QueryResult - a general interface for supporting different types of query results. Actual types differ for different queries.
type QueryResult = fabric_ledger.QueryResult

// PrunePolicy - a general interface for supporting different pruning policies.
type PrunePolicy = fabric_ledger.PrunePolicy
