// Copyright © 2026 Kaleido, Inc.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ffapi

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/stretchr/testify/assert"
)

const uint256Max = "115792089237316195423570985008687907853269984665640564039457584007913129639935"

func mustBigInt(s string) *big.Int {
	i, ok := new(big.Int).SetString(s, 10)
	if !ok {
		panic(s)
	}
	return i
}

// Values passed to the QueryBuilder go through toSimpleValue, which must render big number
// types as their full decimal string rather than falling back to fftypes.JSONObject.GetString
// (which only understands string/bool/float/int/uint, and returns "" for anything else).
func TestQueryBuilderValueTypes(t *testing.T) {
	for name, tc := range map[string]struct {
		value    any
		expected string
	}{
		// Controls - these already work
		"string": {"abc", "abc"},
		"int":    {42, "42"},
		"uint64": {uint64(18446744073709551615), "18446744073709551615"},
		"bool":   {true, "true"},

		// Big number types
		"*big.Int small":       {big.NewInt(42), "42"},
		"*big.Int uint256 max": {mustBigInt(uint256Max), uint256Max},
		"*fftypes.FFBigInt":    {(*fftypes.FFBigInt)(mustBigInt(uint256Max)), uint256Max},
		"json.Number":          {json.Number("9007199254740993"), "9007199254740993"},
	} {
		t.Run(name, func(t *testing.T) {
			// Single value ops (Equal, GreaterThan, ...)
			q := NewQueryBuilder().Equal("amount", tc.value).Query()
			assert.Equal(t, tc.expected, q.Eq[0].Value.String(), "Equal")

			// Multi value ops (In, NotIn)
			q = NewQueryBuilder().In("amount", []any{tc.value}).Query()
			assert.Equal(t, tc.expected, q.In[0].Values[0].String(), "In")

			// Value is `json:"value,omitempty"`, so an empty value would be dropped entirely
			// when the query is serialized
			b, err := json.Marshal(NewQueryBuilder().Equal("amount", tc.value).Query())
			assert.NoError(t, err)
			assert.JSONEq(t, `{"eq":[{"field":"amount","value":"`+tc.expected+`"}]}`, string(b), "JSON")
		})
	}
}

func TestQueryBuilderNilBigInts(t *testing.T) {
	assert.Empty(t, toSimpleValue((*big.Int)(nil)))
	assert.Empty(t, toSimpleValue((*fftypes.FFBigInt)(nil)))
}
