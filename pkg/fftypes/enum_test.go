// Copyright © 2021 Kaleido, Inc.
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

package fftypes

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestEnum = FFEnum

var (
	TestEnumVal1 = FFEnumValue("ut", "test_enum_val1")
	TestEnumVal2 = FFEnumValue("ut", "test_enum_val2")
)

func TestFFEnumStringCompareEtc(t *testing.T) {

	testEnum1 := TestEnum("Test_Enum_VAL1")
	assert.True(t, TestEnumVal1.Equals(testEnum1))
	assert.Equal(t, TestEnumVal1, testEnum1.Lower())

	var tdv driver.Valuer = testEnum1
	v, err := tdv.Value()
	assert.Nil(t, err)
	assert.Equal(t, "test_enum_val1", v)

	var utStruct struct {
		TXType TestEnum `json:"txType"`
	}
	err = json.Unmarshal([]byte(`{"txType": "Test_Enum_VAL1"}`), &utStruct)
	assert.NoError(t, err)
	assert.Equal(t, "test_enum_val1", string(utStruct.TXType))

}

func TestFFEnumValues(t *testing.T) {
	assert.Equal(t, FFEnum("test_enum_val1"), TestEnumVal1)
	assert.Equal(t, FFEnum("test_enum_val2"), TestEnumVal2)
	assert.Equal(t, []interface{}{"test_enum_val1", "test_enum_val2"}, FFEnumValues("ut"))
}

func TestFFEnumTypeCaseInsensitive(t *testing.T) {
	ctx := context.Background()
	enumVal := FFEnumValue("UT_Case", "case_val1")

	assert.Equal(t, []interface{}{"case_val1"}, FFEnumValues("ut_case"))
	assert.Equal(t, []interface{}{"case_val1"}, FFEnumValues("UT_CASE"))

	v, err := FFEnumParseString(ctx, "UT_CASE", "case_val1")
	assert.NoError(t, err)
	assert.Equal(t, enumVal.Lower(), v)
}

func TestFFEnumParseString(t *testing.T) {
	ctx := context.Background()
	v, err := FFEnumParseString(ctx, "ut", "test_enum_val1")
	assert.NoError(t, err)
	assert.Equal(t, v, TestEnumVal1)
	assert.True(t, FFEnumValid(ctx, "ut", "test_enum_val1"))

	v, err = FFEnumParseString(ctx, "foobar", "foobar")
	assert.Regexp(t, "FF00171", err)
	assert.Empty(t, v)
	assert.False(t, FFEnumValid(ctx, "foobar", "foobar"))

	v, err = FFEnumParseString(ctx, "ut", "foobar")
	assert.Regexp(t, "FF00172", err)
	assert.Empty(t, v)

	mixed := FFEnumValue("ut_mixed", "Test_Enum_Mixed")
	v, err = FFEnumParseString(ctx, "ut_mixed", "test_enum_mixed")
	assert.NoError(t, err)
	assert.Equal(t, FFEnum("test_enum_mixed"), v)
	assert.Equal(t, mixed.Lower(), v)
}
