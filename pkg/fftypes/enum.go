// Copyright © 2023 Kaleido, Inc.
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
	"strings"

	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
)

type FFEnum string

var enumValues = map[string][]string{}

func FFEnumValue(t string, val string) FFEnum {
	t = strings.ToLower(t)
	enumValues[t] = append(enumValues[t], val)
	return FFEnum(val)
}

func FFEnumValues(t string) []interface{} {
	values := enumValues[strings.ToLower(t)]

	// Convert []string to []interface{} for openapi3.Schema.Enum.
	result := make([]interface{}, len(values))
	for i, v := range values {
		result[i] = v
	}
	return result
}

func (ts FFEnum) String() string {
	return strings.ToLower(string(ts))
}

func (ts FFEnum) Lower() FFEnum {
	return FFEnum(strings.ToLower(string(ts)))
}

func (ts FFEnum) Equals(ts2 FFEnum) bool {
	return strings.EqualFold(string(ts), string(ts2))
}

func (ts FFEnum) Value() (driver.Value, error) {
	return ts.String(), nil
}

func (ts *FFEnum) UnmarshalText(b []byte) error {
	*ts = FFEnum(strings.ToLower(string(b)))
	return nil
}

func FFEnumValid(ctx context.Context, t string, val FFEnum) bool {
	_, err := FFEnumParseString(ctx, t, string(val))
	if err != nil {
		log.L(ctx).Debugf("Value '%s' invalid for enum '%s'", val, t)
		return false
	}
	return true
}

func FFEnumParseString(ctx context.Context, t, val string) (FFEnum, error) {
	e, ok := enumValues[strings.ToLower(t)]
	if !ok {
		return "", i18n.NewError(ctx, i18n.MsgInvalidEnum, t)
	}
	for _, possible := range e {
		if strings.EqualFold(possible, val) {
			return FFEnum(strings.ToLower(possible)), nil
		}
	}
	return "", i18n.NewError(ctx, i18n.MsgInvalidEnumValue, val, t, e)
}
