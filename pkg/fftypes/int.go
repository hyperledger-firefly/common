// Copyright © 2024 Kaleido, Inc.
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
	"encoding/json"
	"math/big"
	"strconv"

	"github.com/hyperledger-firefly/common/pkg/i18n"
)

// FFuint64 on the API are serialized as Base10 strings, and can be parsed from multiple bases
// (very similar to fftypes.FFBigInt, but limited to a 64bit unsigned integer)
type FFuint64 uint64

func (i FFuint64) MarshalText() ([]byte, error) {
	// Represent as base 10 string in Marshalled JSON
	return []byte(strconv.FormatUint(uint64(i), 10)), nil
}

func (i *FFuint64) UnmarshalJSON(b []byte) error {
	var val interface{}
	if err := json.Unmarshal(b, &val); err != nil {
		return i18n.WrapError(context.Background(), err, i18n.MsgBigIntParseFailed, b)
	}
	switch val := val.(type) {
	case string:
		bi, ok := new(big.Int).SetString(val, 0)
		if !ok {
			return i18n.NewError(context.Background(), i18n.MsgBigIntParseFailed, b)
		}
		*i = FFuint64(bi.Uint64())
		return nil
	case float64:
		*i = FFuint64(val)
		return nil
	default:
		return i18n.NewError(context.Background(), i18n.MsgBigIntParseFailed, b)
	}
}

func (i *FFuint64) Uint64() uint64 {
	if i == nil {
		return 0
	}
	return uint64(*i)
}

// FFint64 on the API are serialized as Base10 strings, and can be parsed from multiple bases
// (very similar to fftypes.FFBigInt, but limited to a 64bit signed integer)
type FFint64 int64

func (i FFint64) MarshalText() ([]byte, error) {
	// Represent as base 10 string in Marshalled JSON
	return []byte(strconv.FormatInt(int64(i), 10)), nil
}

func (i *FFint64) UnmarshalJSON(b []byte) error {
	var val interface{}
	if err := json.Unmarshal(b, &val); err != nil {
		return i18n.WrapError(context.Background(), err, i18n.MsgBigIntParseFailed, b)
	}
	switch val := val.(type) {
	case string:
		bi, ok := new(big.Int).SetString(val, 0)
		if !ok {
			return i18n.NewError(context.Background(), i18n.MsgBigIntParseFailed, b)
		}
		*i = FFint64(bi.Int64())
		return nil
	case float64:
		*i = FFint64(val)
		return nil
	default:
		return i18n.NewError(context.Background(), i18n.MsgBigIntParseFailed, b)
	}
}

func (i *FFint64) Int64() int64 {
	if i == nil {
		return 0
	}
	return int64(*i)
}
