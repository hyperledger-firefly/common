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
	"database/sql/driver"
	"encoding/binary"
	"strings"

	"github.com/google/uuid"
	"github.com/hyperledger-firefly/common/pkg/i18n"
)

// UUID is a wrapper on a UUID implementation, ensuring Value handles nil
type UUID uuid.UUID

func NewNamespacedUUIDString(_ context.Context, namespace string, uuid *UUID) string {
	return namespace + ":" + uuid.String()
}

func ParseNamespacedUUID(ctx context.Context, nsIDStr string) (namespace string, uuid *UUID, err error) {
	idSplit := strings.Split(nsIDStr, ":")
	if len(idSplit) != 2 {
		return "", nil, i18n.NewError(ctx, i18n.MsgInvalidNamespaceUUID, nsIDStr)
	}
	ns := idSplit[0]
	uuidStr := idSplit[1]
	if err := ValidateFFNameField(ctx, ns, "namespace"); err != nil {
		return "", nil, err
	}
	u, err := ParseUUID(ctx, uuidStr)
	return ns, u, err
}

func ParseUUID(ctx context.Context, uuidStr string) (*UUID, error) {
	u, err := uuid.Parse(uuidStr)
	if err != nil {
		return nil, i18n.WrapError(ctx, err, i18n.MsgInvalidUUID)
	}
	uuid := UUID(u)
	return &uuid, nil
}

func MustParseUUID(uuidStr string) *UUID {
	uuid := UUID(uuid.MustParse(uuidStr))
	return &uuid
}

func NewUUID() *UUID {
	u := UUID(uuid.New())
	return &u
}

func (u *UUID) String() string {
	if u == nil {
		return ""
	}
	return (*uuid.UUID)(u).String()
}

func (u UUID) MarshalText() ([]byte, error) {
	return (uuid.UUID)(u).MarshalText()
}

func (u *UUID) UnmarshalText(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return (*uuid.UUID)(u).UnmarshalText(b)
}

func (u UUID) MarshalBinary() ([]byte, error) {
	return (uuid.UUID)(u).MarshalBinary()
}

func (u *UUID) UnmarshalBinary(b []byte) error {
	return (*uuid.UUID)(u).UnmarshalBinary(b)
}

func (u *UUID) HashBucket(buckets int) int {
	if u == nil || buckets <= 0 {
		return 0
	}
	// Explicitly cast `buckets` to uint64 to match the type used in the modulo operation
	// and ensure the result is safely converted back to int.

	// #nosec G115 - Safe because `buckets` is calculated using modulo operation
	return int(binary.BigEndian.Uint64((*u)[8:]) % uint64(buckets))

}

func (u *UUID) Value() (driver.Value, error) {
	if u == nil {
		return nil, nil
	}
	return (uuid.UUID)(*u).Value()
}

func (u *UUID) Scan(src interface{}) error {
	return (*uuid.UUID)(u).Scan(src)
}

func (u *UUID) Equals(u2 *UUID) bool {
	switch {
	case u == nil && u2 == nil:
		return true
	case u == nil || u2 == nil:
		return false
	default:
		return *u == *u2
	}
}
