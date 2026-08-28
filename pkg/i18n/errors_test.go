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

package i18n

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewError(t *testing.T) {
	err := NewError(context.Background(), MsgConfigFailed)
	assert.Error(t, err)
}

func TestNewErrorTruncate(t *testing.T) {
	err := NewError(context.Background(), MsgUnknownFieldValue, "field", strings.Repeat("x", 3000))
	assert.Error(t, err)
	var ffe FFError
	assert.Implements(t, &ffe, err)
	assert.Equal(t, 400, interface{}(err).(FFError).HTTPStatus())
	assert.Equal(t, MsgUnknownFieldValue, interface{}(err).(FFError).MessageKey())
}

func TestWrapError(t *testing.T) {
	err := WrapError(context.Background(), fmt.Errorf("some error"), MsgConfigFailed)
	assert.Error(t, err)
	var ffe FFError
	assert.Implements(t, &ffe, err)
	assert.Equal(t, 500, interface{}(err).(FFError).HTTPStatus())
	assert.Equal(t, MsgConfigFailed, interface{}(err).(FFError).MessageKey())
	stackString := interface{}(err).(FFError).StackTrace()
	fmt.Print(stackString)
	assert.NotEmpty(t, stackString)
}

func TestSafeStackFail(t *testing.T) {
	stackString := (&ffError{}).StackTrace()
	assert.Empty(t, stackString)
}

func TestWrapNilError(t *testing.T) {
	err := WrapError(context.Background(), nil, MsgConfigFailed)
	assert.Error(t, err)
}

func TestStackWithDebug(t *testing.T) {
	err := WrapError(context.Background(), nil, MsgConfigFailed)
	assert.Error(t, err)
}

type testCauseError struct{ code string }

func (e *testCauseError) Error() string { return "cause " + e.code }

func TestWrapErrorUnwrap(t *testing.T) {
	ctx := context.Background()
	cause := &testCauseError{code: "23505"}

	// Wrapped once by WrapError, then again by a caller with a different message
	err := WrapError(ctx, WrapError(ctx, cause, MsgDBInsertFailed), MsgDBInitFailed)
	assert.Regexp(t, "FF00173.*FF00177.*cause 23505", err)

	var target *testCauseError
	assert.True(t, errors.As(err, &target))
	assert.Equal(t, "23505", target.code)
	assert.True(t, errors.Is(err, cause))

	// The ffError itself is still reachable in the chain
	var ffe *ffError
	assert.True(t, errors.As(err, &ffe))
	assert.Equal(t, MsgDBInitFailed, ffe.msgKey)

	// NewError has no cause beyond the message itself
	assert.False(t, errors.As(NewError(ctx, MsgDBInitFailed), &target))
}
