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

package i18n

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/pkg/errors"
)

func truncate(s string, limit int) string {
	if len(s) > limit {
		return s[0:limit-3] + "..."
	}
	return s
}

type FFError interface {
	error
	MessageKey() ErrorMessageKey
	HTTPStatus() int
	StackTrace() string
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

type ffError struct {
	error
	msgKey ErrorMessageKey
	status int
}

// Unwrap exposes the wrapped error so that errors.Is / errors.As can inspect the full chain, including
// the original cause passed to WrapError (for example a database driver error)
func (ffe *ffError) Unwrap() error {
	return ffe.error
}

func (ffe *ffError) MessageKey() ErrorMessageKey {
	return ffe.msgKey
}

func (ffe *ffError) HTTPStatus() int {
	return ffe.status
}

func (ffe *ffError) StackTrace() string {
	if st, ok := interface{}(ffe.error).(stackTracer); ok {
		buff := new(strings.Builder)
		for _, frame := range st.StackTrace() {
			buff.WriteString(fmt.Sprintf("%+v\n", frame))
		}
		return buff.String()
	}
	return ""
}

func ffWrap(err error, msgKey ErrorMessageKey) error {
	status, ok := statusHints[string(msgKey)]
	if !ok {
		status = http.StatusInternalServerError
	}
	return &ffError{
		error:  err,
		msgKey: msgKey,
		status: status,
	}
}

// NewError creates a new error
func NewError(ctx context.Context, msg ErrorMessageKey, inserts ...interface{}) error {
	return ffWrap(errors.New(truncate(ExpandWithCode(ctx, MessageKey(msg), inserts...), 2048)), msg)
}

// WrapError wraps an error
func WrapError(ctx context.Context, err error, msg ErrorMessageKey, inserts ...interface{}) error {
	if err == nil {
		return NewError(ctx, msg, inserts...)
	}
	return ffWrap(errors.Wrap(err, truncate(ExpandWithCode(ctx, MessageKey(msg), inserts...), 2048)), msg)
}
