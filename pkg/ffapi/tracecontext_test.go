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
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testTraceID     = "4bf92f3577b34da6a3ce929d0e0e4736"
	testParentID    = "00f067aa0ba902b7"
	testTraceParent = "00-" + testTraceID + "-" + testParentID + "-00"
)

func TestTraceContextFromHeaders(t *testing.T) {
	h := http.Header{}
	h.Set(TraceParentHeader, testTraceParent)
	h.Set(TraceStateHeader, "dd=s:-1;o:rum")
	assert.Equal(t, &TraceContext{
		TraceID:    testTraceID,
		ParentID:   testParentID,
		Flags:      "00",
		TraceState: "dd=s:-1;o:rum",
	}, TraceContextFromHeaders(h))

	// A later version may append fields, which are dropped when the context is passed on
	h.Set(TraceParentHeader, "01-"+testTraceID+"-"+testParentID+"-01-extra")
	tc := TraceContextFromHeaders(h)
	require.NotNil(t, tc)
	assert.Equal(t, "00-"+testTraceID+"-"+testParentID+"-01", tc.TraceParent())

	for _, bad := range []string{
		"",
		"not-a-traceparent",
		"ff-" + testTraceID + "-" + testParentID + "-01",              // invalid version
		"00-" + testTraceID + "-" + testParentID + "-01-extra",        // version 00 has exactly 4 fields
		"00-00000000000000000000000000000000-" + testParentID + "-01", // zero trace ID
		"00-" + testTraceID + "-0000000000000000-01",                  // zero parent ID
		"00-4BF92F3577B34DA6A3CE929D0E0E4736-" + testParentID + "-01", // uppercase
		"00-" + testTraceID + "-" + testParentID,                      // missing flags
	} {
		h.Set(TraceParentHeader, bad)
		assert.Nil(t, TraceContextFromHeaders(h), bad)
	}
}

func TestNewTraceContext(t *testing.T) {
	tc := NewTraceContext(testTraceID)
	assert.Equal(t, testTraceID, tc.TraceID)
	assert.Regexp(t, "^[0-9a-f]{16}$", tc.ParentID)
	assert.Equal(t, "01", tc.Flags)

	for _, notTraceID := range []string{"", "abc12345", "00000000000000000000000000000000"} {
		tc = NewTraceContext(notTraceID)
		assert.Regexp(t, "^[0-9a-f]{32}$", tc.TraceID)
		assert.NotEqual(t, notTraceID, tc.TraceID)
	}
	assert.NotEqual(t, NewTraceContext("").TraceID, NewTraceContext("").TraceID)
}

func TestTraceContextSetHeaders(t *testing.T) {
	h := http.Header{}
	h.Set(TraceStateHeader, "stale=1")
	(&TraceContext{TraceID: testTraceID, ParentID: testParentID, Flags: "01"}).SetHeaders(h)
	assert.Equal(t, "00-"+testTraceID+"-"+testParentID+"-01", h.Get(TraceParentHeader))
	assert.Empty(t, h.Values(TraceStateHeader))

	(&TraceContext{TraceID: testTraceID, ParentID: testParentID, Flags: "01", TraceState: "dd=s:1"}).SetHeaders(h)
	assert.Equal(t, "dd=s:1", h.Get(TraceStateHeader))
}

func TestRequestContextFromHeaders(t *testing.T) {
	// The traceparent wins over the request ID header, and is passed on unchanged
	h := http.Header{}
	h.Set(DefaultRequestIDHeader, "abc12345")
	h.Set(TraceParentHeader, testTraceParent)
	requestID, tc := RequestContextFromHeaders(h)
	assert.Equal(t, testTraceID, requestID)
	assert.Equal(t, testTraceParent, tc.TraceParent())

	// A request ID that is a valid trace ID starts a trace with that ID
	h = http.Header{}
	h.Set(DefaultRequestIDHeader, testTraceID)
	requestID, tc = RequestContextFromHeaders(h)
	assert.Equal(t, testTraceID, requestID)
	assert.Equal(t, testTraceID, tc.TraceID)

	// Any other request ID is kept, without a trace context
	h.Set(DefaultRequestIDHeader, "abc12345")
	requestID, tc = RequestContextFromHeaders(h)
	assert.Equal(t, "abc12345", requestID)
	assert.Nil(t, tc)

	// Nothing from the caller starts a new trace, and its trace ID is the request ID
	requestID, tc = RequestContextFromHeaders(http.Header{})
	assert.Regexp(t, "^[0-9a-f]{32}$", requestID)
	assert.Equal(t, requestID, tc.TraceID)
}

func TestSetRequestContextHeaders(t *testing.T) {
	h := http.Header{}
	SetRequestContextHeaders(h, "abc12345", nil)
	assert.Equal(t, "abc12345", h.Get(DefaultRequestIDHeader))
	assert.Empty(t, h.Get(TraceParentHeader))

	SetRequestContextHeaders(h, testTraceID, &TraceContext{TraceID: testTraceID, ParentID: testParentID, Flags: "00"})
	assert.Equal(t, testTraceID, h.Get(DefaultRequestIDHeader))
	assert.Equal(t, testTraceParent, h.Get(TraceParentHeader))
}

func TestAPIWrapperTraceContext(t *testing.T) {
	var ctx context.Context
	s, _, done := newTestServer(t, []*Route{{
		Name:            "testRoute",
		Path:            "/test",
		Method:          "GET",
		JSONOutputCodes: []int{200},
		JSONHandler: func(r *APIRequest) (output interface{}, err error) {
			ctx = r.Req.Context()
			return map[string]interface{}{}, nil
		},
	}}, "", nil)
	defer done()

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/test", s.Addr()), nil)
	require.NoError(t, err)
	req.Header.Set(DefaultRequestIDHeader, "abc12345")
	req.Header.Set(TraceParentHeader, testTraceParent)
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode)
	assert.Equal(t, testTraceID, ctx.Value(CtxFFRequestIDKey{}))
	assert.Equal(t, testTraceParent, ctx.Value(CtxTraceContextKey{}).(*TraceContext).TraceParent())

	// A legacy request ID puts no trace context on the request context
	req.Header.Del(TraceParentHeader)
	res, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode)
	assert.Equal(t, "abc12345", ctx.Value(CtxFFRequestIDKey{}))
	assert.Nil(t, ctx.Value(CtxTraceContextKey{}))
}
