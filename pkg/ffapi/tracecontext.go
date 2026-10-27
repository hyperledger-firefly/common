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
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"regexp"
	"strings"
)

const (
	// TraceParentHeader and TraceStateHeader are the W3C Trace Context headers
	// (https://www.w3.org/TR/trace-context/), read by OpenTelemetry, Datadog, and most
	// other tracing SDKs by default.
	TraceParentHeader = "traceparent"
	TraceStateHeader  = "tracestate"
)

var (
	// traceParentRegex matches version-traceid-parentid-flags, with the trailing fields
	// that later versions of the spec may add
	traceParentRegex = regexp.MustCompile(`^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})(-.*)?$`)
	traceIDRegex     = regexp.MustCompile(`^[0-9a-f]{32}$`)
	zeroTraceID      = strings.Repeat("0", 32)
	zeroParentID     = strings.Repeat("0", 16)
)

// TraceContext is a W3C trace context, either received from the caller or started by
// this process. This process does not record spans, so the context is passed on to
// downstream services unchanged and their spans join the caller's trace.
type TraceContext struct {
	TraceID    string // 32 lowercase hex characters
	ParentID   string // 16 lowercase hex characters
	Flags      string // 2 lowercase hex characters
	TraceState string // optional vendor-specific data, passed on unchanged
}

type CtxTraceContextKey struct{}

// TraceContextFromHeaders returns the trace context from a valid traceparent header,
// or nil if there is none.
func TraceContextFromHeaders(headers http.Header) *TraceContext {
	m := traceParentRegex.FindStringSubmatch(headers.Get(TraceParentHeader))
	if m == nil {
		return nil
	}
	version, traceID, parentID, flags, extra := m[1], m[2], m[3], m[4], m[5]
	if version == "ff" || (version == "00" && extra != "") || traceID == zeroTraceID || parentID == zeroParentID {
		return nil
	}
	return &TraceContext{
		TraceID:    traceID,
		ParentID:   parentID,
		Flags:      flags,
		TraceState: headers.Get(TraceStateHeader),
	}
}

// NewTraceContext starts a new sampled trace. If traceID is a valid trace ID it is used,
// otherwise a random one is generated.
func NewTraceContext(traceID string) *TraceContext {
	if !traceIDRegex.MatchString(traceID) || traceID == zeroTraceID {
		traceID = randomHex(16)
	}
	return &TraceContext{
		TraceID:  traceID,
		ParentID: randomHex(8),
		Flags:    "01",
	}
}

// TraceParent returns the traceparent header value for this trace context
func (tc *TraceContext) TraceParent() string {
	return "00-" + tc.TraceID + "-" + tc.ParentID + "-" + tc.Flags
}

// SetHeaders sets the traceparent and tracestate headers on an outbound request
func (tc *TraceContext) SetHeaders(headers http.Header) {
	headers.Set(TraceParentHeader, tc.TraceParent())
	if tc.TraceState != "" {
		headers.Set(TraceStateHeader, tc.TraceState)
	} else {
		headers.Del(TraceStateHeader)
	}
}

// RequestContextFromHeaders resolves the request ID and trace context for an inbound
// request. W3C trace context takes precedence, so that the request ID logged by every
// FireFly service is the trace ID that tracing tools show for the same request:
//
//  1. A valid traceparent is passed on unchanged, and its trace ID is the request ID
//  2. Otherwise a request ID header from the caller is kept as the request ID, and when
//     it is a valid trace ID a new trace is started with it. Any other request ID (such
//     as the short IDs of older FireFly services) is passed on without a trace context,
//     so every service logs the same ID.
//  3. Otherwise a new trace is started, and its trace ID is the request ID
func RequestContextFromHeaders(headers http.Header) (string, *TraceContext) {
	if tc := TraceContextFromHeaders(headers); tc != nil {
		return tc.TraceID, tc
	}
	if requestID := headers.Get(RequestIDHeader()); requestID != "" {
		if traceIDRegex.MatchString(requestID) && requestID != zeroTraceID {
			return requestID, NewTraceContext(requestID)
		}
		return requestID, nil
	}
	tc := NewTraceContext("")
	return tc.TraceID, tc
}

// SetRequestContextHeaders sets the request ID header, and the trace context headers when
// there is a trace context, on an outbound request
func SetRequestContextHeaders(headers http.Header, requestID string, tc *TraceContext) {
	headers.Set(RequestIDHeader(), requestID)
	if tc != nil {
		tc.SetHeaders(headers)
	}
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
