// Copyright © 2022 Kaleido, Inc.
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

package auth

import (
	"context"
	"net/http"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/auth/basic"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/stretchr/testify/assert"
)

func getTestBasicAuth() *basic.Auth {
	config.RootConfigReset()
	basicAuthConfig := config.RootSection("auth.basic")
	a := &basic.Auth{}
	a.InitConfig(basicAuthConfig)
	basicAuthConfig.Set(basic.PasswordFile, "../../test/data/test_users")
	a.Init(context.Background(), "basic", basicAuthConfig)
	return a
}

func getTestHandler() *Handler {
	h := NewHandler(getTestBasicAuth())
	return h
}

func TestHandlerAuthorized(t *testing.T) {
	h := getTestHandler()
	f := h.Handler(&TestHTTPHandler{})
	res := &ResponseWriter{}
	req, _ := http.NewRequest("GET", "http://localhost/api", nil)
	req.Header.Add("Authorization", "Basic ZmlyZWZseTphd2Vzb21l")
	f.ServeHTTP(res, req)
	assert.Nil(t, res.b)
}

func TestHandlerUnauthorized(t *testing.T) {
	h := getTestHandler()
	f := h.Handler(&TestHTTPHandler{})
	res := &ResponseWriter{}
	req, _ := http.NewRequest("GET", "http://localhost/api", nil)
	f.ServeHTTP(res, req)
	assert.Regexp(t, "FF00169", string(res.b))
}

type ResponseWriter struct {
	b []byte
}

func (r *ResponseWriter) Header() http.Header {
	return nil
}

func (r *ResponseWriter) Write(b []byte) (int, error) {
	if r.b == nil {
		r.b = []byte{}
	}
	r.b = append(r.b, b...)
	return len(b), nil
}

func (r *ResponseWriter) WriteHeader(statusCode int) {}

type TestHTTPHandler struct{}

func (h *TestHTTPHandler) ServeHTTP(http.ResponseWriter, *http.Request) {}
