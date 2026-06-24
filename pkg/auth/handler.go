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
	"encoding/json"
	"net/http"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
)

type Handler struct {
	plugin Plugin
}

func NewHandler(plugin Plugin) *Handler {
	return &Handler{
		plugin: plugin,
	}
}

func (h *Handler) Handler(chain http.Handler) http.Handler {
	return http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		authReq := &fftypes.AuthReq{
			Method: req.Method,
			URL:    req.URL,
			Header: req.Header,
		}
		if err := h.plugin.Authorize(context.Background(), authReq); err != nil {
			res.WriteHeader(403)
			encoder := json.NewEncoder(res)
			// Error is unchecked here because it is not possible to error when encoding a string to JSON
			_ = encoder.Encode(map[string]interface{}{"error": err.Error()})
		} else {
			chain.ServeHTTP(res, req)
		}
	})
}
