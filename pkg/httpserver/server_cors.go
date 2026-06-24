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

package httpserver

import (
	"context"
	"net/http"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/rs/cors"
)

func WrapCorsIfEnabled(ctx context.Context, conf config.Section, chain http.Handler) http.Handler {
	if !conf.GetBool(CorsEnabled) {
		return chain
	}
	corsOptions := cors.Options{
		AllowedOrigins:   conf.GetStringSlice(CorsAllowedOrigins),
		AllowedMethods:   conf.GetStringSlice(CorsAllowedMethods),
		AllowedHeaders:   conf.GetStringSlice(CorsAllowedHeaders),
		AllowCredentials: conf.GetBool(CorsAllowCredentials),
		MaxAge:           conf.GetInt(CorsMaxAge),
		Debug:            conf.GetBool(CorsDebug),
	}
	log.L(ctx).Debugf("CORS origins=%v methods=%v headers=%v creds=%t maxAge=%d",
		corsOptions.AllowedOrigins,
		corsOptions.AllowedMethods,
		corsOptions.AllowedHeaders,
		corsOptions.AllowCredentials,
		corsOptions.MaxAge,
	)
	c := cors.New(corsOptions)
	return c.Handler(chain)
}
