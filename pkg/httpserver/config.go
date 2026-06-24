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
	"net/http"

	"github.com/hyperledger-firefly/common/pkg/auth/authfactory"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/fftls"
)

const (
	// CorsAllowCredentials CORS setting to control whether a browser allows credentials to be sent to this API
	CorsAllowCredentials = "credentials"
	// CorsAllowedHeaders CORS setting to control the allowed headers
	CorsAllowedHeaders = "headers"
	// CorsAllowedMethods CORS setting to control the allowed methods
	CorsAllowedMethods = "methods"
	// CorsAllowedOrigins CORS setting to control the allowed origins
	CorsAllowedOrigins = "origins"
	// CorsDebug is whether debug is enabled for the CORS implementation
	CorsDebug = "debug"
	// CorsEnabled is whether cors is enabled
	CorsEnabled = "enabled"
	// CorsMaxAge is the maximum age a browser should rely on CORS checks
	CorsMaxAge = "maxAge"
)

func InitCORSConfig(conf config.Section) {

	conf.AddKnownKey(CorsAllowCredentials, true)
	conf.AddKnownKey(CorsAllowedHeaders, []string{"*"})
	conf.AddKnownKey(CorsAllowedMethods, []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete})
	conf.AddKnownKey(CorsAllowedOrigins, []string{"*"})
	conf.AddKnownKey(CorsEnabled, true)
	conf.AddKnownKey(CorsMaxAge, 600)
	conf.AddKnownKey(CorsDebug, false)

}

const (
	// HTTPConfAddress the local address to listen on
	HTTPConfAddress = "address"
	// HTTPConfPublicURL the public address of the node to advertise in the swagger
	HTTPConfPublicURL = "publicURL"
	// HTTPConfPort the local port to listen on for HTTP/Websocket connections
	HTTPConfPort = "port"
	// HTTPConfReadTimeout the write timeout for the HTTP server
	HTTPConfReadTimeout = "readTimeout"
	// HTTPConfWriteTimeout the write timeout for the HTTP server
	HTTPConfWriteTimeout = "writeTimeout"
	// HTTPConfShutdownTimeout The maximum amount of time to wait for any open HTTP requests to finish before shutting down the HTTP server
	HTTPConfShutdownTimeout = "shutdownTimeout"
	// HTTPAuthType the auth plugin to use for the HTTP server
	HTTPAuthType = "auth.type"
)

func InitHTTPConfig(conf config.Section, defaultPort int) {
	conf.AddKnownKey(HTTPConfAddress, "127.0.0.1")
	conf.AddKnownKey(HTTPConfPublicURL)
	conf.AddKnownKey(HTTPConfPort, defaultPort)
	conf.AddKnownKey(HTTPConfReadTimeout, "15s")
	conf.AddKnownKey(HTTPConfWriteTimeout, "15s")
	conf.AddKnownKey(HTTPConfShutdownTimeout, "10s")
	conf.AddKnownKey(HTTPAuthType)

	ac := conf.SubSection("auth")
	authfactory.InitConfig(ac)

	tlsConfig := conf.SubSection("tls")
	fftls.InitTLSConfig(tlsConfig)
}

const (

	// DebugEnabled is whether to actually run the debug server or not
	DebugEnabled = "enabled"
)

func InitDebugConfig(conf config.Section) {
	InitHTTPConfig(conf, 0 /* auto assign */)
	conf.AddKnownKey(DebugEnabled, true /* on by default */)
}
