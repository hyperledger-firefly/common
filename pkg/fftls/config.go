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

package fftls

import (
	"github.com/hyperledger-firefly/common/pkg/config"
)

const (
	// HTTPConfTLSCAFile the TLS certificate authority file for the HTTP server
	HTTPConfTLSCAFile = "caFile"
	// HTTPConfTLSCA the TLS certificate authority in PEM format, this option is ignored if HTTPConfTLSCAFile is also set
	HTTPConfTLSCA = "ca"
	// HTTPConfTLSCertFile the TLS certificate file for the HTTP server
	HTTPConfTLSCertFile = "certFile"
	// HTTPConfTLSCert the TLS certificate in PEM format, this option is ignored if HTTPConfTLSCertFile is also set
	HTTPConfTLSCert = "cert"
	// HTTPConfTLSClientAuth whether the HTTP server requires a mutual TLS connection
	HTTPConfTLSClientAuth = "clientAuth"
	// HTTPConfTLSEnabled whether TLS is enabled for the HTTP server
	HTTPConfTLSEnabled = "enabled"
	// HTTPConfTLSKeyFile the private key file for TLS on the server
	HTTPConfTLSKeyFile = "keyFile"
	// HTTPConfTLSKey the TLS certificate key in PEM format, this option is ignored if HTTPConfTLSKeyFile is also set
	HTTPConfTLSKey = "key"

	// HTTPConfTLSInsecureSkipHostVerify disables host verification - insecure (for dev only)
	HTTPConfTLSInsecureSkipHostVerify = "insecureSkipHostVerify"

	// HTTPConfTLSRequiredDNAttributes provides a set of regular expressions, to match against the DN of the client. Requires HTTPConfTLSClientAuth
	HTTPConfTLSRequiredDNAttributes = "requiredDNAttributes"

	// HTTPConfTLSIncludeSystemCAs when true and a custom CA (caFile/ca) is configured, merges that CA
	// into the system certificate pool instead of replacing RootCAs. Useful for outbound HTTP clients
	// that talk to a private CA endpoint and may follow redirects to publicly trusted hosts (e.g. S3).
	HTTPConfTLSIncludeSystemCAs = "includeSystemCAs"

	defaultHTTPTLSEnabled          = false
	defaultHTTPTLSIncludeSystemCAs = false
)

type Config struct {
	Enabled                bool                   `ffstruct:"tlsconfig" json:"enabled"`
	ClientAuth             bool                   `ffstruct:"tlsconfig" json:"clientAuth,omitempty"`
	CAFile                 string                 `ffstruct:"tlsconfig" json:"caFile,omitempty"`
	CA                     string                 `ffstruct:"tlsconfig" json:"ca,omitempty"`
	CertFile               string                 `ffstruct:"tlsconfig" json:"certFile,omitempty"`
	Cert                   string                 `ffstruct:"tlsconfig" json:"cert,omitempty"`
	KeyFile                string                 `ffstruct:"tlsconfig" json:"keyFile,omitempty"`
	Key                    string                 `ffstruct:"tlsconfig" json:"key,omitempty"`
	InsecureSkipHostVerify bool                   `ffstruct:"tlsconfig" json:"insecureSkipHostVerify"`
	IncludeSystemCAs       bool                   `ffstruct:"tlsconfig" json:"includeSystemCAs,omitempty"`
	RequiredDNAttributes   map[string]interface{} `ffstruct:"tlsconfig" json:"requiredDNAttributes,omitempty"`
}

func InitTLSConfig(conf config.Section) {
	conf.AddKnownKey(HTTPConfTLSEnabled, defaultHTTPTLSEnabled)
	conf.AddKnownKey(HTTPConfTLSCAFile)
	conf.AddKnownKey(HTTPConfTLSCA)
	conf.AddKnownKey(HTTPConfTLSClientAuth)
	conf.AddKnownKey(HTTPConfTLSCertFile)
	conf.AddKnownKey(HTTPConfTLSCert)
	conf.AddKnownKey(HTTPConfTLSKeyFile)
	conf.AddKnownKey(HTTPConfTLSKey)
	conf.AddKnownKey(HTTPConfTLSRequiredDNAttributes)
	conf.AddKnownKey(HTTPConfTLSInsecureSkipHostVerify)
	conf.AddKnownKey(HTTPConfTLSIncludeSystemCAs, defaultHTTPTLSIncludeSystemCAs)
}

func GenerateConfig(conf config.Section) *Config {
	return &Config{
		Enabled:                conf.GetBool(HTTPConfTLSEnabled),
		ClientAuth:             conf.GetBool(HTTPConfTLSClientAuth),
		CAFile:                 conf.GetString(HTTPConfTLSCAFile),
		CA:                     conf.GetString(HTTPConfTLSCA),
		CertFile:               conf.GetString(HTTPConfTLSCertFile),
		Cert:                   conf.GetString(HTTPConfTLSCert),
		KeyFile:                conf.GetString(HTTPConfTLSKeyFile),
		Key:                    conf.GetString(HTTPConfTLSKey),
		InsecureSkipHostVerify: conf.GetBool(HTTPConfTLSInsecureSkipHostVerify),
		IncludeSystemCAs:       conf.GetBool(HTTPConfTLSIncludeSystemCAs),
		RequiredDNAttributes:   conf.GetObject(HTTPConfTLSRequiredDNAttributes),
	}
}
