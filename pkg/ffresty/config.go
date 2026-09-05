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

package ffresty

import (
	"context"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/ffdns"
	"github.com/hyperledger-firefly/common/pkg/ffnet"
	"github.com/hyperledger-firefly/common/pkg/fftls"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
)

const (
	defaultRetryEnabled                  = false
	defaultRetryCount                    = 5
	defaultRetryWaitTime                 = "250ms"
	defaultRetryMaxWaitTime              = "30s"
	defaultRequestTimeout                = "30s"
	defaultHTTPIdleTimeout               = "4750ms" // Node.js default keepAliveTimeout is 5 seconds, so we have to set a base below this (was a 475ms typo for years)
	defaultHTTPMaxIdleConns              = 100     // match Go's default
	defaultHTTPMaxConnsPerHost           = 0       // unlimited
	defaultHTTPMaxIdleConnsPerHost       = 100     // avoid Go's conservative default of 2, we'd rather it be the same as the maxIdleConns so a single host can use all the connections
	defaultHTTPConnectionTimeout         = "30s"
	defaultHTTPTLSHandshakeTimeout       = "10s" // match Go's default
	defaultHTTPExpectContinueTimeout     = "1s"  // match Go's default
	defaultHTTPPassthroughHeadersEnabled = false
)

const (
	// HTTPConfigURL is the url to connect to for this HTTP configuration
	HTTPConfigURL = "url"
	// HTTPConfigProxyURL adds a proxy
	HTTPConfigProxyURL = "proxy.url"
	// HTTPConfigHeaders adds custom headers to the requests
	HTTPConfigHeaders = "headers"
	// HTTPConfigAuthUsername HTTPS Basic Auth configuration - username
	HTTPConfigAuthUsername = "auth.username"
	// HTTPConfigAuthPassword HTTPS Basic Auth configuration - secret / password
	HTTPConfigAuthPassword = "auth.password"
	// HTTPConfigRetryEnabled whether retry is enabled on the actions performed over this HTTP request (does not disable retry at higher layers)
	HTTPConfigRetryEnabled = "retry.enabled"
	// HTTPConfigRetryCount the maximum number of retries
	HTTPConfigRetryCount = "retry.count"
	// HTTPConfigRetryInitDelay the initial retry delay
	HTTPConfigRetryInitDelay = "retry.initWaitTime"
	// HTTPConfigRetryMaxDelay the maximum retry delay
	HTTPConfigRetryMaxDelay = "retry.maxWaitTime"
	// HTTPConfigRetryErrorStatusCodeRegex the regex that the error response status code must match to trigger retry
	HTTPConfigRetryErrorStatusCodeRegex = "retry.errorStatusCodeRegex"

	// HTTPConfigRequestTimeout the request timeout
	HTTPConfigRequestTimeout = "requestTimeout"
	// HTTPIdleTimeout the max duration to hold a HTTP keepalive connection between calls
	HTTPIdleTimeout = "idleTimeout"
	// HTTPMaxIdleConns the max number of idle connections to hold pooled
	HTTPMaxIdleConns = "maxIdleConns"
	// HTTPThrottleRequestsPerSecond The average rate at which requests are allowed to pass through over time. Default to RPS
	// requests over the limit will be blocked using a buffered channel
	// the blocked time period is not counted in request timeout
	HTTPThrottleRequestsPerSecond = "throttle.requestsPerSecond"

	// HTTPThrottleBurst The maximum number of requests that can be made in a short period of time before the RPS throttling kicks in.
	HTTPThrottleBurst = "throttle.burst"

	// HTTPMaxConnsPerHost the max number of concurrent connections per host
	HTTPMaxConnsPerHost = "maxConnsPerHost"
	// HTTPMaxIdleConnsPerHost the max number of idle connections per host
	HTTPMaxIdleConnsPerHost = "maxIdleConnsPerHost"

	// HTTPConnectionTimeout the connection timeout for new connections
	HTTPConnectionTimeout = "connectionTimeout"
	// HTTPTLSHandshakeTimeout the TLS handshake connection timeout
	HTTPTLSHandshakeTimeout = "tlsHandshakeTimeout"
	// HTTPExpectContinueTimeout see ExpectContinueTimeout in Go docs
	HTTPExpectContinueTimeout = "expectContinueTimeout"
	// HTTPPassthroughHeadersEnabled will pass through any HTTP headers found on the context
	HTTPPassthroughHeadersEnabled = "passthroughHeadersEnabled"

	// HTTPCustomClient - unit test only - allows injection of a custom HTTP client to resty
	HTTPCustomClient = "customClient"
)

func InitConfig(conf config.Section) {
	conf.AddKnownKey(HTTPConfigURL)
	conf.AddKnownKey(HTTPConfigProxyURL)
	conf.AddKnownKey(HTTPConfigHeaders)
	conf.AddKnownKey(HTTPConfigAuthUsername)
	conf.AddKnownKey(HTTPConfigAuthPassword)
	conf.AddKnownKey(HTTPConfigRetryEnabled, defaultRetryEnabled)
	conf.AddKnownKey(HTTPConfigRetryCount, defaultRetryCount)
	conf.AddKnownKey(HTTPConfigRetryInitDelay, defaultRetryWaitTime)
	conf.AddKnownKey(HTTPConfigRetryMaxDelay, defaultRetryMaxWaitTime)
	conf.AddKnownKey(HTTPConfigRetryErrorStatusCodeRegex)
	conf.AddKnownKey(HTTPConfigRequestTimeout, defaultRequestTimeout)
	conf.AddKnownKey(HTTPThrottleRequestsPerSecond)
	conf.AddKnownKey(HTTPThrottleBurst)
	conf.AddKnownKey(HTTPIdleTimeout, defaultHTTPIdleTimeout)
	conf.AddKnownKey(HTTPMaxIdleConns, defaultHTTPMaxIdleConns)
	conf.AddKnownKey(HTTPMaxConnsPerHost, defaultHTTPMaxConnsPerHost)
	conf.AddKnownKey(HTTPMaxIdleConnsPerHost, defaultHTTPMaxIdleConnsPerHost)
	conf.AddKnownKey(HTTPConnectionTimeout, defaultHTTPConnectionTimeout)
	conf.AddKnownKey(HTTPTLSHandshakeTimeout, defaultHTTPTLSHandshakeTimeout)
	conf.AddKnownKey(HTTPExpectContinueTimeout, defaultHTTPExpectContinueTimeout)
	conf.AddKnownKey(HTTPPassthroughHeadersEnabled, defaultHTTPPassthroughHeadersEnabled)
	conf.AddKnownKey(HTTPCustomClient)

	tlsConfig := conf.SubSection("tls")
	fftls.InitTLSConfig(tlsConfig)

	dnsConfig := conf.SubSection("dns")
	ffdns.InitConfig(dnsConfig)

	netConfig := conf.SubSection("net")
	ffnet.InitConfig(netConfig)
}

func GenerateConfig(ctx context.Context, conf config.Section) (*Config, error) {
	ffrestyConfig := &Config{
		URL: conf.GetString(HTTPConfigURL),
		HTTPConfig: HTTPConfig{
			ProxyURL:                      conf.GetString(HTTPConfigProxyURL),
			HTTPHeaders:                   conf.GetObject(HTTPConfigHeaders),
			AuthUsername:                  conf.GetString(HTTPConfigAuthUsername),
			AuthPassword:                  conf.GetString(HTTPConfigAuthPassword),
			Retry:                         conf.GetBool(HTTPConfigRetryEnabled),
			RetryCount:                    conf.GetInt(HTTPConfigRetryCount),
			RetryInitialDelay:             fftypes.FFDuration(conf.GetDuration(HTTPConfigRetryInitDelay)),
			RetryMaximumDelay:             fftypes.FFDuration(conf.GetDuration(HTTPConfigRetryMaxDelay)),
			RetryErrorStatusCodeRegex:     conf.GetString(HTTPConfigRetryErrorStatusCodeRegex),
			ThrottleRequestsPerSecond:     conf.GetInt(HTTPThrottleRequestsPerSecond),
			ThrottleBurst:                 conf.GetInt(HTTPThrottleBurst),
			HTTPRequestTimeout:            fftypes.FFDuration(conf.GetDuration(HTTPConfigRequestTimeout)),
			HTTPIdleConnTimeout:           fftypes.FFDuration(conf.GetDuration(HTTPIdleTimeout)),
			HTTPMaxIdleConns:              conf.GetInt(HTTPMaxIdleConns),
			HTTPMaxConnsPerHost:           conf.GetInt(HTTPMaxConnsPerHost),
			HTTPMaxIdleConnsPerHost:       conf.GetInt(HTTPMaxIdleConnsPerHost),
			HTTPConnectionTimeout:         fftypes.FFDuration(conf.GetDuration(HTTPConnectionTimeout)),
			HTTPTLSHandshakeTimeout:       fftypes.FFDuration(conf.GetDuration(HTTPTLSHandshakeTimeout)),
			HTTPExpectContinueTimeout:     fftypes.FFDuration(conf.GetDuration(HTTPExpectContinueTimeout)),
			HTTPPassthroughHeadersEnabled: conf.GetBool(HTTPPassthroughHeadersEnabled),
			HTTPCustomClient:              conf.Get(HTTPCustomClient),
		},
	}
	tlsSection := conf.SubSection("tls")
	tlsClientConfig, err := fftls.ConstructTLSConfig(ctx, tlsSection, fftls.ClientType)
	if err != nil {
		return nil, err
	}

	ffrestyConfig.TLSClientConfig = tlsClientConfig

	dnsCfg, err := ffdns.GenerateConfig(conf.SubSection("dns"))
	if err != nil {
		return nil, err
	}
	ffrestyConfig.Resolver = ffdns.NewResolverWithConfig(dnsCfg)

	netCfg, err := ffnet.GenerateConfig(conf.SubSection("net"))
	if err != nil {
		return nil, err
	}
	dialControl, err := ffnet.NewDialControl(ctx, netCfg)
	if err != nil {
		return nil, err
	}
	ffrestyConfig.DialControl = dialControl

	return ffrestyConfig, nil
}
