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
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/common/pkg/metric"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

const (
	metricsHTTPResponsesTotal = "http_responses_total"
	metricsNetworkErrorsTotal = "network_errors_total"
	metricsHTTPResponseTime   = "http_response_time_seconds"
)

type retryCtxKey struct{}
type hostCtxKey struct{}

type retryCtx struct {
	id       string
	start    time.Time
	attempts uint
}

type Config struct {
	URL string `json:"httpURL,omitempty"`
	HTTPConfig
}

var (
	metricsManager metric.MetricsManager
	onErrorHooks   []resty.ErrorHook
	onSuccessHooks []resty.SuccessHook
)

// HTTPConfig is all the optional configuration separate to the URL you wish to invoke.
// This is JSON serializable with docs, so you can embed it into API objects.
type HTTPConfig struct {
	ProxyURL                      string                                                 `ffstruct:"RESTConfig" json:"proxyURL,omitempty"`
	HTTPRequestTimeout            fftypes.FFDuration                                     `ffstruct:"RESTConfig" json:"requestTimeout,omitempty"`
	HTTPIdleConnTimeout           fftypes.FFDuration                                     `ffstruct:"RESTConfig" json:"idleTimeout,omitempty"`
	HTTPMaxIdleTimeout            fftypes.FFDuration                                     `ffstruct:"RESTConfig" json:"maxIdleTimeout,omitempty"`
	HTTPConnectionTimeout         fftypes.FFDuration                                     `ffstruct:"RESTConfig" json:"connectionTimeout,omitempty"`
	HTTPExpectContinueTimeout     fftypes.FFDuration                                     `ffstruct:"RESTConfig" json:"expectContinueTimeout,omitempty"`
	AuthUsername                  string                                                 `ffstruct:"RESTConfig" json:"authUsername,omitempty"`
	AuthPassword                  string                                                 `ffstruct:"RESTConfig" json:"authPassword,omitempty"`
	ThrottleRequestsPerSecond     int                                                    `ffstruct:"RESTConfig" json:"requestsPerSecond,omitempty"`
	ThrottleBurst                 int                                                    `ffstruct:"RESTConfig" json:"burst,omitempty"`
	Retry                         bool                                                   `ffstruct:"RESTConfig" json:"retry,omitempty"`
	RetryCount                    int                                                    `ffstruct:"RESTConfig" json:"retryCount,omitempty"`
	RetryInitialDelay             fftypes.FFDuration                                     `ffstruct:"RESTConfig" json:"retryInitialDelay,omitempty"`
	RetryMaximumDelay             fftypes.FFDuration                                     `ffstruct:"RESTConfig" json:"retryMaximumDelay,omitempty"`
	RetryErrorStatusCodeRegex     string                                                 `ffstruct:"RESTConfig" json:"retryErrorStatusCodeRegex,omitempty"`
	HTTPMaxIdleConns              int                                                    `ffstruct:"RESTConfig" json:"maxIdleConns,omitempty"`
	HTTPMaxConnsPerHost           int                                                    `ffstruct:"RESTConfig" json:"maxConnsPerHost,omitempty"`
	HTTPMaxIdleConnsPerHost       int                                                    `ffstruct:"RESTConfig" json:"maxIdleConnsPerHost,omitempty"`
	HTTPPassthroughHeadersEnabled bool                                                   `ffstruct:"RESTConfig" json:"httpPassthroughHeadersEnabled,omitempty"`
	HTTPHeaders                   fftypes.JSONObject                                     `ffstruct:"RESTConfig" json:"headers,omitempty"`
	HTTPTLSHandshakeTimeout       fftypes.FFDuration                                     `ffstruct:"RESTConfig" json:"tlsHandshakeTimeout,omitempty"`
	HTTPCustomClient              interface{}                                            `json:"-"`
	TLSClientConfig               *tls.Config                                            `json:"-"` // should be built from separate TLSConfig using fftls utils
	Resolver                      *net.Resolver                                          `json:"-"` // programmatic DNS resolver override; takes precedence over DNSServers
	DialControl                   func(network, address string, c syscall.RawConn) error `json:"-"` // SSRF CIDR-denylist guard applied to the dialer; built from the dns config via ffdns
	OnCheckRetry                  func(res *resty.Response, err error) bool              `json:"-"` // response could be nil on err
	OnBeforeRequest               func(req *resty.Request) error                         `json:"-"` // called before each request, even retry
}

func EnableClientMetrics(ctx context.Context, metricsRegistry metric.MetricsRegistry) error {
	// create a metrics manager (if not already set)
	if metricsManager == nil {
		mm, err := metricsRegistry.NewMetricsManagerForSubsystem(ctx, "resty")
		metricsManager = mm
		if err != nil {
			return err
		}
		metricsManager.NewCounterMetricWithLabels(ctx, metricsHTTPResponsesTotal, "HTTP response", []string{"status", "error", "host", "method"}, false)
		metricsManager.NewCounterMetricWithLabels(ctx, metricsNetworkErrorsTotal, "Network error", []string{"host", "method"}, false)
		metricsManager.NewSummaryMetricWithLabels(ctx, metricsHTTPResponseTime, "HTTP response time", []string{"status", "host", "method"}, false)
	}

	// create hooks
	onErrorMetricsHook := func(req *resty.Request, _ error) {
		// whilst there it is a possibility to get an response returned in the error here (and resty doc for OnError shows this) it seems to be a special case and the statuscode in such cases was not set.
		// therefore we log all cases as network_error we may in future find reason to extract more detail from the error
		metricsManager.IncCounterMetricWithLabels(ctx, metricsNetworkErrorsTotal, map[string]string{"host": req.Context().Value(hostCtxKey{}).(string), "method": req.Method}, nil)
	}
	RegisterGlobalOnError(onErrorMetricsHook)

	onSuccessMetricsHook := func(_ *resty.Client, resp *resty.Response) {
		metricsManager.IncCounterMetricWithLabels(ctx, metricsHTTPResponsesTotal, map[string]string{"status": fmt.Sprintf("%d", resp.RawResponse.StatusCode), "error": "false", "host": resp.Request.Context().Value(hostCtxKey{}).(string), "method": resp.Request.Method}, nil)
	}
	RegisterGlobalOnSuccess(onSuccessMetricsHook)
	return nil
}

func RegisterGlobalOnError(onError func(req *resty.Request, err error)) {
	onErrorHooks = append(onErrorHooks, onError)
}

func RegisterGlobalOnSuccess(onSuccess func(c *resty.Client, resp *resty.Response)) {
	onSuccessHooks = append(onSuccessHooks, onSuccess)
}

// OnAfterResponse when using SetDoNotParseResponse(true) for streaming binary replies,
// the caller should invoke ffresty.OnAfterResponse on the response manually.
// The middleware is disabled on this path :-(
// See: https://github.com/go-resty/resty/blob/d01e8d1bac5ba1fed0d9e03c4c47ca21e94a7e8e/client.go#L912-L948
func OnAfterResponse(c *resty.Client, resp *resty.Response) {
	if c == nil || resp == nil {
		return
	}
	rCtx := resp.Request.Context()
	rc := rCtx.Value(retryCtxKey{}).(*retryCtx)
	elapsed := time.Since(rc.start)
	level := logrus.DebugLevel
	status := resp.StatusCode()
	if status >= 300 {
		level = logrus.ErrorLevel
	}
	log.L(rCtx).Logf(level, "<== %s %s [%d] (%dms)", resp.Request.Method, resp.Request.URL, status, time.Since(rc.start).Milliseconds())
	if metricsManager != nil {
		metricsManager.ObserveSummaryMetricWithLabels(rCtx, metricsHTTPResponseTime, elapsed.Seconds(), map[string]string{"status": fmt.Sprintf("%d", status), "host": rCtx.Value(hostCtxKey{}).(string), "method": resp.Request.Method}, nil)
	}
	// TODO use req.TraceInfo() for richer metrics at the DNS and transport layer
}

func OnError(req *resty.Request, err error) {
	for _, hook := range onErrorHooks {
		hook(req, err)
	}
}

func OnSuccess(c *resty.Client, resp *resty.Response) {
	for _, hook := range onSuccessHooks {
		hook(c, resp)
	}
}

// New creates a new Resty client, using static configuration (from the config file)
// from a given section in the static configuration
//
// You can use the normal Resty builder pattern, to set per-instance configuration
// as required.
func New(ctx context.Context, staticConfig config.Section) (client *resty.Client, err error) {
	ffrestyConfig, err := GenerateConfig(ctx, staticConfig)
	if err != nil {
		return nil, err
	}

	return NewWithConfig(ctx, *ffrestyConfig), nil
}

var getRateLimiter = GetRateLimiter

func GetRateLimiter(rps, burst int) *rate.Limiter {
	if rps != 0 { // if rps is not set no need for a rate limiter
		rpsLimiter := rate.Limit(rps)
		if burst == 0 {
			burst = rps
		}
		return rate.NewLimiter(rpsLimiter, burst)
	}
	return nil
}

// New creates a new Resty client, using static configuration (from the config file)
// from a given section in the static configuration
//
// You can use the normal Resty builder pattern, to set per-instance configuration
// as required.
func NewWithConfig(ctx context.Context, ffrestyConfig Config) (client *resty.Client) { //nolint:gocyclo
	if ffrestyConfig.HTTPCustomClient != nil {
		if httpClient, ok := ffrestyConfig.HTTPCustomClient.(*http.Client); ok {
			client = resty.NewWithClient(httpClient)
		}
	}

	if client == nil {

		dialer := &net.Dialer{
			Timeout:   time.Duration(ffrestyConfig.HTTPConnectionTimeout),
			KeepAlive: time.Duration(ffrestyConfig.HTTPConnectionTimeout),
		}
		// An explicit programmatic resolver wins; otherwise build one from any configured DNS servers.
		// Either way the system resolver is replaced with Go's built-in resolver.
		if ffrestyConfig.Resolver != nil {
			dialer.Resolver = ffrestyConfig.Resolver
		}
		// CIDR-denylist guard for SSRF and/or high-trust, checked against the resolved IP just before connect.
		if ffrestyConfig.DialControl != nil {
			dialer.Control = ffrestyConfig.DialControl
		}

		httpTransport := &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           dialer.DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          ffrestyConfig.HTTPMaxIdleConns,
			MaxConnsPerHost:       ffrestyConfig.HTTPMaxConnsPerHost,
			MaxIdleConnsPerHost:   ffrestyConfig.HTTPMaxConnsPerHost,
			IdleConnTimeout:       time.Duration(ffrestyConfig.HTTPIdleConnTimeout),
			TLSHandshakeTimeout:   time.Duration(ffrestyConfig.HTTPTLSHandshakeTimeout),
			ExpectContinueTimeout: time.Duration(ffrestyConfig.HTTPExpectContinueTimeout),
		}

		if ffrestyConfig.TLSClientConfig != nil {
			httpTransport.TLSClientConfig = ffrestyConfig.TLSClientConfig
		}

		httpClient := &http.Client{
			Transport: httpTransport,
		}
		client = resty.NewWithClient(httpClient)
	}

	// this var is held in memory for the lifetime of the client bc the OnBeforeRequest func
	// uses it to wait for permission to proceed with the request
	rateLimiter := getRateLimiter(ffrestyConfig.ThrottleRequestsPerSecond, ffrestyConfig.ThrottleBurst)

	_url := strings.TrimSuffix(ffrestyConfig.URL, "/")
	if _url != "" {
		client.SetBaseURL(_url)
		log.L(ctx).Debugf("Created REST client to %s", _url)
	}

	if ffrestyConfig.ProxyURL != "" {
		client.SetProxy(ffrestyConfig.ProxyURL)
	}

	client.SetTimeout(time.Duration(ffrestyConfig.HTTPRequestTimeout))

	client.OnBeforeRequest(func(c *resty.Client, req *resty.Request) error {
		if rateLimiter != nil {
			// Wait for permission to proceed with the request
			err := rateLimiter.Wait(req.Context())
			if err != nil {
				return err
			}
		}
		rCtx := req.Context()
		// Record host in context to avoid redundant parses in hooks
		var u *url.URL
		if req.URL != "" {
			u, _ = url.Parse(req.URL)
		}
		// The req.URL might have only set a path i.e. /home, fallbacking to the base URL of the client.
		// So if the URL is nil, that's likely the case and we'll derive the host from the configured
		// base instead.
		if (u == nil || u.Host == "") && _url != "" {
			u, _ = url.Parse(_url)
		}
		if (u == nil || u.Host == "") && c.BaseURL != "" {
			u, _ = url.Parse(c.BaseURL)
		}
		if u != nil && u.Host != "" {
			host := u.Host
			rCtx = context.WithValue(rCtx, hostCtxKey{}, host)
		} else {
			rCtx = context.WithValue(rCtx, hostCtxKey{}, "unknown")
		}
		rc := rCtx.Value(retryCtxKey{})
		if rc == nil {
			// First attempt
			r := &retryCtx{
				id:    fftypes.ShortID(),
				start: time.Now(),
			}
			rCtx = context.WithValue(rCtx, retryCtxKey{}, r)
			// Create a request logger from the root logger passed into the client
			rCtx = log.WithLogFields(rCtx, "breq", r.id)
			req.SetContext(rCtx)
		}

		// If passthroughHeaders: true for this rest client, pass any of the allowed headers on the original req
		if ffrestyConfig.HTTPPassthroughHeadersEnabled {
			ctxHeaders := rCtx.Value(ffapi.CtxHeadersKey{})
			if ctxHeaders != nil {
				passthroughHeaders := ctxHeaders.(http.Header)
				for key := range passthroughHeaders {
					req.Header.Set(key, passthroughHeaders.Get(key))
				}
			}
		}

		// If an X-FireFlyRequestID was set on the context, pass that header on this request too
		ffRequestID := rCtx.Value(ffapi.CtxFFRequestIDKey{})
		if ffRequestID != nil {
			req.Header.Set(ffapi.RequestIDHeader(), ffRequestID.(string))
		}

		if ffrestyConfig.OnBeforeRequest != nil {
			if err := ffrestyConfig.OnBeforeRequest(req); err != nil {
				return err
			}
		}

		log.L(rCtx).Debugf("==> %s %s%s", req.Method, _url, req.URL)
		if logrus.IsLevelEnabled(logrus.TraceLevel) {
			log.L(rCtx).Tracef("==> (body) %s", traceBody(req.Body))
		}

		return nil
	})

	// Note that callers using SetNotParseResponse will need to invoke this themselves
	client.OnAfterResponse(func(c *resty.Client, r *resty.Response) error { OnAfterResponse(c, r); return nil })

	client.OnError(func(req *resty.Request, e error) { OnError(req, e) })
	client.OnSuccess(func(c *resty.Client, r *resty.Response) { OnSuccess(c, r) })

	for k, v := range ffrestyConfig.HTTPHeaders {
		if vs, ok := v.(string); ok {
			client.SetHeader(k, vs)
		}
	}
	if ffrestyConfig.AuthUsername != "" && ffrestyConfig.AuthPassword != "" {
		client.SetHeader("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", ffrestyConfig.AuthUsername, ffrestyConfig.AuthPassword)))))
	}

	var retryStatusCodeRegex *regexp.Regexp
	if ffrestyConfig.RetryErrorStatusCodeRegex != "" {
		retryStatusCodeRegex = regexp.MustCompile(ffrestyConfig.RetryErrorStatusCodeRegex)
	}

	if ffrestyConfig.Retry {
		retryCount := ffrestyConfig.RetryCount
		minTimeout := time.Duration(ffrestyConfig.RetryInitialDelay)
		maxTimeout := time.Duration(ffrestyConfig.RetryMaximumDelay)
		client.
			SetRetryCount(retryCount).
			SetRetryWaitTime(minTimeout).
			SetRetryMaxWaitTime(maxTimeout).
			AddRetryCondition(func(r *resty.Response, err error) bool {
				if r == nil || r.IsSuccess() {
					return false
				}

				if r.StatusCode() > 0 && retryStatusCodeRegex != nil && !retryStatusCodeRegex.MatchString(r.Status()) {
					// the error status code doesn't match the retry status code regex, stop retry
					return false
				}

				rCtx := r.Request.Context()
				rc := rCtx.Value(retryCtxKey{}).(*retryCtx)
				if ffrestyConfig.OnCheckRetry != nil && !ffrestyConfig.OnCheckRetry(r, err) {
					log.L(rCtx).Debugf("retry cancelled after %d attempts", rc.attempts)
					return false
				}
				rc.attempts++
				log.L(rCtx).Tracef("retry %d/%d (min=%dms/max=%dms) status=%d", rc.attempts, retryCount, minTimeout.Milliseconds(), maxTimeout.Milliseconds(), r.StatusCode())
				return true
			})
	}

	return client
}

func traceBody(v any) string {
	switch vt := v.(type) {
	case string:
		return vt
	case []byte:
		return string(vt)
	case nil:
		return ""
	default:
		_, isReader := v.(io.Reader)
		if isReader {
			return "(binary reader)" // Future work could create a pipe to log the chunks if that were required
		}
		jv, _ := json.Marshal(v)
		return string(jv)
	}
}

func WrapRestErr(ctx context.Context, res *resty.Response, err error, key i18n.ErrorMessageKey) error {
	var respData string
	if res != nil {
		if res.RawBody() != nil {
			defer func() { _ = res.RawBody().Close() }()
			if r, err := io.ReadAll(res.RawBody()); err == nil {
				respData = string(r)
			}
		}
		if respData == "" {
			respData = res.String()
		}
		if len(respData) > 256 {
			respData = respData[0:256] + "..."
		}
	}
	if err != nil {
		return i18n.WrapError(ctx, err, key, respData)
	}
	return i18n.NewError(ctx, key, respData)
}
