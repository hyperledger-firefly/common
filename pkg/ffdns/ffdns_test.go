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

package ffdns

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/metric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// counterTotal sums the values of all series of a counter whose metric family name ends with
// the given suffix (the registry prefixes names with component + subsystem).
func counterTotal(t *testing.T, mr metric.MetricsRegistry, nameSuffix string) float64 {
	families, err := mr.GetGatherer().Gather()
	require.NoError(t, err)
	var total float64
	for _, mf := range families {
		if strings.HasSuffix(mf.GetName(), nameSuffix) {
			for _, m := range mf.GetMetric() {
				if c := m.GetCounter(); c != nil {
					total += c.GetValue()
				}
			}
		}
	}
	return total
}

var utConf = config.RootSection("dns_unit_tests")

func resetConf() {
	config.RootConfigReset()
	InitConfig(utConf)
}

func TestWithDefaultDNSPort(t *testing.T) {
	assert.Equal(t, "8.8.8.8:53", withDefaultDNSPort("8.8.8.8"))
	assert.Equal(t, "8.8.8.8:5353", withDefaultDNSPort("8.8.8.8:5353"))
	assert.Equal(t, "[2001:db8::1]:53", withDefaultDNSPort("2001:db8::1"))
	assert.Equal(t, "[2001:db8::1]:5353", withDefaultDNSPort("[2001:db8::1]:5353"))
}

func TestNewResolverWithConfig(t *testing.T) {
	// No servers -> nil, leaving Go's default system resolver selection in place
	assert.Nil(t, NewResolverWithConfig(&Config{}))

	// Servers configured -> pure-Go resolver
	r := NewResolverWithConfig(&Config{Servers: []string{"8.8.8.8"}})
	require.NotNil(t, r)
	assert.True(t, r.PreferGo)
	assert.NotNil(t, r.Dial)
}

func TestConfigKeysDocumented(t *testing.T) {
	// Initialize the DNS config as a subsection (as ffresty/wsclient do), then generate the
	// config markdown for every known key. This panics if any key is missing a translation,
	// guarding against the "Translation for config key '...dns.servers' was not found" regression.
	config.RootConfigReset()
	InitConfig(config.RootSection("backend").SubSection("dns"))

	assert.NotPanics(t, func() {
		_, err := config.GenerateConfigMarkdown(context.Background(), "", config.GetKnownKeys())
		assert.NoError(t, err)
	})
}

func TestNewResolverFromConfigSection(t *testing.T) {
	resetConf()
	utConf.Set(DNSServers, []string{"8.8.8.8", "1.1.1.1:53"})
	r := NewResolver(utConf)
	require.NotNil(t, r)
	assert.True(t, r.PreferGo)

	resetConf()
	assert.Nil(t, NewResolver(utConf))
}

func TestResolverDialFailover(t *testing.T) {
	// Stand up a listener acting as the "good" DNS server
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	accepted := make(chan struct{}, 1)
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr == nil {
			accepted <- struct{}{}
			_ = conn.Close()
		}
	}()

	// First server is unroutable so the dialer must fail over to the live listener
	r := NewResolverWithConfig(&Config{
		Timeout: 5 * time.Second,
		Servers: []string{"127.0.0.1:1", ln.Addr().String()},
	})
	require.NotNil(t, r)

	conn, err := r.Dial(context.Background(), "tcp", "ignored:53")
	require.NoError(t, err)
	defer conn.Close()
	assert.Equal(t, ln.Addr().String(), conn.RemoteAddr().String())

	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("DNS dial did not reach the configured server")
	}
}

func TestResolverDialAllFail(t *testing.T) {
	r := NewResolverWithConfig(&Config{
		Timeout: 250 * time.Millisecond,
		Servers: []string{"127.0.0.1:1"},
	})
	require.NotNil(t, r)
	_, err := r.Dial(context.Background(), "tcp", "ignored:53")
	assert.Error(t, err)
}

func TestEnableResolverMetrics(t *testing.T) {
	metricsManager = nil
	defer func() { metricsManager = nil }()

	ctx := context.Background()
	mr := metric.NewPrometheusMetricsRegistry("test")
	EnableResolverMetrics(ctx, mr)
	require.NotNil(t, metricsManager)

	// Idempotent - a second call is a no-op rather than re-registering
	EnableResolverMetrics(ctx, mr)
}

func TestResolverDialRecordsMetrics(t *testing.T) {
	metricsManager = nil
	defer func() { metricsManager = nil }()

	ctx := context.Background()
	mr := metric.NewPrometheusMetricsRegistry("test")
	EnableResolverMetrics(ctx, mr)

	// Live listener acts as the second (good) DNS server; the first is unroutable so a single
	// Dial exercises the request, error (failover), and response metric paths together.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go func() {
		if conn, acceptErr := ln.Accept(); acceptErr == nil {
			_ = conn.Close()
		}
	}()

	r := NewResolverWithConfig(&Config{
		Timeout: 5 * time.Second,
		Servers: []string{"127.0.0.1:1", ln.Addr().String()},
	})
	require.NotNil(t, r)
	conn, err := r.Dial(ctx, "tcp", "ignored:53")
	require.NoError(t, err)
	defer conn.Close()

	assert.GreaterOrEqual(t, counterTotal(t, mr, "dns_requests_total"), float64(2), "one request per server attempted")
	assert.GreaterOrEqual(t, counterTotal(t, mr, "dns_responses_total"), float64(1), "one successful response")
	assert.GreaterOrEqual(t, counterTotal(t, mr, "dns_errors_total"), float64(1), "first server failed over")
}

func TestResolverDialNoMetricsWhenDisabled(t *testing.T) {
	metricsManager = nil // metrics not enabled -> recording is a no-op, no panic
	r := NewResolverWithConfig(&Config{
		Timeout: 250 * time.Millisecond,
		Servers: []string{"127.0.0.1:1"},
	})
	require.NotNil(t, r)
	_, err := r.Dial(context.Background(), "tcp", "ignored:53")
	assert.Error(t, err)
}

func TestClassifyDNSError(t *testing.T) {
	assert.Equal(t, "error", classifyDNSError(assertAnErr{}))
	assert.Equal(t, "timeout", classifyDNSError(timeoutErr{}))
}

type assertAnErr struct{}

func (assertAnErr) Error() string { return "boom" }

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }
