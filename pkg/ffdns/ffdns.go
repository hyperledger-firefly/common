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
	"errors"
	"net"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/metric"
)

const (
	metricsDNSRequestsTotal  = "dns_requests_total"
	metricsDNSResponsesTotal = "dns_responses_total"
	metricsDNSErrorsTotal    = "dns_errors_total"
)

var metricsManager metric.MetricsManager

func EnableResolverMetrics(ctx context.Context, metricsRegistry metric.MetricsRegistry) {
	if metricsManager != nil {
		return
	}
	metricsManager, _ = metricsRegistry.NewMetricsManagerForSubsystem(ctx, "dns")
	metricsManager.NewCounterMetricWithLabels(ctx, metricsDNSRequestsTotal, "DNS requests", []string{"server"}, false)
	metricsManager.NewCounterMetricWithLabels(ctx, metricsDNSResponsesTotal, "DNS responses", []string{"server", "status"}, false)
	metricsManager.NewCounterMetricWithLabels(ctx, metricsDNSErrorsTotal, "DNS errors", []string{"server", "error"}, false)
}

// NewDNSResolver builds a pure-Go *net.Resolver for metrics instructmentation, custom timeouts, and/or custom servers.
// The resolver will dial the given DNS servers (each host or host:port, port defaulting to 53) in order, failing over to the
// next on error. Returns nil if none of the customizations (metrics, timeout, or servers) are enabeld.
// Exported so non-ffresty dialers — e.g. a WebSocket dialer — can honour the same
// DNS config as the HTTP client.
func NewResolver(config config.Section) *net.Resolver {
	cfg, err := GenerateConfig(config)
	if err != nil {
		return nil
	}

	return NewResolverWithConfig(cfg)
}

func NewResolverWithConfig(cfg *Config) *net.Resolver {
	var servers []string
	if len(cfg.Servers) > 0 {
		servers = make([]string, len(cfg.Servers))
		for i, server := range cfg.Servers {
			servers[i] = withDefaultDNSPort(server)
		}
	}

	// If we have nothing to layer on top of the system resolver — no configured servers, no
	// dial timeout, and metrics disabled — leave it untouched (callers treat nil as "use the
	// system resolver"). Returning a resolver here would force Go's built-in resolver
	// (PreferGo) in deployments that haven't opted into any of these.
	if len(servers) == 0 && cfg.Timeout <= 0 && metricsManager == nil {
		return nil
	}

	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := net.Dialer{Timeout: cfg.Timeout}
			// When no servers are explicitly configured, wrap Go's built-in resolver: it has
			// already selected a nameserver from the system config (resolv.conf) and passes it
			// as address, so we dial that and still apply our timeout and metrics.
			dialServers := servers
			if len(dialServers) == 0 {
				dialServers = []string{address}
			}
			var err error
			// Go's built-in resolver dials a fresh connection per query exchange (escalating
			// from UDP to TCP for truncated responses), so each Dial maps to a DNS request. We
			// record metrics at this connection level.
			for _, server := range dialServers {
				recordDNSMetric(ctx, metricsDNSRequestsTotal, map[string]string{"server": server})
				var conn net.Conn
				if conn, err = d.DialContext(ctx, network, server); err == nil {
					recordDNSMetric(ctx, metricsDNSResponsesTotal, map[string]string{"server": server, "status": "success"})
					return conn, nil
				}
				recordDNSMetric(ctx, metricsDNSErrorsTotal, map[string]string{"server": server, "error": classifyDNSError(err)})
			}
			return nil, err
		},
	}
}

// recordDNSMetric increments a DNS counter when resolver metrics have been enabled, and is a no-op otherwise.
func recordDNSMetric(ctx context.Context, name string, labels map[string]string) {
	if metricsManager == nil {
		return
	}
	metricsManager.IncCounterMetricWithLabels(ctx, name, labels, nil)
}

// classifyDNSError maps a dial error to a low-cardinality label so the dns_errors_total metric doesn't explode.
func classifyDNSError(err error) string {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	return "error"
}

// withDefaultDNSPort ensures a DNS server address has a port, defaulting to 53.
func withDefaultDNSPort(server string) string {
	if _, _, err := net.SplitHostPort(server); err == nil {
		return server
	}
	return net.JoinHostPort(server, "53")
}
