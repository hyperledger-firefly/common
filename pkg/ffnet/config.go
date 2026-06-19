// Copyright © 2025 Kaleido, Inc.
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

// Package ffnet builds outbound net.Dialers and their egress controls: a custom DNS resolver
// (via ffdns) plus a CIDR egress denylist for SSRF protection. It is the single place to
// configure how — and where — a client is allowed to make outbound connections.
package ffnet

import (
	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/ffdns"
)

const (
	// CIDRDenylist fully overrides the built-in default denylist of reserved/metadata ranges
	// blocked to mitigate SSRF. Leave unset to keep the secure defaults. Set to an empty list
	// to disable denylisting entirely (e.g. to allow a localhost target).
	CIDRDenylist = "cidrDenylist"
	// AdditionalDeniedCIDRs appends extra CIDRs on top of the effective base denylist (either
	// the built-in defaults or a configured cidrDenylist override).
	AdditionalDeniedCIDRs = "additionalDeniedCIDRs"
)

// DefaultDeniedCIDRs are blocked by default to mitigate SSRF: loopback, link-local (including
// the cloud metadata endpoint 169.254.169.254 and the AWS IMDS IPv6 endpoint), unspecified /
// "this host", and multicast / reserved / broadcast ranges. Private RFC1918 / IPv6 ULA ranges
// are intentionally NOT included — these dialers are commonly used for legitimate internal
// service-to-service calls, so blocking private space is deferred to network firewalls /
// zero-trust rather than baked in (callers wanting that can use additionalDeniedCIDRs).
var DefaultDeniedCIDRs = []string{
	"0.0.0.0/8",         // unspecified / "this host" (RFC 1122)
	"127.0.0.0/8",       // IPv4 loopback
	"169.254.0.0/16",    // IPv4 link-local, incl. cloud metadata 169.254.169.254
	"224.0.0.0/4",       // IPv4 multicast
	"240.0.0.0/4",       // IPv4 reserved (incl. 255.255.255.255 broadcast)
	"::1/128",           // IPv6 loopback
	"::/128",            // IPv6 unspecified
	"fe80::/10",         // IPv6 link-local
	"fd00:ec2::254/128", // AWS IMDS IPv6 endpoint (cloud metadata)
	"ff00::/8",          // IPv6 multicast
}

// Config is the combined outbound-dialer configuration: the DNS resolver settings plus the
// egress CIDR denylist.
type Config struct {
	DNS ffdns.Config
	// CIDRDenylist, when non-nil, fully replaces DefaultDeniedCIDRs (an empty non-nil slice
	// disables denylisting entirely). Leave nil to keep the secure defaults.
	CIDRDenylist []string
	// AdditionalDeniedCIDRs is appended on top of the effective base denylist.
	AdditionalDeniedCIDRs []string
}

func InitConfig(conf config.Section) {
	ffdns.InitConfig(conf)
	conf.AddKnownKey(CIDRDenylist)
	conf.AddKnownKey(AdditionalDeniedCIDRs)
}

func GenerateConfig(conf config.Section) (*Config, error) {
	dnsCfg, err := ffdns.GenerateConfig(conf)
	if err != nil {
		return nil, err
	}
	cfg := &Config{
		DNS:                   *dnsCfg,
		AdditionalDeniedCIDRs: conf.GetStringSlice(AdditionalDeniedCIDRs),
	}
	// Distinguish "unset" (keep secure defaults) from an explicit override (including an
	// empty list, which disables denylisting).
	if conf.IsSet(CIDRDenylist) {
		cfg.CIDRDenylist = conf.GetStringSlice(CIDRDenylist)
	}
	return cfg, nil
}
