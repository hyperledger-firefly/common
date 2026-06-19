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
	"slices"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/ffdns"
)

const (
	// CIDRDenylist is the list of CIDR ranges to which outbound connections are blocked, as a
	// core SSRF mitigation. It is empty by default — ffnet/ffresty is frequently used for private
	// service-to-service traffic, so we do not presume which ranges are off-limits. Callers should
	// compose an appropriate denylist from the exported building-block lists below (e.g.
	// RecommendedSSRFDenylist for externally-reachable/webhook clients, or a narrower set such as
	// CloudMetadataCIDRs for internal clients that still want to block IMDS).
	CIDRDenylist = "cidrDenylist"
)

// Exported building-block CIDR lists, grouped by category so callers can concatenate exactly the
// protection they need (see slices.Concat). Each is a distinct, non-overlapping category; the
// metadata endpoints are also called out separately for callers who want only those.
var (
	// IPv4Unspecified is the "this host on this network" range 0.0.0.0/8 (RFC 1122). 0.0.0.0
	// itself frequently resolves to localhost when dialed.
	IPv4Unspecified = []string{"0.0.0.0/8"}
	// IPv4Loopback is the IPv4 loopback range 127.0.0.0/8 (RFC 1122).
	IPv4Loopback = []string{"127.0.0.0/8"}
	// IPv4LinkLocal is the IPv4 link-local range 169.254.0.0/16 (RFC 3927). It contains the
	// cloud metadata endpoint 169.254.169.254 (see CloudMetadataCIDRs).
	IPv4LinkLocal = []string{"169.254.0.0/16"}
	// IPv4Private are the RFC 1918 private ranges.
	IPv4Private = []string{
		"10.0.0.0/8",

		"172.16.0.0/12",
		"192.168.0.0/16",
	}
	// IPv4CGNAT is the carrier-grade NAT / shared address space 100.64.0.0/10 (RFC 6598).
	IPv4CGNAT = []string{"100.64.0.0/10"}
	// IPv4Multicast is the IPv4 multicast range 224.0.0.0/4 (RFC 5771).
	IPv4Multicast = []string{"224.0.0.0/4"}
	// IPv4Reserved is the reserved range 240.0.0.0/4 (RFC 1112), which includes the
	// 255.255.255.255 limited broadcast address.
	IPv4Reserved = []string{"240.0.0.0/4"}

	// IPv6Unspecified is the IPv6 unspecified address ::/128 (RFC 4291).
	IPv6Unspecified = []string{"::/128"}
	// IPv6Loopback is the IPv6 loopback address ::1/128 (RFC 4291).
	IPv6Loopback = []string{"::1/128"}
	// IPv6LinkLocal is the IPv6 link-local range fe80::/10 (RFC 4291).
	IPv6LinkLocal = []string{"fe80::/10"}
	// IPv6ULA is the IPv6 unique local address range fc00::/7 (RFC 4193) — the IPv6 equivalent
	// of the RFC 1918 private ranges.
	IPv6ULA = []string{"fc00::/7"}
	// IPv6Multicast is the IPv6 multicast range ff00::/8 (RFC 4291).
	IPv6Multicast = []string{"ff00::/8"}

	// CloudMetadataCIDRs are the well-known cloud instance metadata endpoints. The common
	// 169.254.169.254 endpoint (AWS/GCP/Azure/OpenStack) is already within IPv4LinkLocal; this
	// list additionally covers the AWS IMDS IPv6 endpoint, which is a global unicast address and
	// so is NOT covered by any of the ranges above. Block this even on internal clients.
	CloudMetadataCIDRs = []string{
		"169.254.169.254/32", // AWS/GCP/Azure/OpenStack IMDS (also within IPv4LinkLocal)
		"fd00:ec2::254/128",  // AWS IMDS IPv6 endpoint
	}

	// LoopbackCIDRs blocks loopback and unspecified addresses for both IP families.
	LoopbackCIDRs = slices.Concat(IPv4Loopback, IPv4Unspecified, IPv6Loopback, IPv6Unspecified)

	// LinkLocalCIDRs blocks link-local addresses (including cloud metadata) for both IP families.
	LinkLocalCIDRs = slices.Concat(IPv4LinkLocal, IPv6LinkLocal, CloudMetadataCIDRs)

	// PrivateCIDRs blocks the private/internal ranges for both IP families: RFC 1918, CGNAT and
	// IPv6 ULA. ffnet does NOT block these by default since service-to-service traffic commonly
	// uses them — opt in only for externally-reachable clients.
	PrivateCIDRs = slices.Concat(IPv4Private, IPv4CGNAT, IPv6ULA)

	// MulticastCIDRs blocks multicast/reserved/broadcast ranges for both IP families.
	MulticastCIDRs = slices.Concat(IPv4Multicast, IPv4Reserved, IPv6Multicast)

	// SSRFDenylist is the recommended denylist for externally-reachable or
	// user-configurable clients (e.g. webhooks): every category above. Internal service-to-service
	// clients that must reach private ranges can compose a narrower list instead — e.g.
	// slices.Concat(LoopbackCIDRs, LinkLocalCIDRs, MulticastCIDRs), or just CloudMetadataCIDRs.
	SSRFDenylist = slices.Concat(LoopbackCIDRs, LinkLocalCIDRs, PrivateCIDRs, MulticastCIDRs)
)

// Config is the combined outbound-dialer configuration: the DNS resolver settings plus the
// egress CIDR denylist.
type Config struct {
	DNS ffdns.Config
	// CIDRDenylist is the set of CIDR ranges to block outbound connections to. Empty means no
	// restriction. Compose it from the exported building-block lists — e.g.
	// SSRFDenylist for any externally-configurable/webhook dialer.
	CIDRDenylist []string
}

func InitConfig(conf config.Section) {
	ffdns.InitConfig(conf)
	conf.AddKnownKey(CIDRDenylist)
}

func GenerateConfig(conf config.Section) (*Config, error) {
	dnsCfg, err := ffdns.GenerateConfig(conf)
	if err != nil {
		return nil, err
	}
	cfg := &Config{
		DNS: *dnsCfg,
	}
	// Empty by default (no egress restriction); callers opt in via config or by composing one of
	// the exported denylists.
	cfg.CIDRDenylist = conf.GetStringSlice(CIDRDenylist)
	return cfg, nil
}
