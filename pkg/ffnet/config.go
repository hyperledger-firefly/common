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
	// CIDRDenylist is the list of CIDR ranges to which outbound connections are blocked, as a
	// core SSRF mitigation. It is empty by default — ffnet/ffresty is frequently used for private
	// service-to-service traffic, so we do not presume which ranges are off-limits. Callers should
	// compose an appropriate denylist from the exported building-block lists below (e.g.
	// RecommendedSSRFDenylist for externally-reachable/webhook clients, or a narrower set such as
	// CloudMetadataCIDRs for internal clients that still want to block IMDS).
	CIDRDenylist = "cidrDenylist"
)

// Config is the combined outbound-dialer configuration: the DNS resolver settings plus the
// egress CIDR denylist.
type Config struct {
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
	cfg := &Config{}
	// Empty by default (no egress restriction); callers opt in via config or by composing one of
	// the exported denylists.
	cfg.CIDRDenylist = conf.GetStringSlice(CIDRDenylist)
	return cfg, nil
}
