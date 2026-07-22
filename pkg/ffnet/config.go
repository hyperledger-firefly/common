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

// Package ffnet builds outbound net.Dialers and their egress controls: a custom DNS resolver
// (via ffdns) plus a CIDR egress denylist for SSRF protection. It is the single place to
// configure how — and where — a client is allowed to make outbound connections.
package ffnet

import (
	"github.com/hyperledger-firefly/common/pkg/config"
)

const (
	// CIDRDenylist is the list of CIDR ranges to which outbound connections are blocked, as a
	// core SSRF mitigation. It is empty by default. Callers should
	// compose an appropriate denylist depending on the client's use case.
	NetCIDRDenylist = "cidrDenylist"
)

// Config is the outbound-dialer configuration.
type Config struct {
	// CIDRDenylist is the set of CIDR ranges to block outbound connections to. Empty means no
	// restriction.
	CIDRDenylist []string
}

func InitConfig(conf config.Section) {
	conf.AddKnownKey(NetCIDRDenylist)
}

func GenerateConfig(conf config.Section) (*Config, error) {
	cfg := &Config{}
	cfg.CIDRDenylist = conf.GetStringSlice(NetCIDRDenylist)
	return cfg, nil
}
