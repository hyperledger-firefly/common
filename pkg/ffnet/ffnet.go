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

package ffnet

import (
	"context"
	"net"
	"syscall"

	"github.com/hyperledger/firefly-common/pkg/ffdns"
	"github.com/hyperledger/firefly-common/pkg/i18n"
)

// Resolver returns the custom DNS resolver for this config, or nil to use the system resolver.
func (cfg *Config) Resolver() *net.Resolver {
	return ffdns.NewResolverWithConfig(&cfg.DNS)
}

// NewDialer builds a *net.Dialer wired with the custom DNS resolver (if any) and the SSRF
// egress guard (on by default). The caller is responsible for setting Timeout / KeepAlive to
// suit its protocol. Exported so any dialer-based client — HTTP, WebSocket, etc. — can apply
// identical outbound protection from the same config.
func NewDialer(ctx context.Context, cfg *Config) (*net.Dialer, error) {
	control, err := NewDialControl(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &net.Dialer{
		Resolver: cfg.Resolver(),
		Control:  control,
	}, nil
}

// NewDialControl builds a net.Dialer Control function that rejects connections to any address
// inside the effective CIDR denylist — the core SSRF mitigation. It runs after DNS resolution
// against the actual resolved IP, so it also defeats DNS-rebinding and literal-IP bypasses.
// Returns (nil, nil) when the effective denylist is empty (no restrictions).
func NewDialControl(ctx context.Context, cfg *Config) (func(network, address string, c syscall.RawConn) error, error) {
	entries := cfg.CIDRDenylist
	if len(entries) == 0 {
		return nil, nil
	}
	denied := make([]*net.IPNet, 0, len(entries))
	for _, entry := range entries {
		_, ipNet, err := net.ParseCIDR(entry)
		if err != nil {
			return nil, i18n.NewError(ctx, i18n.MsgInvalidCIDR, entry)
		}
		denied = append(denied, ipNet)
	}
	return func(_, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			host = address
		}
		ip := net.ParseIP(host)
		if ip == nil {
			// Control is always invoked with a resolved IP literal; if it isn't one, fail
			// closed rather than allow an unexpected target through.
			return i18n.NewError(ctx, i18n.MsgConnectionToCIDRBlocked, address, "unparseable address")
		}
		for _, ipNet := range denied {
			if ipNet.Contains(ip) {
				return i18n.NewError(ctx, i18n.MsgConnectionToCIDRBlocked, ip.String(), ipNet.String())
			}
		}
		return nil
	}, nil
}
