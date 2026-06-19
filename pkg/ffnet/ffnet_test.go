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
	"testing"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/ffdns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var utConf = config.RootSection("net_unit_tests")

func resetConf() {
	config.RootConfigReset()
	InitConfig(utConf)
}

func TestNewDialControlDefaults(t *testing.T) {
	// nil CIDRDenylist => secure defaults
	control, err := NewDialControl(context.Background(), &Config{})
	require.NoError(t, err)
	require.NotNil(t, control)

	// Blocked by default: loopback, link-local/cloud-metadata, unspecified, multicast/reserved
	assert.Error(t, control("tcp", "127.0.0.1:8080", nil))
	assert.Error(t, control("tcp", "169.254.169.254:80", nil)) // cloud metadata
	assert.Error(t, control("tcp", "0.0.0.0:80", nil))
	assert.Error(t, control("tcp", "[::1]:443", nil))
	assert.Error(t, control("tcp", "[fd00:ec2::254]:80", nil)) // AWS IMDS IPv6
	assert.Error(t, control("tcp", "224.0.0.1:80", nil))       // IPv4 multicast
	assert.Error(t, control("tcp", "255.255.255.255:80", nil)) // IPv4 broadcast (reserved)
	assert.Error(t, control("tcp", "240.0.0.1:80", nil))       // IPv4 reserved
	assert.Error(t, control("tcp", "[ff02::1]:80", nil))       // IPv6 multicast

	// NOT blocked by default: public, and private RFC1918 (deferred to firewalls)
	assert.NoError(t, control("tcp", "8.8.8.8:443", nil))
	assert.NoError(t, control("tcp", "10.1.2.3:80", nil))
	assert.NoError(t, control("tcp", "192.168.1.5:80", nil))

	// IPv4-mapped IPv6 loopback is normalized and still blocked
	assert.Error(t, control("tcp", "[::ffff:127.0.0.1]:80", nil))
}

func TestNewDialControlOverride(t *testing.T) {
	// Explicit empty override disables denylisting entirely
	control, err := NewDialControl(context.Background(), &Config{CIDRDenylist: []string{}})
	require.NoError(t, err)
	assert.Nil(t, control)

	// Custom override fully replaces defaults
	control, err = NewDialControl(context.Background(), &Config{CIDRDenylist: []string{"10.0.0.0/8"}})
	require.NoError(t, err)
	require.NotNil(t, control)
	assert.Error(t, control("tcp", "10.1.2.3:80", nil))    // now blocked
	assert.NoError(t, control("tcp", "127.0.0.1:80", nil)) // defaults no longer apply
}

func TestNewDialControlAdditional(t *testing.T) {
	// Additional CIDRs extend the defaults
	control, err := NewDialControl(context.Background(), &Config{AdditionalDeniedCIDRs: []string{"10.0.0.0/8"}})
	require.NoError(t, err)
	require.NotNil(t, control)
	assert.Error(t, control("tcp", "10.1.2.3:80", nil))  // added
	assert.Error(t, control("tcp", "127.0.0.1:80", nil)) // default still applies
}

func TestNewDialControlInvalidCIDR(t *testing.T) {
	_, err := NewDialControl(context.Background(), &Config{AdditionalDeniedCIDRs: []string{"not-a-cidr"}})
	assert.Regexp(t, "FF00260", err)
}

func TestDialControlBlocksUnparseableAddress(t *testing.T) {
	control, err := NewDialControl(context.Background(), &Config{})
	require.NoError(t, err)
	// Fail closed if an address somehow isn't a resolved IP literal
	assert.Regexp(t, "FF00261", control("tcp", "not-an-ip:80", nil))
}

func TestDialControlBareIPNoPort(t *testing.T) {
	// Addresses without a port still resolve their IP (SplitHostPort error fallback)
	control, err := NewDialControl(context.Background(), &Config{})
	require.NoError(t, err)
	assert.Error(t, control("tcp", "127.0.0.1", nil))       // blocked
	assert.NoError(t, control("tcp", "8.8.8.8", nil))       // allowed
	assert.Error(t, control("tcp", "169.254.169.254", nil)) // metadata blocked
}

func TestNewDialerEndToEndBlocks(t *testing.T) {
	// The guard fires through a real net.Dialer dial, before any connection is made
	d, err := NewDialer(context.Background(), &Config{})
	require.NoError(t, err)
	require.NotNil(t, d.Control)
	_, err = d.DialContext(context.Background(), "tcp", "127.0.0.1:1")
	assert.Regexp(t, "FF00261", err)
}

func TestNewDialerAllowsAndConnects(t *testing.T) {
	// With the denylist disabled, the dialer connects normally to a loopback listener
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go func() {
		if conn, acceptErr := ln.Accept(); acceptErr == nil {
			_ = conn.Close()
		}
	}()

	d, err := NewDialer(context.Background(), &Config{CIDRDenylist: []string{}})
	require.NoError(t, err)
	assert.Nil(t, d.Control)
	conn, err := d.DialContext(context.Background(), "tcp", ln.Addr().String())
	require.NoError(t, err)
	require.NotNil(t, conn)
	_ = conn.Close()
}

func TestGenerateConfigOverrideReplacesDefaults(t *testing.T) {
	resetConf()
	utConf.Set(CIDRDenylist, []string{"10.0.0.0/8"})
	cfg, err := GenerateConfig(utConf)
	require.NoError(t, err)
	assert.Equal(t, []string{"10.0.0.0/8"}, cfg.CIDRDenylist)

	control, err := NewDialControl(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, control)
	assert.Error(t, control("tcp", "10.1.2.3:80", nil))    // override entry blocked
	assert.NoError(t, control("tcp", "127.0.0.1:80", nil)) // defaults replaced, loopback allowed
}

func TestNewDialer(t *testing.T) {
	// Default config => dialer with the SSRF guard wired (resolver nil since no DNS servers)
	d, err := NewDialer(context.Background(), &Config{})
	require.NoError(t, err)
	require.NotNil(t, d)
	assert.Nil(t, d.Resolver)
	require.NotNil(t, d.Control)
	assert.Error(t, d.Control("tcp", "127.0.0.1:80", nil))

	// DNS servers => resolver attached; empty denylist => no control
	d, err = NewDialer(context.Background(), &Config{
		DNS:          ffdns.Config{Servers: []string{"8.8.8.8"}},
		CIDRDenylist: []string{},
	})
	require.NoError(t, err)
	require.NotNil(t, d.Resolver)
	assert.Nil(t, d.Control)

	// Invalid CIDR propagates as an error
	_, err = NewDialer(context.Background(), &Config{AdditionalDeniedCIDRs: []string{"bad"}})
	assert.Regexp(t, "FF00260", err)
}

func TestGenerateConfigDenylistSemantics(t *testing.T) {
	// Unset cidrDenylist => defaults active (control blocks loopback)
	resetConf()
	cfg, err := GenerateConfig(utConf)
	require.NoError(t, err)
	assert.Nil(t, cfg.CIDRDenylist)
	control, err := NewDialControl(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, control)
	assert.Error(t, control("tcp", "127.0.0.1:80", nil))

	// Explicitly-set empty cidrDenylist => disabled
	resetConf()
	utConf.Set(CIDRDenylist, []string{})
	cfg, err = GenerateConfig(utConf)
	require.NoError(t, err)
	assert.NotNil(t, cfg.CIDRDenylist)
	control, err = NewDialControl(context.Background(), cfg)
	require.NoError(t, err)
	assert.Nil(t, control)
}

func TestGenerateConfigDNSAndAdditional(t *testing.T) {
	resetConf()
	utConf.Set(ffdns.DNSServers, []string{"8.8.8.8"})
	utConf.Set(AdditionalDeniedCIDRs, []string{"10.0.0.0/8"})
	cfg, err := GenerateConfig(utConf)
	require.NoError(t, err)
	assert.Equal(t, []string{"8.8.8.8"}, cfg.DNS.Servers)
	assert.Equal(t, []string{"10.0.0.0/8"}, cfg.AdditionalDeniedCIDRs)
	assert.NotNil(t, cfg.Resolver())
}
