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

var testSSRDenylist = []string{
	"0.0.0.0/8",            // unspecified / "this host" (RFC 1122)
	"127.0.0.0/8",          // IPv4 loopback
	"169.254.0.0/16",       // IPv4 link-local, incl. cloud metadata 169.254.169.254
	"10.0.0.0/8",           // IPv4 private RFC1918
	"172.16.0.0/12",        // IPv4 private RFC1918
	"192.168.0.0/16",       // IPv4 private RFC1918
	"100.64.0.0/10",        // IPv4 CGNAT
	"224.0.0.0/4",          // IPv4 multicast
	"240.0.0.0/4",          // IPv4 reserved (incl. 255.255.255.255 broadcast)
	"fc00::/7",             // IPv6 ULA
	"fe00::/8",             // IPv6 private RFC4193
	"ff00::/8",             // IPv6 reserved
	"::ffff:127.0.0.1/128", // IPv4-mapped IPv6 loopback
	"::1/128",              // IPv6 loopback
	"::/0",                 // IPv6 unspecified
	"::/128",               // IPv6 unspecified
}

func resetConf() {
	config.RootConfigReset()
	InitConfig(utConf)
}

func TestNewDialControlEmptyByDefault(t *testing.T) {
	// No denylist configured => no egress restriction at all
	control, err := NewDialControl(context.Background(), &Config{})
	require.NoError(t, err)
	assert.Nil(t, control)
}

func TestNewDialControlSSRFDenylist(t *testing.T) {
	// The test denylist blocks every reserved/internal category
	control, err := NewDialControl(context.Background(), &Config{CIDRDenylist: testSSRDenylist})
	require.NoError(t, err)
	require.NotNil(t, control)

	assert.Error(t, control("tcp", "127.0.0.1:8080", nil))     // loopback
	assert.Error(t, control("tcp", "169.254.169.254:80", nil)) // cloud metadata
	assert.Error(t, control("tcp", "0.0.0.0:80", nil))         // unspecified
	assert.Error(t, control("tcp", "[::1]:443", nil))          // IPv6 loopback
	assert.Error(t, control("tcp", "[fd00:ec2::254]:80", nil)) // AWS IMDS IPv6
	assert.Error(t, control("tcp", "224.0.0.1:80", nil))       // IPv4 multicast
	assert.Error(t, control("tcp", "255.255.255.255:80", nil)) // IPv4 broadcast (reserved)
	assert.Error(t, control("tcp", "240.0.0.1:80", nil))       // IPv4 reserved
	assert.Error(t, control("tcp", "[ff02::1]:80", nil))       // IPv6 multicast
	assert.Error(t, control("tcp", "10.1.2.3:80", nil))        // RFC1918 private
	assert.Error(t, control("tcp", "192.168.1.5:80", nil))     // RFC1918 private
	assert.Error(t, control("tcp", "100.64.1.1:80", nil))      // CGNAT
	assert.Error(t, control("tcp", "[fc00::1]:80", nil))       // IPv6 ULA

	// Public addresses still allowed
	assert.NoError(t, control("tcp", "8.8.8.8:443", nil))

	// IPv4-mapped IPv6 loopback is normalized and still blocked
	assert.Error(t, control("tcp", "[::ffff:127.0.0.1]:80", nil))
}

func TestNewDialControlCloudMetadataOnly(t *testing.T) {
	// The minimal protection: block only the cloud metadata endpoints
	cloudMetadataCIDRs := []string{
		"169.254.169.254/32",
		"fd00:ec2::254/128",
	}
	control, err := NewDialControl(context.Background(), &Config{CIDRDenylist: cloudMetadataCIDRs})
	require.NoError(t, err)
	require.NotNil(t, control)

	assert.Error(t, control("tcp", "169.254.169.254:80", nil)) // metadata blocked
	assert.Error(t, control("tcp", "[fd00:ec2::254]:80", nil)) // IMDS IPv6 blocked
	assert.NoError(t, control("tcp", "127.0.0.1:80", nil))     // loopback reachable
	assert.NoError(t, control("tcp", "169.254.1.1:80", nil))   // other link-local reachable
}

func TestNewDialControlOverride(t *testing.T) {
	// Explicit empty list disables denylisting entirely
	control, err := NewDialControl(context.Background(), &Config{CIDRDenylist: []string{}})
	require.NoError(t, err)
	assert.Nil(t, control)

	// A custom list is used exactly as given
	ipv4PrivateCIDRs := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
	}
	control, err = NewDialControl(context.Background(), &Config{CIDRDenylist: ipv4PrivateCIDRs})
	require.NoError(t, err)
	require.NotNil(t, control)
	assert.Error(t, control("tcp", "10.1.2.3:80", nil))    // in list
	assert.NoError(t, control("tcp", "127.0.0.1:80", nil)) // not in list
}

func TestNewDialControlInvalidCIDR(t *testing.T) {
	_, err := NewDialControl(context.Background(), &Config{CIDRDenylist: []string{"not-a-cidr"}})
	assert.Regexp(t, "FF00260", err)
}

func TestDialControlBlocksUnparseableAddress(t *testing.T) {
	control, err := NewDialControl(context.Background(), &Config{CIDRDenylist: testSSRDenylist})
	require.NoError(t, err)
	// Fail closed if an address somehow isn't a resolved IP literal
	assert.Regexp(t, "FF00261", control("tcp", "not-an-ip:80", nil))
}

func TestDialControlBareIPNoPort(t *testing.T) {
	// Addresses without a port still resolve their IP (SplitHostPort error fallback)
	control, err := NewDialControl(context.Background(), &Config{CIDRDenylist: testSSRDenylist})
	require.NoError(t, err)
	assert.Error(t, control("tcp", "127.0.0.1", nil))       // blocked
	assert.NoError(t, control("tcp", "8.8.8.8", nil))       // allowed
	assert.Error(t, control("tcp", "169.254.169.254", nil)) // metadata blocked
}

func TestNewDialerEndToEndBlocks(t *testing.T) {
	// The guard fires through a real net.Dialer dial, before any connection is made
	loopbackCIDRs := []string{
		"127.0.0.0/8",
	}
	d, err := NewDialer(context.Background(), &Config{CIDRDenylist: loopbackCIDRs}, nil)
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

	d, err := NewDialer(context.Background(), &Config{CIDRDenylist: []string{}}, nil)
	require.NoError(t, err)
	assert.Nil(t, d.Control)
	conn, err := d.DialContext(context.Background(), "tcp", ln.Addr().String())
	require.NoError(t, err)
	require.NotNil(t, conn)
	_ = conn.Close()
}

func TestGenerateConfigDenylistFromConfig(t *testing.T) {
	resetConf()
	ipv4PrivateCIDRs := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
	}
	utConf.Set(CIDRDenylist, ipv4PrivateCIDRs)
	cfg, err := GenerateConfig(utConf)
	require.NoError(t, err)
	assert.Equal(t, ipv4PrivateCIDRs, cfg.CIDRDenylist)

	control, err := NewDialControl(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, control)
	assert.Error(t, control("tcp", "10.1.2.3:80", nil))    // configured entry blocked
	assert.NoError(t, control("tcp", "127.0.0.1:80", nil)) // loopback not in the configured list
}

func TestNewDialer(t *testing.T) {
	// SSRF denylist => dialer with the egress guard wired (resolver nil since no DNS servers)
	d, err := NewDialer(context.Background(), &Config{CIDRDenylist: testSSRDenylist}, nil)
	require.NoError(t, err)
	require.NotNil(t, d)
	assert.Nil(t, d.Resolver)
	require.NotNil(t, d.Control)
	assert.Error(t, d.Control("tcp", "127.0.0.1:80", nil))

	resolver := ffdns.NewResolverWithConfig(&ffdns.Config{Servers: []string{"8.8.8.8"}})
	// DNS servers => resolver attached; no denylist => no control
	d, err = NewDialer(context.Background(), &Config{}, resolver)
	require.NoError(t, err)
	require.NotNil(t, d.Resolver)
	assert.Nil(t, d.Control)

	// Invalid CIDR propagates as an error
	_, err = NewDialer(context.Background(), &Config{CIDRDenylist: []string{"bad"}}, resolver)
	assert.Regexp(t, "FF00260", err)
}

func TestGenerateConfigDenylistSemantics(t *testing.T) {
	// Unset cidrDenylist => empty, no guard
	resetConf()
	cfg, err := GenerateConfig(utConf)
	require.NoError(t, err)
	assert.Empty(t, cfg.CIDRDenylist)
	control, err := NewDialControl(context.Background(), cfg)
	require.NoError(t, err)
	assert.Nil(t, control)

	// Configured denylist => guard active
	resetConf()
	utConf.Set(CIDRDenylist, testSSRDenylist)
	cfg, err = GenerateConfig(utConf)
	require.NoError(t, err)
	assert.NotEmpty(t, cfg.CIDRDenylist)
	control, err = NewDialControl(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, control)
	assert.Error(t, control("tcp", "127.0.0.1:80", nil))
}
