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

package fftls

import (
	"context"
	_ "embed"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/metric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The certificates below are static self-signed fixtures generated once with openssl. The CA
// fixtures are long-lived (valid until 2036); the leaf is deliberately generated to expire on
// 2026-06-10, i.e. it is already expired. This proves we record the expiry of an expired cert
// just the same - we only parse the NotAfter, we never validate the cert, so loading and
// recording succeed regardless. Example commands:
//
//	openssl req -x509 -newkey rsa:2048 -keyout leaf-key.pem -out leaf-cert.pem \
//	  -not_before 20260609000000Z -not_after 20260610000000Z -nodes \
//	  -subj "/CN=leaf/O=FireFly" -addext "subjectAltName=IP:127.0.0.1"
//
// ca-bundle.pem is the concatenation of ca-a-cert.pem and ca-b-cert.pem.
//
// The expiry timestamps below are the NotAfter (unix) of each fixture, hard-coded so the test
// asserts against a fixed expectation rather than re-deriving it from the same parsing logic
// under test. DO NOT regenerate the fixtures without also updating these constants.
var (
	//go:embed testdata/ca-a-cert.pem
	caACertPEM []byte
	//go:embed testdata/ca-bundle.pem
	caBundlePEM []byte
	//go:embed testdata/leaf-cert.pem
	leafCertPEM []byte
	//go:embed testdata/leaf-key.pem
	leafKeyPEM []byte
	// leaf-bundle.pem is leaf-cert.pem followed by ca-a-cert.pem, standing in for a leaf cert file
	// that bundles the leaf with its intermediate chain.
	//go:embed testdata/leaf-bundle.pem
	leafBundlePEM []byte
)

const (
	caAExpiryUnix  = float64(2096370463) // CN=ca-a, notAfter=2036-06-06 13:07:43Z
	caBExpiryUnix  = float64(2096370464) // CN=ca-b, notAfter=2036-06-06 13:07:44Z
	leafExpiryUnix = float64(1781049600) // CN=leaf, notAfter=2026-06-10 00:00:00Z (already expired)
)

func writeTemp(t *testing.T, dir, name string, content []byte) string {
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, content, 0600))
	return p
}

// findCertExpiry gathers from the registry and returns the value of the ff_tls_certificate_expiry
// gauge for the series matching the given "type" label and whose "subject" label contains the supplied
// substring.
func findCertExpiry(t *testing.T, mr metric.MetricsRegistry, certType, subjectSubstr string) (float64, bool) {
	mfs, err := mr.GetGatherer().Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "ff_tls_certificate_expiry" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var typeMatch, subjectMatch bool
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "type" && lp.GetValue() == certType {
					typeMatch = true
				}
				if lp.GetName() == "subject" && strings.Contains(lp.GetValue(), subjectSubstr) {
					subjectMatch = true
				}
			}
			if typeMatch && subjectMatch {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// enableTestCertMetrics resets the package-level manager and binds it to a fresh registry for the test.
func enableTestCertMetrics(t *testing.T, component string) metric.MetricsRegistry {
	certMetricsManager = nil
	t.Cleanup(func() { certMetricsManager = nil })
	mr := metric.NewPrometheusMetricsRegistry(component)
	require.NoError(t, EnableCertificateMetrics(context.Background(), mr))
	return mr
}

func TestEnableCertificateMetricsRecordsCAAndClientExpiry(t *testing.T) {
	mr := enableTestCertMetrics(t, "test_fftls_client")

	dir := t.TempDir()
	caBundleFile := writeTemp(t, dir, "ca-bundle.pem", caBundlePEM)
	leafCertFile := writeTemp(t, dir, "leaf-cert.pem", leafCertPEM)
	leafKeyFile := writeTemp(t, dir, "leaf-key.pem", leafKeyPEM)

	conf := config.RootSection("fftls_metrics_client")
	InitTLSConfig(conf)
	conf.Set(HTTPConfTLSEnabled, true)
	conf.Set(HTTPConfTLSCAFile, caBundleFile)
	conf.Set(HTTPConfTLSCertFile, leafCertFile)
	conf.Set(HTTPConfTLSKeyFile, leafKeyFile)

	_, err := ConstructTLSConfig(context.Background(), conf, ClientType)
	require.NoError(t, err)

	// Each certificate in the CA bundle gets its own series under the type="ca" label
	caAVal, ok := findCertExpiry(t, mr, "ca", "ca-a")
	require.True(t, ok, "ca-a expiry gauge not found")
	assert.Equal(t, caAExpiryUnix, caAVal)

	caBVal, ok := findCertExpiry(t, mr, "ca", "ca-b")
	require.True(t, ok, "ca-b expiry gauge not found")
	assert.Equal(t, caBExpiryUnix, caBVal)

	// A client TLS config records the leaf under type="client"
	clientVal, ok := findCertExpiry(t, mr, "client", "leaf")
	require.True(t, ok, "client expiry gauge not found")
	assert.Equal(t, leafExpiryUnix, clientVal)

	// ... and nothing should have been recorded under type="server"
	_, ok = findCertExpiry(t, mr, "server", "leaf")
	assert.False(t, ok, "server gauge should not be set for a client config")
}

func TestEnableCertificateMetricsBundledLeafRecordsLeafNotChain(t *testing.T) {
	mr := enableTestCertMetrics(t, "test_fftls_leaf_bundle")

	dir := t.TempDir()
	// The cert file bundles the leaf with an intermediate (ca-a). We must record the leaf's expiry,
	// not the intermediate's.
	leafCertFile := writeTemp(t, dir, "leaf-bundle.pem", leafBundlePEM)
	leafKeyFile := writeTemp(t, dir, "leaf-key.pem", leafKeyPEM)

	conf := config.RootSection("fftls_metrics_leaf_bundle")
	InitTLSConfig(conf)
	conf.Set(HTTPConfTLSEnabled, true)
	conf.Set(HTTPConfTLSCertFile, leafCertFile)
	conf.Set(HTTPConfTLSKeyFile, leafKeyFile)

	_, err := ConstructTLSConfig(context.Background(), conf, ClientType)
	require.NoError(t, err)

	// The recorded client expiry is the leaf's, even though ca-a appears later in the bundle
	clientVal, ok := findCertExpiry(t, mr, "client", "leaf")
	require.True(t, ok, "client expiry gauge not found")
	assert.Equal(t, leafExpiryUnix, clientVal)

	// The intermediate in the cert bundle must not be recorded as a client (or ca) series here
	_, ok = findCertExpiry(t, mr, "client", "ca-a")
	assert.False(t, ok, "intermediate from the leaf bundle should not be recorded")
}

func TestEnableCertificateMetricsRecordsServerExpiryInline(t *testing.T) {
	mr := enableTestCertMetrics(t, "test_fftls_server")

	// Inline (non-file) PEM material is recorded the same way
	conf := &Config{
		Enabled: true,
		CA:      string(caACertPEM),
		Cert:    string(leafCertPEM),
		Key:     string(leafKeyPEM),
	}
	_, err := NewTLSConfig(context.Background(), conf, ServerType)
	require.NoError(t, err)

	caAVal, ok := findCertExpiry(t, mr, "ca", "ca-a")
	require.True(t, ok, "ca-a expiry gauge not found")
	assert.Equal(t, caAExpiryUnix, caAVal)

	// A server TLS config records the leaf under type="server"
	serverVal, ok := findCertExpiry(t, mr, "server", "leaf")
	require.True(t, ok, "server expiry gauge not found")
	assert.Equal(t, leafExpiryUnix, serverVal)

	_, ok = findCertExpiry(t, mr, "client", "leaf")
	assert.False(t, ok, "client gauge should not be set for a server config")
}

func TestEnableCertificateMetricsIdempotent(t *testing.T) {
	certMetricsManager = nil
	t.Cleanup(func() { certMetricsManager = nil })
	ctx := context.Background()
	mr := metric.NewPrometheusMetricsRegistry("test_fftls_idem")
	require.NoError(t, EnableCertificateMetrics(ctx, mr))
	// Second call is a no-op and must not error (would otherwise fail re-registering the subsystem)
	require.NoError(t, EnableCertificateMetrics(ctx, mr))
}

func TestEnableCertificateMetricsError(t *testing.T) {
	certMetricsManager = nil
	t.Cleanup(func() { certMetricsManager = nil })
	ctx := context.Background()
	mr := metric.NewPrometheusMetricsRegistry("test_fftls_err")
	// Claim the "tls" subsystem before fftls can, forcing a registration error
	_, _ = mr.NewMetricsManagerForSubsystem(ctx, CertMetricsSubsystem)
	err := EnableCertificateMetrics(ctx, mr)
	assert.Error(t, err)
}

func TestNewTLSConfigNoMetricsManagerNoPanic(t *testing.T) {
	// With metrics disabled (the default), loading certs must not panic
	certMetricsManager = nil
	dir := t.TempDir()
	caFile := writeTemp(t, dir, "ca-a-cert.pem", caACertPEM)

	conf := config.RootSection("fftls_no_metrics")
	InitTLSConfig(conf)
	conf.Set(HTTPConfTLSEnabled, true)
	conf.Set(HTTPConfTLSCAFile, caFile)

	tlsConfig, err := ConstructTLSConfig(context.Background(), conf, ClientType)
	require.NoError(t, err)
	assert.NotNil(t, tlsConfig)
}
