// Copyright © 2024 Kaleido, Inc.
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
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"github.com/stretchr/testify/require"
	"math/big"
	"net"
	"os"
	"strings"
	"testing"
	"time"
	"io"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/stretchr/testify/assert"
)

func buildSelfSignedTLSKeyPair(t *testing.T, subject pkix.Name) (string, string) {
	// Create an X509 certificate pair
	privatekey, _ := rsa.GenerateKey(rand.Reader, 2048)
	publickey := &privatekey.PublicKey
	var privateKeyBytes []byte = x509.MarshalPKCS1PrivateKey(privatekey)
	privateKeyBlock := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: privateKeyBytes}
	privateKeyPEM := &strings.Builder{}
	err := pem.Encode(privateKeyPEM, privateKeyBlock)
	require.NoError(t, err)
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	x509Template := &x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               subject,
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(100 * time.Second),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	require.NoError(t, err)
	derBytes, err := x509.CreateCertificate(rand.Reader, x509Template, x509Template, publickey, privatekey)
	require.NoError(t, err)
	publicKeyPEM := &strings.Builder{}
	err = pem.Encode(publicKeyPEM, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	require.NoError(t, err)
	return publicKeyPEM.String(), privateKeyPEM.String()
}

func buildSelfSignedTLSKeyPairFiles(t *testing.T, subject pkix.Name) (string, string) {
	// Create an X509 certificate pair
	privatekey, _ := rsa.GenerateKey(rand.Reader, 2048)
	publickey := &privatekey.PublicKey
	var privateKeyBytes []byte = x509.MarshalPKCS1PrivateKey(privatekey)
	tmpDir := t.TempDir()
	privateKeyFile, _ := os.CreateTemp(tmpDir, "key.pem")
	privateKeyBlock := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: privateKeyBytes}
	pem.Encode(privateKeyFile, privateKeyBlock)
	serialNumber, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	x509Template := &x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               subject,
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(100 * time.Second),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, x509Template, x509Template, publickey, privatekey)
	assert.NoError(t, err)
	publicKeyFile, _ := os.CreateTemp(tmpDir, "cert.pem")
	pem.Encode(publicKeyFile, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	return publicKeyFile.Name(), privateKeyFile.Name()
}

func buildTLSListener(t *testing.T, conf config.Section, tlsType TLSType) (string, func()) {

	// Create the TLS Config with the CA pool and enable Client certificate validation
	tlsConfig, err := ConstructTLSConfig(context.Background(), conf, tlsType)
	assert.NoError(t, err)

	// Create a Server instance to listen on port 8443 with the TLS config
	server, err := tls.Listen("tcp4", "127.0.0.1:0", tlsConfig)
	assert.NoError(t, err)

	done := make(chan struct{})

	go func() {
		for {
			tlsConn, err := server.Accept()
			if err != nil {
				select {
				case <-done:
					return // cleanup in progress, don't log
				default:
					t.Logf("Server ending: %s", err)
				}
				return
			}
			// Just read until EOF, echoing back
			for {
				oneByte := make([]byte, 1)
				_, err = tlsConn.Read(oneByte)
				if err != nil {
					select {
					case <-done:
						// cleanup in progress, don't log
					default:
						if err != io.EOF {
							t.Logf("read failed: %s", err)
						}
					}
					break
				}
				_, _ = tlsConn.Write(oneByte) // ignore write errors during shutdown
			}
			tlsConn.Close()
		}
	}()
	return server.Addr().String(), func() {
		close(done)
		err := server.Close()
		assert.NoError(t, err)
	}

}

func TestNilIfNotEnabled(t *testing.T) {

	config.RootConfigReset()
	conf := config.RootSection("fftls_server")
	InitTLSConfig(conf)

	tlsConfig, err := ConstructTLSConfig(context.Background(), conf, ClientType)
	assert.NoError(t, err)
	assert.Nil(t, tlsConfig)

}

func TestTLSDefault(t *testing.T) {

	config.RootConfigReset()
	conf := config.RootSection("fftls_server")
	InitTLSConfig(conf)
	conf.Set(HTTPConfTLSEnabled, true)

	tlsConfig, err := ConstructTLSConfig(context.Background(), conf, ClientType)
	assert.NoError(t, err)
	assert.NotNil(t, tlsConfig)

	assert.False(t, tlsConfig.InsecureSkipVerify)
	assert.Equal(t, tls.NoClientCert, tlsConfig.ClientAuth)

}

func TestErrInvalidCAFile(t *testing.T) {

	config.RootConfigReset()
	_, notTheCAFileTheKey := buildSelfSignedTLSKeyPairFiles(t, pkix.Name{
		CommonName: "server.example.com",
	})

	conf := config.RootSection("fftls_server")
	InitTLSConfig(conf)
	conf.Set(HTTPConfTLSEnabled, true)
	conf.Set(HTTPConfTLSCAFile, notTheCAFileTheKey)

	_, err := ConstructTLSConfig(context.Background(), conf, ClientType)
	assert.Regexp(t, "FF00152", err)
}

func TestErrInvalidCA(t *testing.T) {

	config.RootConfigReset()
	_, notTheCATheKey := buildSelfSignedTLSKeyPair(t, pkix.Name{
		CommonName: "server.example.com",
	})

	conf := config.RootSection("fftls_server")
	InitTLSConfig(conf)
	conf.Set(HTTPConfTLSEnabled, true)
	conf.Set(HTTPConfTLSCA, notTheCATheKey)

	_, err := ConstructTLSConfig(context.Background(), conf, ClientType)
	assert.Regexp(t, "FF00152", err)
}

func TestErrInvalidKeyPairFile(t *testing.T) {

	config.RootConfigReset()
	notTheKeyFile, notTheCertFile := buildSelfSignedTLSKeyPairFiles(t, pkix.Name{
		CommonName: "server.example.com",
	})

	conf := config.RootSection("fftls_server")
	InitTLSConfig(conf)
	conf.Set(HTTPConfTLSEnabled, true)
	conf.Set(HTTPConfTLSKeyFile, notTheKeyFile)
	conf.Set(HTTPConfTLSCertFile, notTheCertFile)

	_, err := ConstructTLSConfig(context.Background(), conf, ClientType)
	assert.Regexp(t, "FF00206", err)

}

func TestErrInvalidKeyPair(t *testing.T) {

	config.RootConfigReset()
	notTheKey, notTheCert := buildSelfSignedTLSKeyPair(t, pkix.Name{
		CommonName: "server.example.com",
	})

	conf := config.RootSection("fftls_server")
	InitTLSConfig(conf)
	conf.Set(HTTPConfTLSEnabled, true)
	conf.Set(HTTPConfTLSKey, notTheKey)
	conf.Set(HTTPConfTLSCert, notTheCert)

	_, err := ConstructTLSConfig(context.Background(), conf, ClientType)
	assert.Regexp(t, "FF00206", err)

}

func TestMTLSOk(t *testing.T) {
	serverPublicKey, serverKey := buildSelfSignedTLSKeyPair(t, pkix.Name{
		CommonName: "server.example.com",
	})
	clientPublicKey, clientKey := buildSelfSignedTLSKeyPair(t, pkix.Name{
		CommonName: "client.example.com",
	})

	config.RootConfigReset()

	serverConf := config.RootSection("fftls_server")
	InitTLSConfig(serverConf)
	serverConf.Set(HTTPConfTLSEnabled, true)
	serverConf.Set(HTTPConfTLSCA, clientPublicKey)
	serverConf.Set(HTTPConfTLSCert, serverPublicKey)
	serverConf.Set(HTTPConfTLSKey, serverKey)
	serverConf.Set(HTTPConfTLSClientAuth, true)

	addr, done := buildTLSListener(t, serverConf, ServerType)
	defer done()

	clientConf := config.RootSection("fftls_client")
	InitTLSConfig(clientConf)
	clientConf.Set(HTTPConfTLSEnabled, true)
	clientConf.Set(HTTPConfTLSCA, serverPublicKey)
	clientConf.Set(HTTPConfTLSCert, clientPublicKey)
	clientConf.Set(HTTPConfTLSKey, clientKey)

	tlsConfig, err := ConstructTLSConfig(context.Background(), clientConf, ClientType)
	assert.NoError(t, err)
	conn, err := tls.Dial("tcp4", addr, tlsConfig)
	assert.NoError(t, err)
	written, err := conn.Write([]byte{42})
	assert.NoError(t, err)
	assert.Equal(t, written, 1)
	readBytes := []byte{0}
	readCount, err := conn.Read(readBytes)
	assert.NoError(t, err)
	assert.Equal(t, readCount, 1)
	assert.Equal(t, []byte{42}, readBytes)
	_ = conn.Close()

}

func TestMTLSMissingClientCert(t *testing.T) {

	serverPublicKeyFile, serverKeyFile := buildSelfSignedTLSKeyPairFiles(t, pkix.Name{
		CommonName: "server.example.com",
	})

	config.RootConfigReset()

	serverConf := config.RootSection("fftls_server")
	InitTLSConfig(serverConf)
	serverConf.Set(HTTPConfTLSEnabled, true)
	serverConf.Set(HTTPConfTLSCAFile, serverPublicKeyFile)
	serverConf.Set(HTTPConfTLSCertFile, serverPublicKeyFile)
	serverConf.Set(HTTPConfTLSKeyFile, serverKeyFile)
	serverConf.Set(HTTPConfTLSClientAuth, true)

	addr, done := buildTLSListener(t, serverConf, ServerType)
	defer done()

	clientConf := config.RootSection("fftls_client")
	InitTLSConfig(clientConf)
	clientConf.Set(HTTPConfTLSEnabled, true)
	clientConf.Set(HTTPConfTLSCAFile, serverPublicKeyFile)

	tlsConfig, err := ConstructTLSConfig(context.Background(), clientConf, ClientType)
	assert.NoError(t, err)
	conn, err := tls.Dial("tcp4", addr, tlsConfig)
	assert.NoError(t, err)
	_, _ = conn.Write([]byte{1})
	_, err = conn.Read([]byte{1})
	assert.Regexp(t, "certificate required", err)
	_ = conn.Close()

}

func TestMTLSMatchFullSubject(t *testing.T) {

	serverPublicKeyFile, serverKeyFile := buildSelfSignedTLSKeyPairFiles(t, pkix.Name{
		CommonName: "server.example.com",
	})
	clientPublicKeyFile, clientKeyFile := buildSelfSignedTLSKeyPairFiles(t, pkix.Name{
		CommonName:         "client.example.com",
		Country:            []string{"GB"},
		Organization:       []string{"hyperledger"},
		OrganizationalUnit: []string{"firefly"},
		Province:           []string{"SomeCounty"},
		Locality:           []string{"SomeTown"},
		StreetAddress:      []string{"SomeAddress"},
		PostalCode:         []string{"AB12 3CD"},
		SerialNumber:       "12345",
	})

	config.RootConfigReset()

	serverConf := config.RootSection("fftls_server")
	InitTLSConfig(serverConf)
	serverConf.Set(HTTPConfTLSEnabled, true)
	serverConf.Set(HTTPConfTLSCAFile, clientPublicKeyFile)
	serverConf.Set(HTTPConfTLSCertFile, serverPublicKeyFile)
	serverConf.Set(HTTPConfTLSKeyFile, serverKeyFile)
	serverConf.Set(HTTPConfTLSClientAuth, true)
	serverConf.Set(HTTPConfTLSRequiredDNAttributes, map[string]interface{}{
		"cn":           `[a-z]+\.example\.com`,
		"C":            "GB",
		"O":            "hyperledger",
		"OU":           "firefly",
		"ST":           "SomeCounty",
		"L":            "SomeTown",
		"STREET":       "SomeAddress",
		"POSTALCODE":   "AB12 3CD",
		"SERIALNUMBER": "12345",
	})

	addr, done := buildTLSListener(t, serverConf, ServerType)
	defer done()

	clientConf := config.RootSection("fftls_client")
	InitTLSConfig(clientConf)
	clientConf.Set(HTTPConfTLSEnabled, true)
	clientConf.Set(HTTPConfTLSCAFile, serverPublicKeyFile)
	clientConf.Set(HTTPConfTLSCertFile, clientPublicKeyFile)
	clientConf.Set(HTTPConfTLSKeyFile, clientKeyFile)

	tlsConfig, err := ConstructTLSConfig(context.Background(), clientConf, ClientType)
	assert.NoError(t, err)
	conn, err := tls.Dial("tcp4", addr, tlsConfig)
	written, err := conn.Write([]byte{42})
	assert.NoError(t, err)
	assert.Equal(t, written, 1)
	readBytes := []byte{0}
	readCount, err := conn.Read(readBytes)
	assert.NoError(t, err)
	assert.Equal(t, readCount, 1)
	assert.Equal(t, []byte{42}, readBytes)
	_ = conn.Close()

}

func TestMTLSMismatchSubject(t *testing.T) {

	serverPublicKeyFile, serverKeyFile := buildSelfSignedTLSKeyPairFiles(t, pkix.Name{
		CommonName: "server.example.com",
	})
	clientPublicKeyFile, clientKeyFile := buildSelfSignedTLSKeyPairFiles(t, pkix.Name{
		CommonName: "wrong.example.com",
	})

	config.RootConfigReset()

	serverConf := config.RootSection("fftls_server")
	InitTLSConfig(serverConf)
	serverConf.Set(HTTPConfTLSEnabled, true)
	serverConf.Set(HTTPConfTLSCAFile, clientPublicKeyFile)
	serverConf.Set(HTTPConfTLSCertFile, serverPublicKeyFile)
	serverConf.Set(HTTPConfTLSKeyFile, serverKeyFile)
	serverConf.Set(HTTPConfTLSClientAuth, true)
	serverConf.Set(HTTPConfTLSRequiredDNAttributes, map[string]interface{}{
		"cn": `right\.example\.com`,
	})

	addr, done := buildTLSListener(t, serverConf, ServerType)
	defer done()

	clientConf := config.RootSection("fftls_client")
	InitTLSConfig(clientConf)
	clientConf.Set(HTTPConfTLSEnabled, true)
	clientConf.Set(HTTPConfTLSCAFile, serverPublicKeyFile)
	clientConf.Set(HTTPConfTLSCertFile, clientPublicKeyFile)
	clientConf.Set(HTTPConfTLSKeyFile, clientKeyFile)

	tlsConfig, err := ConstructTLSConfig(context.Background(), clientConf, ClientType)
	assert.NoError(t, err)
	conn, err := tls.Dial("tcp4", addr, tlsConfig)
	_, _ = conn.Write([]byte{1})
	_, err = conn.Read([]byte{1})
	assert.Regexp(t, "bad certificate", err)
	_ = conn.Close()

}

func TestSubjectDNKnownAttributesAlwaysArray(t *testing.T) {

	assert.Equal(t, []string{}, SubjectDNKnownAttributes["CN"](pkix.Name{}))
	assert.Equal(t, []string{}, SubjectDNKnownAttributes["SERIALNUMBER"](pkix.Name{}))

}

func TestMTLSInvalidDNConfUnknown(t *testing.T) {

	config.RootConfigReset()
	serverConf := config.RootSection("fftls_server")
	InitTLSConfig(serverConf)
	serverConf.Set(HTTPConfTLSEnabled, true)
	serverConf.Set(HTTPConfTLSClientAuth, true)
	serverConf.Set(HTTPConfTLSRequiredDNAttributes, map[string]interface{}{
		"unknown": "anything",
	})

	_, err := ConstructTLSConfig(context.Background(), serverConf, ServerType)
	assert.Regexp(t, "FF00209", err)

}

func TestMTLSInvalidDNConfBadMap(t *testing.T) {

	config.RootConfigReset()
	serverConf := config.RootSection("fftls_server")
	InitTLSConfig(serverConf)
	serverConf.Set(HTTPConfTLSEnabled, true)
	serverConf.Set(HTTPConfTLSClientAuth, true)
	serverConf.Set(HTTPConfTLSRequiredDNAttributes, map[string]interface{}{
		"cn": map[string]interface{}{
			"some": "nestedness",
		},
	})

	_, err := ConstructTLSConfig(context.Background(), serverConf, ServerType)
	assert.Regexp(t, "FF00207", err)

}

func TestMTLSInvalidDNConfBadRegexp(t *testing.T) {

	config.RootConfigReset()
	serverConf := config.RootSection("fftls_server")
	InitTLSConfig(serverConf)
	serverConf.Set(HTTPConfTLSEnabled, true)
	serverConf.Set(HTTPConfTLSClientAuth, true)
	serverConf.Set(HTTPConfTLSRequiredDNAttributes, map[string]interface{}{
		"cn": "((((open regexp",
	})

	_, err := ConstructTLSConfig(context.Background(), serverConf, ServerType)
	assert.Regexp(t, "FF00208", err)

}

func TestMTLSDNValidatorNotVerified(t *testing.T) {

	testValidator, err := buildDNValidator(context.Background(), map[string]interface{}{
		"cn": "test",
	})
	assert.NoError(t, err)

	err = testValidator(nil, nil)
	assert.Regexp(t, "FF00210", err)

}

func TestMTLSDNValidatorEmptyChain(t *testing.T) {

	testValidator, err := buildDNValidator(context.Background(), map[string]interface{}{
		"cn": "test",
	})
	assert.NoError(t, err)

	err = testValidator(nil, [][]*x509.Certificate{{}})
	assert.Regexp(t, "FF00210", err)

}

func TestConnectSkipVerification(t *testing.T) {

	serverPublicKeyFile, serverKeyFile := buildSelfSignedTLSKeyPairFiles(t, pkix.Name{
		CommonName: "server.example.com",
	})

	config.RootConfigReset()

	serverConf := config.RootSection("fftls_server")
	InitTLSConfig(serverConf)
	serverConf.Set(HTTPConfTLSEnabled, true)
	serverConf.Set(HTTPConfTLSCAFile, serverPublicKeyFile)
	serverConf.Set(HTTPConfTLSCertFile, serverPublicKeyFile)
	serverConf.Set(HTTPConfTLSKeyFile, serverKeyFile)

	addr, done := buildTLSListener(t, serverConf, ServerType)
	defer done()

	clientConf := config.RootSection("fftls_client")
	InitTLSConfig(clientConf)
	clientConf.Set(HTTPConfTLSEnabled, true)
	clientConf.Set(HTTPConfTLSInsecureSkipHostVerify, true)

	tlsConfig, err := ConstructTLSConfig(context.Background(), clientConf, ClientType)
	assert.NoError(t, err)
	conn, err := tls.Dial("tcp4", addr, tlsConfig)
	assert.NoError(t, err)
	written, err := conn.Write([]byte{42})
	assert.NoError(t, err)
	assert.Equal(t, written, 1)
	readBytes := []byte{0}
	readCount, err := conn.Read(readBytes)
	assert.NoError(t, err)
	assert.Equal(t, readCount, 1)
	assert.Equal(t, []byte{42}, readBytes)
	_ = conn.Close()

}
