// Copyright © 2021 Kaleido, Inc.
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

package httpserver

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/hyperledger-firefly/common/mocks/httpservermocks"
	"github.com/hyperledger-firefly/common/pkg/auth/basic"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/fftls"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const configDir = "../../test/data/config"

func TestInvalidListener(t *testing.T) {
	cp := config.RootSection("ut")
	InitHTTPConfig(cp, 0)
	cc := config.RootSection("utCors")
	InitCORSConfig(cc)
	cp.Set(HTTPConfAddress, "...")
	_, err := NewHTTPServer(context.Background(), "ut", mux.NewRouter(), make(chan error), cp, cc)
	assert.Error(t, err)
}

func TestServeFail(t *testing.T) {
	config.RootConfigReset()
	cp := config.RootSection("ut")
	InitHTTPConfig(cp, 0)
	cc := config.RootSection("utCors")
	InitCORSConfig(cc)
	errChan := make(chan error)
	hs, err := NewHTTPServer(context.Background(), "ut", mux.NewRouter(), errChan, cp, cc)
	assert.NoError(t, err)
	hs.(*httpServer).l.Close() // So server will fail
	go hs.ServeHTTP(context.Background())
	err = <-errChan
	assert.Error(t, err)
}

func TestShutdownOkCustomOpts(t *testing.T) {
	config.RootConfigReset()
	cp := config.RootSection("ut")
	InitHTTPConfig(cp, 0)
	cc := config.RootSection("utCors")
	InitCORSConfig(cc)
	errChan := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	l, err := NewHTTPServer(ctx, "ut", mux.NewRouter(), errChan, cp, cc, &ServerOptions{
		MaximumRequestTimeout: 1 * time.Hour,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, l.Addr().String())
	assert.Equal(t, 1*time.Hour, l.(*httpServer).options.MaximumRequestTimeout)
	cancel()
}

func TestShutdownError(t *testing.T) {
	testDone := make(chan struct{})
	config.RootConfigReset()
	cp := config.RootSection("ut")
	cp.Set(HTTPConfShutdownTimeout, "1ms")
	InitHTTPConfig(cp, 0)
	cc := config.RootSection("utCors")
	InitCORSConfig(cc)
	errChan := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	s, err := NewHTTPServer(ctx, "ut", mux.NewRouter(), errChan, cp, cc)
	assert.NoError(t, err)
	m := &httpservermocks.GoHTTPServer{}
	m.On("Shutdown", mock.Anything).Return(errors.New("forced error"))
	m.On("Serve", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		<-testDone
		return
	})
	s.(*httpServer).s = m
	go func() {
		s.ServeHTTP(ctx)
	}()
	cancel()
	err = <-errChan
	assert.Error(t, err)
	testDone <- struct{}{}
}

func TestMissingCAFile(t *testing.T) {
	cp := config.RootSection("ut")
	InitHTTPConfig(cp, 0)
	tlsSection := cp.SubSection("tls")
	tlsSection.Set(fftls.HTTPConfTLSEnabled, true)
	tlsSection.Set(fftls.HTTPConfTLSCAFile, "badness")
	cc := config.RootSection("utCors")
	InitCORSConfig(cc)
	_, err := NewHTTPServer(context.Background(), "ut", mux.NewRouter(), make(chan error), cp, cc)
	assert.Regexp(t, "FF00153", err)
}

func TestBadCAFile(t *testing.T) {
	cp := config.RootSection("ut")
	InitHTTPConfig(cp, 0)
	cc := config.RootSection("utCors")
	InitCORSConfig(cc)
	tlsSection := cp.SubSection("tls")
	tlsSection.Set(fftls.HTTPConfTLSEnabled, true)
	tlsSection.Set(fftls.HTTPConfTLSCAFile, configDir+"/firefly.common.yaml")
	_, err := NewHTTPServer(context.Background(), "ut", mux.NewRouter(), make(chan error), cp, cc)
	assert.Regexp(t, "FF00152", err)
}

func TestTLSServerSelfSignedWithClientAuth(t *testing.T) {

	// Create an X509 certificate pair
	privatekey, _ := rsa.GenerateKey(rand.Reader, 2048)
	publickey := &privatekey.PublicKey
	var privateKeyBytes []byte = x509.MarshalPKCS1PrivateKey(privatekey)
	privateKeyFile, _ := os.CreateTemp("", "key.pem")
	defer os.Remove(privateKeyFile.Name())
	privateKeyBlock := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: privateKeyBytes}
	pem.Encode(privateKeyFile, privateKeyBlock)
	serialNumber, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	x509Template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Unit Tests"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(100 * time.Second),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, x509Template, x509Template, publickey, privatekey)
	assert.NoError(t, err)
	publicKeyFile, _ := os.CreateTemp("", "cert.pem")
	defer os.Remove(publicKeyFile.Name())
	pem.Encode(publicKeyFile, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	// Start up a listener configured for TLS Mutual auth
	config.RootConfigReset() // ensure APIShutdownTimeout cleared from earlier tests
	cp := config.RootSection("ut")
	InitHTTPConfig(cp, 0)
	cc := config.RootSection("utCors")
	InitCORSConfig(cc)
	tlsSection := cp.SubSection("tls")
	cp.Set(HTTPConfAddress, "127.0.0.1")
	tlsSection.Set(fftls.HTTPConfTLSEnabled, true)
	tlsSection.Set(fftls.HTTPConfTLSClientAuth, true)
	tlsSection.Set(fftls.HTTPConfTLSKeyFile, privateKeyFile.Name())
	tlsSection.Set(fftls.HTTPConfTLSCertFile, publicKeyFile.Name())
	tlsSection.Set(fftls.HTTPConfTLSCAFile, publicKeyFile.Name())
	cp.Set(HTTPConfPort, 0)
	ctx, cancelCtx := context.WithCancel(context.Background())
	r := mux.NewRouter()
	r.HandleFunc("/test", func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		json.NewEncoder(res).Encode(map[string]interface{}{"hello": "world"})
	})
	errChan := make(chan error)
	hs, err := NewHTTPServer(context.Background(), "ut", r, errChan, cp, cc)
	assert.NoError(t, err)
	go hs.ServeHTTP(ctx)

	// Attempt a request, with a client certificate
	rootCAs := x509.NewCertPool()
	caPEM, _ := os.ReadFile(publicKeyFile.Name())
	ok := rootCAs.AppendCertsFromPEM(caPEM)
	assert.True(t, ok)
	c := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
					clientKeyPair, err := tls.LoadX509KeyPair(publicKeyFile.Name(), privateKeyFile.Name())
					return &clientKeyPair, err
				},
				RootCAs: rootCAs,
			},
		},
	}
	httpsAddr := fmt.Sprintf("https://%s/test", hs.(*httpServer).l.Addr().String())
	res, err := c.Get(httpsAddr)
	assert.NoError(t, err)
	if res != nil {
		assert.Equal(t, 200, res.StatusCode)
		var resBody map[string]interface{}
		json.NewDecoder(res.Body).Decode(&resBody)
		assert.Equal(t, "world", resBody["hello"])
	}

	// Close down the server and wait for it to complete
	cancelCtx()
	err = <-errChan
	assert.NoError(t, err)
}

func TestServeAuthorization(t *testing.T) {
	config.RootConfigReset()
	cp := config.RootSection("ut")
	InitHTTPConfig(cp, 0)
	cp.Set(HTTPAuthType, "basic")
	ac := cp.SubSection("auth")
	// authfactory.InitConfig(ac)
	bc := ac.SubSection("basic")
	bc.Set(basic.PasswordFile, "../../test/data/test_users")
	cc := config.RootSection("utCors")
	InitCORSConfig(cc)

	r := mux.NewRouter()
	r.HandleFunc("/test", func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		json.NewEncoder(res).Encode(map[string]interface{}{"hello": "world"})
		remoteAddr := RemoteAddr(req.Context())
		localAddr := LocalAddr(req.Context())
		assert.NotNil(t, remoteAddr)
		assert.NotNil(t, localAddr)
	})
	errChan := make(chan error)
	hs, err := NewHTTPServer(context.Background(), "ut", r, errChan, cp, cc)
	assert.NoError(t, err)

	go hs.ServeHTTP(context.Background())

	c := http.Client{}
	httpAddr := fmt.Sprintf("http://%s/test", hs.(*httpServer).l.Addr().String())
	res, err := c.Get(httpAddr)
	assert.NoError(t, err)
	if res != nil {
		assert.Equal(t, 403, res.StatusCode)
		var resBody map[string]interface{}
		json.NewDecoder(res.Body).Decode(&resBody)
		assert.Equal(t, "FF00169: Unauthorized", resBody["error"])
	}

	req, err := http.NewRequest("GET", httpAddr, nil)
	assert.NoError(t, err)
	req.SetBasicAuth("firefly", "awesome")
	c = http.Client{}
	res, err = c.Do(req)
	assert.NoError(t, err)
	if res != nil {
		assert.Equal(t, 200, res.StatusCode)
		var resBody map[string]interface{}
		json.NewDecoder(res.Body).Decode(&resBody)
		assert.Equal(t, "world", resBody["hello"])
	}
}

func TestServeAuthorizationBadPluginName(t *testing.T) {
	config.RootConfigReset()
	cp := config.RootSection("ut")
	InitHTTPConfig(cp, 0)
	cp.Set(HTTPAuthType, "banana")
	ac := cp.SubSection("auth")
	// authfactory.InitConfig(ac)
	bc := ac.SubSection("basic")
	bc.Set(basic.PasswordFile, "../../test/data/test_users")
	cc := config.RootSection("utCors")
	InitCORSConfig(cc)

	r := mux.NewRouter()
	r.HandleFunc("/test", func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		json.NewEncoder(res).Encode(map[string]interface{}{"hello": "world"})
	})
	errChan := make(chan error)
	_, err := NewHTTPServer(context.Background(), "ut", r, errChan, cp, cc)
	assert.Regexp(t, "FF00168", err)
}
