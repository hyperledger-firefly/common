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

package ffresty

import (
	"bytes"
	"compress/gzip"
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
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly-common/pkg/ffdns"
	"github.com/hyperledger/firefly-common/pkg/ffnet"
	"github.com/hyperledger/firefly-common/pkg/fftls"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/metric"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const configDir = "../../test/data/config"

var utConf = config.RootSection("http_unit_tests")

func resetConf() {
	config.RootConfigReset()
	InitConfig(utConf)
}

func TestRequestOK(t *testing.T) {

	customClient := &http.Client{}

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigHeaders, map[string]interface{}{
		"someheader": "headervalue",
	})
	utConf.Set(HTTPConfigAuthUsername, "user")
	utConf.Set(HTTPConfigAuthPassword, "pass")
	utConf.Set(HTTPConfigRetryEnabled, true)
	utConf.Set(HTTPCustomClient, customClient)

	c, err := New(context.Background(), utConf)
	assert.Nil(t, err)
	httpmock.ActivateNonDefault(customClient)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "http://localhost:12345/test",
		func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, "headervalue", req.Header.Get("someheader"))
			assert.Equal(t, "Basic dXNlcjpwYXNz", req.Header.Get("Authorization"))
			return httpmock.NewStringResponder(200, `{"some": "data"}`)(req)
		})

	resp, err := c.R().Get("/test")
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode())
	assert.Equal(t, `{"some": "data"}`, resp.String())

	assert.Equal(t, 1, httpmock.GetTotalCallCount())
}

func TestRequestOKForGzip(t *testing.T) {

	customClient := &http.Client{}

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigHeaders, map[string]interface{}{
		"someheader": "headervalue",
	})
	utConf.Set(HTTPConfigAuthUsername, "user")
	utConf.Set(HTTPConfigAuthPassword, "pass")
	utConf.Set(HTTPConfigRetryEnabled, true)
	utConf.Set(HTTPCustomClient, customClient)

	c, err := New(context.Background(), utConf)
	assert.Nil(t, err)
	httpmock.ActivateNonDefault(customClient)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "http://localhost:12345/test",
		func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, "headervalue", req.Header.Get("someheader"))
			assert.Equal(t, "Basic dXNlcjpwYXNz", req.Header.Get("Authorization"))
			resp := httpmock.NewStringResponse(200, `{"some": "data"}`)
			resp.Header.Set("Content-Encoding", "gzip")
			var b bytes.Buffer
			gz := gzip.NewWriter(&b)
			if _, err := gz.Write([]byte(`{"some": "data"}`)); err != nil {
				return nil, err
			}
			if err := gz.Close(); err != nil {
				return nil, err
			}
			resp.Body = io.NopCloser(&b)
			return resp, nil
		})

	resp, err := c.R().Get("/test")
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode())
	assert.Equal(t, `{"some": "data"}`, resp.String())

	assert.Equal(t, 1, httpmock.GetTotalCallCount())
}

func TestRequestWithRateLimiter(t *testing.T) {
	rps := 5
	expectedNumberOfRequest := 20 // should take longer than 3 seconds less than 4 seconds

	customClient := &http.Client{}

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigHeaders, map[string]interface{}{
		"someheader": "headervalue",
	})
	utConf.Set(HTTPConfigAuthUsername, "user")
	utConf.Set(HTTPConfigAuthPassword, "pass")
	utConf.Set(HTTPThrottleRequestsPerSecond, rps)
	utConf.Set(HTTPConfigRetryEnabled, true)
	utConf.Set(HTTPCustomClient, customClient)

	c, err := New(context.Background(), utConf)
	assert.Nil(t, err)
	httpmock.ActivateNonDefault(customClient)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "http://localhost:12345/test",
		func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, "headervalue", req.Header.Get("someheader"))
			assert.Equal(t, "Basic dXNlcjpwYXNz", req.Header.Get("Authorization"))
			return httpmock.NewStringResponder(200, `{"some": "data"}`)(req)
		})
	requestChan := make(chan bool, expectedNumberOfRequest)
	startTime := time.Now()
	for i := 0; i < expectedNumberOfRequest; i++ {
		go func() {
			resp, err := c.R().Get("/test")
			assert.NoError(t, err)
			assert.Equal(t, 200, resp.StatusCode())
			assert.Equal(t, `{"some": "data"}`, resp.String())
			requestChan <- true
		}()
	}
	count := 0
	for {
		<-requestChan
		count++
		if count == expectedNumberOfRequest {
			break

		}
	}

	duration := time.Since(startTime)
	assert.GreaterOrEqual(t, duration, 3*time.Second)
	assert.LessOrEqual(t, duration, 4*time.Second)
	assert.Equal(t, expectedNumberOfRequest, httpmock.GetTotalCallCount())
}

func TestRequestWithRateLimiterHighBurst(t *testing.T) {
	expectedNumberOfRequest := 20 // allow all requests to be processed within 1 second

	customClient := &http.Client{}

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigHeaders, map[string]interface{}{
		"someheader": "headervalue",
	})
	utConf.Set(HTTPConfigAuthUsername, "user")
	utConf.Set(HTTPConfigAuthPassword, "pass")
	utConf.Set(HTTPThrottleRequestsPerSecond, 0)
	utConf.Set(HTTPThrottleBurst, expectedNumberOfRequest)
	utConf.Set(HTTPConfigRetryEnabled, true)
	utConf.Set(HTTPCustomClient, customClient)

	c, err := New(context.Background(), utConf)
	assert.Nil(t, err)
	httpmock.ActivateNonDefault(customClient)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "http://localhost:12345/test",
		func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, "headervalue", req.Header.Get("someheader"))
			assert.Equal(t, "Basic dXNlcjpwYXNz", req.Header.Get("Authorization"))
			return httpmock.NewStringResponder(200, `{"some": "data"}`)(req)
		})
	requestChan := make(chan bool, expectedNumberOfRequest)
	startTime := time.Now()
	for i := 0; i < expectedNumberOfRequest; i++ {
		go func() {
			resp, err := c.R().Get("/test")
			assert.NoError(t, err)
			assert.Equal(t, 200, resp.StatusCode())
			assert.Equal(t, `{"some": "data"}`, resp.String())
			requestChan <- true
		}()
	}
	count := 0
	for {
		<-requestChan
		count++
		if count == expectedNumberOfRequest {
			break

		}
	}

	duration := time.Since(startTime)
	assert.Less(t, duration, 1*time.Second)
	assert.Equal(t, expectedNumberOfRequest, httpmock.GetTotalCallCount())
}

func TestRateLimiterFailure(t *testing.T) {
	customClient := &http.Client{}

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigHeaders, map[string]interface{}{
		"someheader": "headervalue",
	})
	utConf.Set(HTTPConfigAuthUsername, "user")
	utConf.Set(HTTPConfigAuthPassword, "pass")
	utConf.Set(HTTPConfigRetryEnabled, true)

	utConf.Set(HTTPCustomClient, customClient)

	getRateLimiter = func(_, _ int) (rl *rate.Limiter) {
		return rate.NewLimiter(rate.Limit(1), 0)
	}

	c, err := New(context.Background(), utConf)
	assert.Nil(t, err)
	httpmock.ActivateNonDefault(customClient)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "http://localhost:12345/test",
		func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, "headervalue", req.Header.Get("someheader"))
			assert.Equal(t, "Basic dXNlcjpwYXNz", req.Header.Get("Authorization"))
			return httpmock.NewStringResponder(200, `{"some": "data"}`)(req)
		})
	resp, err := c.R().Get("/test")
	assert.Error(t, err)
	assert.Regexp(t, "exceeds", err)
	assert.Nil(t, resp)
	getRateLimiter = GetRateLimiter
}

func TestRequestRetry(t *testing.T) {

	ctx := context.Background()

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigRetryEnabled, true)
	utConf.Set(HTTPConfigRetryInitDelay, 1)

	c, err := New(ctx, utConf)
	assert.Nil(t, err)
	httpmock.ActivateNonDefault(c.GetClient())
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "http://localhost:12345/test",
		httpmock.NewStringResponder(500, `{"message": "pop"}`))

	resp, err := c.R().Get("/test")
	assert.NoError(t, err)
	assert.Equal(t, 500, resp.StatusCode())
	assert.Equal(t, 6, httpmock.GetTotalCallCount())

	err = WrapRestErr(ctx, resp, err, i18n.MsgConfigFailed)
	assert.Error(t, err)

}

func TestRequestRetryErrorStatusCodeRegex(t *testing.T) {

	ctx := context.Background()

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigRetryEnabled, true)
	utConf.Set(HTTPConfigRetryInitDelay, 1)
	utConf.Set(HTTPConfigRetryErrorStatusCodeRegex, "(?:429|503)")

	c, err := New(ctx, utConf)
	assert.Nil(t, err)
	httpmock.ActivateNonDefault(c.GetClient())
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "http://localhost:12345/test",
		httpmock.NewStringResponder(500, `{"message": "pop"}`))

	httpmock.RegisterResponder("GET", "http://localhost:12345/test2",
		httpmock.NewStringResponder(429, `{"message": "pop"}`))

	httpmock.RegisterResponder("GET", "http://localhost:12345/test3",
		httpmock.NewErrorResponder(errors.New("not http response")))

	resp, err := c.R().Get("/test")
	assert.NoError(t, err)
	assert.Equal(t, 500, resp.StatusCode())
	assert.Equal(t, 1, httpmock.GetTotalCallCount())

	err = WrapRestErr(ctx, resp, err, i18n.MsgConfigFailed)
	assert.Error(t, err)

	resp, err = c.R().Get("/test2")
	assert.NoError(t, err)
	assert.Equal(t, 429, resp.StatusCode())
	assert.Equal(t, 7, httpmock.GetTotalCallCount())

	err = WrapRestErr(ctx, resp, err, i18n.MsgConfigFailed)
	assert.Error(t, err)

	resp, err = c.R().Get("/test3")
	assert.Error(t, err)
	assert.Equal(t, 0, resp.StatusCode())
	assert.Equal(t, 13, httpmock.GetTotalCallCount())

	err = WrapRestErr(ctx, resp, err, i18n.MsgConfigFailed)
	assert.Error(t, err)

}
func TestConfWithProxy(t *testing.T) {

	ctx := context.Background()

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigProxyURL, "http://myproxy.example.com:12345")
	utConf.Set(HTTPConfigRetryEnabled, false)

	c, err := New(ctx, utConf)
	assert.Nil(t, err)
	assert.True(t, c.IsProxySet())
}

func TestLongResponse(t *testing.T) {

	ctx := context.Background()

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigRetryEnabled, false)

	c, err := New(ctx, utConf)
	assert.Nil(t, err)
	httpmock.ActivateNonDefault(c.GetClient())
	defer httpmock.DeactivateAndReset()

	resText := strings.Builder{}
	for i := 0; i < 512; i++ {
		resText.WriteByte(byte('a' + (i % 26)))
	}
	httpmock.RegisterResponder("GET", "http://localhost:12345/test",
		httpmock.NewStringResponder(500, resText.String()))

	resp, err := c.R().Get("/test")
	err = WrapRestErr(ctx, resp, err, i18n.MsgConfigFailed)
	assert.Error(t, err)
}

func TestErrResponse(t *testing.T) {

	ctx := context.Background()

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigRetryEnabled, false)

	c, err := New(ctx, utConf)
	assert.Nil(t, err)
	httpmock.ActivateNonDefault(c.GetClient())
	defer httpmock.DeactivateAndReset()

	resText := strings.Builder{}
	for i := 0; i < 512; i++ {
		resText.WriteByte(byte('a' + (i % 26)))
	}
	httpmock.RegisterResponder("GET", "http://localhost:12345/test",
		httpmock.NewErrorResponder(fmt.Errorf("pop")))

	resp, err := c.R().Get("/test")
	err = WrapRestErr(ctx, resp, err, i18n.MsgConfigFailed)
	assert.Error(t, err)
}

func TestErrResponseBlockRetry(t *testing.T) {

	ctx := context.Background()

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigRetryEnabled, true)
	utConf.Set(HTTPConfigRetryInitDelay, "1ms")
	utConf.Set(HTTPConfigRetryCount, 10000)

	ffrestyConfig, err := GenerateConfig(ctx, utConf)
	assert.NoError(t, err)
	called := make(chan bool, 1)
	ffrestyConfig.OnCheckRetry = func(res *resty.Response, err error) bool {
		assert.NotNil(t, res) // We expect a response object, even though it was an error
		assert.NotNil(t, err)
		called <- true
		return false
	}

	c := NewWithConfig(ctx, *ffrestyConfig)
	httpmock.ActivateNonDefault(c.GetClient())
	defer httpmock.DeactivateAndReset()

	resText := strings.Builder{}
	for i := 0; i < 512; i++ {
		resText.WriteByte(byte('a' + (i % 26)))
	}
	httpmock.RegisterResponder("POST", "http://localhost:12345/test",
		httpmock.NewErrorResponder(fmt.Errorf("pop")))

	resp, err := c.R().SetBody("stuff").Post("/test")
	err = WrapRestErr(ctx, resp, err, i18n.MsgConfigFailed)
	assert.Error(t, err)

	<-called
}

func TestRetryReGenHeaderOnEachRequest(t *testing.T) {

	ctx := context.Background()

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigRetryEnabled, true)
	utConf.Set(HTTPConfigRetryInitDelay, "1ms")
	utConf.Set(HTTPConfigRetryCount, 1)

	ffrestyConfig, err := GenerateConfig(ctx, utConf)
	assert.NoError(t, err)
	reqCount := 0
	ffrestyConfig.OnBeforeRequest = func(req *resty.Request) error {
		reqCount++
		req.Header.Set("utheader", fmt.Sprintf("request_%d", reqCount))
		return nil
	}

	c := NewWithConfig(ctx, *ffrestyConfig)
	httpmock.ActivateNonDefault(c.GetClient())
	defer httpmock.DeactivateAndReset()

	resText := strings.Builder{}
	for i := 0; i < 512; i++ {
		resText.WriteByte(byte('a' + (i % 26)))
	}
	utHeaders := make(chan string, 2)
	httpmock.RegisterResponder("POST", "http://localhost:12345/test", func(req *http.Request) (*http.Response, error) {
		utHeaders <- req.Header.Get("utheader")
		return &http.Response{StatusCode: 500, Body: http.NoBody}, nil
	})

	resp, err := c.R().SetBody("stuff").Post("/test")
	err = WrapRestErr(ctx, resp, err, i18n.MsgConfigFailed)
	assert.Error(t, err)

	assert.Equal(t, "request_1", <-utHeaders)
	assert.Equal(t, "request_2", <-utHeaders)
}

func TestRetryReGenHeaderOnEachRequestFail(t *testing.T) {

	ctx := context.Background()

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigRetryEnabled, true)
	utConf.Set(HTTPConfigRetryInitDelay, "1ms")
	utConf.Set(HTTPConfigRetryCount, 1)

	ffrestyConfig, err := GenerateConfig(ctx, utConf)
	assert.NoError(t, err)
	ffrestyConfig.OnBeforeRequest = func(req *resty.Request) error {
		return fmt.Errorf("pop")
	}

	c := NewWithConfig(ctx, *ffrestyConfig)
	httpmock.ActivateNonDefault(c.GetClient())
	defer httpmock.DeactivateAndReset()

	resp, err := c.R().SetBody("stuff").Post("/test")
	err = WrapRestErr(ctx, resp, err, i18n.MsgConfigFailed)
	assert.Regexp(t, "pop", err)
}

func TestOnAfterResponseNil(t *testing.T) {
	OnAfterResponse(nil, nil)
}

func TestPassthroughHeaders(t *testing.T) {
	passthroughHeaders := http.Header{}
	passthroughHeaders.Set("X-Custom-Header", "custom value")
	ctx := context.WithValue(context.Background(), ffapi.CtxHeadersKey{}, passthroughHeaders)
	ctx = context.WithValue(ctx, ffapi.CtxFFRequestIDKey{}, "customReqID")

	customClient := &http.Client{}

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigHeaders, map[string]interface{}{
		"someheader": "headervalue",
	})
	utConf.Set(HTTPConfigAuthUsername, "user")
	utConf.Set(HTTPConfigAuthPassword, "pass")
	utConf.Set(HTTPConfigRetryEnabled, true)
	utConf.Set(HTTPCustomClient, customClient)
	utConf.Set(HTTPPassthroughHeadersEnabled, true)

	c, err := New(context.Background(), utConf)
	assert.Nil(t, err)
	httpmock.ActivateNonDefault(customClient)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "http://localhost:12345/test",
		func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, "customReqID", req.Header.Get(ffapi.RequestIDHeader()))
			assert.Equal(t, "headervalue", req.Header.Get("someheader"))
			assert.Equal(t, "custom value", req.Header.Get("X-Custom-Header"))
			assert.Equal(t, "Basic dXNlcjpwYXNz", req.Header.Get("Authorization"))
			return httpmock.NewStringResponder(200, `{"some": "data"}`)(req)
		})

	resp, err := c.R().SetContext(ctx).Get("/test")
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode())
	assert.Equal(t, `{"some": "data"}`, resp.String())

	assert.Equal(t, 1, httpmock.GetTotalCallCount())
}

func TestMissingCAFile(t *testing.T) {
	resetConf()
	utConf.Set(HTTPConfigURL, "https://localhost:12345")
	tlsSection := utConf.SubSection("tls")
	tlsSection.Set(fftls.HTTPConfTLSEnabled, true)
	tlsSection.Set(fftls.HTTPConfTLSCAFile, "non-existent.pem")

	_, err := New(context.Background(), utConf)
	assert.Regexp(t, "FF00153", err)
}

func TestBadCAFile(t *testing.T) {
	resetConf()
	utConf.Set(HTTPConfigURL, "https://localhost:12345")
	tlsSection := utConf.SubSection("tls")
	tlsSection.Set(fftls.HTTPConfTLSEnabled, true)
	tlsSection.Set(fftls.HTTPConfTLSCAFile, configDir+"/firefly.common.yaml")

	_, err := New(context.Background(), utConf)
	assert.Regexp(t, "FF00152", err)
}

func TestBadKeyPair(t *testing.T) {
	resetConf()
	utConf.Set(HTTPConfigURL, "https://localhost:12345")
	tlsSection := utConf.SubSection("tls")
	tlsSection.Set(fftls.HTTPConfTLSEnabled, true)
	tlsSection.Set(fftls.HTTPConfTLSCertFile, configDir+"/firefly.common.yaml")
	tlsSection.Set(fftls.HTTPConfTLSKeyFile, configDir+"/firefly.common.yaml")

	_, err := New(context.Background(), utConf)
	assert.Regexp(t, "FF00206", err)
}

func TestMTLSClientWithServer(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

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

	http.HandleFunc("/hello", func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		json.NewEncoder(res).Encode(map[string]interface{}{"hello": "world"})
	})

	// Create a CA certificate pool and add cert.pem to it
	caCert, err := os.ReadFile(publicKeyFile.Name())
	if err != nil {
		log.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Create the TLS Config with the CA pool and enable Client certificate validation
	tlsConfig := &tls.Config{
		ClientCAs:  caCertPool,
		ClientAuth: tls.RequireAndVerifyClientCert,
	}
	tlsConfig.BuildNameToCertificate()

	// Create a Server instance to listen on port 8443 with the TLS config
	server := &http.Server{
		TLSConfig: tlsConfig,
	}

	ctx, cancelCtx := context.WithCancel(context.Background())
	go func() {
		<-ctx.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			return
		}
	}()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}

	defer ln.Close()

	go server.ServeTLS(ln, publicKeyFile.Name(), privateKeyFile.Name())

	// Use ffresty to test the mTLS client as well
	var restyConfig = config.RootSection("resty")
	InitConfig(restyConfig)
	clientTLSSection := restyConfig.SubSection("tls")
	restyConfig.Set(HTTPConfigURL, ln.Addr()) // note this does not have https:// in the URL
	clientTLSSection.Set(fftls.HTTPConfTLSEnabled, true)
	clientTLSSection.Set(fftls.HTTPConfTLSKeyFile, privateKeyFile.Name())
	clientTLSSection.Set(fftls.HTTPConfTLSCertFile, publicKeyFile.Name())
	clientTLSSection.Set(fftls.HTTPConfTLSCAFile, publicKeyFile.Name())

	c, err := New(context.Background(), restyConfig)
	assert.Nil(t, err)

	httpsAddr := fmt.Sprintf("https://%s/hello", ln.Addr())
	fmt.Println(httpsAddr)
	res, err := c.R().Get(httpsAddr)
	assert.NoError(t, err)

	assert.NoError(t, err)
	if res != nil {
		assert.Equal(t, 200, res.StatusCode())
		var resBody map[string]interface{}
		err = json.Unmarshal(res.Body(), &resBody)
		assert.NoError(t, err)
		assert.Equal(t, "world", resBody["hello"])
	}
	cancelCtx()
}

func TestEnableClientMetricsError(t *testing.T) {
	ctx := context.Background()
	mr := metric.NewPrometheusMetricsRegistry("testerr")
	//claim the "resty subsystem before resty can :/"
	_, _ = mr.NewMetricsManagerForSubsystem(ctx, "resty")
	err := EnableClientMetrics(ctx, mr)
	assert.Error(t, err)
}

func TestEnableClientMetrics(t *testing.T) {

	ctx := context.Background()
	mr := metric.NewPrometheusMetricsRegistry("test")

	err := EnableClientMetrics(ctx, mr)
	assert.NoError(t, err)
}

func TestEnableClientMetricsIdempotent(t *testing.T) {
	ctx := context.Background()
	mr := metric.NewPrometheusMetricsRegistry("test")
	_ = EnableClientMetrics(ctx, mr)
	err := EnableClientMetrics(ctx, mr)
	assert.NoError(t, err)
}

func TestHooks(t *testing.T) {

	ctx := context.Background()
	mr := metric.NewPrometheusMetricsRegistry("test")

	err := EnableClientMetrics(ctx, mr)
	assert.NoError(t, err)

	onErrorCount := 0
	onSuccessCount := 0

	customOnError := func(req *resty.Request, err error) {
		onErrorCount++
	}

	customOnSuccess := func(c *resty.Client, resp *resty.Response) {
		assert.Equal(t, "localhost:12345", resp.Request.Context().Value(hostCtxKey{}).(string))
		onSuccessCount++
	}

	RegisterGlobalOnError(customOnError)
	RegisterGlobalOnSuccess(customOnSuccess)

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigRetryEnabled, false)

	c, err := New(ctx, utConf)
	assert.Nil(t, err)
	httpmock.ActivateNonDefault(c.GetClient())
	defer httpmock.DeactivateAndReset()

	resText := strings.Builder{}
	for i := 0; i < 512; i++ {
		resText.WriteByte(byte('a' + (i % 26)))
	}
	httpmock.RegisterResponder("GET", "http://localhost:12345/testerr",
		httpmock.NewErrorResponder(fmt.Errorf("pop")))

	httpmock.RegisterResponder("GET", "http://localhost:12345/testerrhttp",
		func(req *http.Request) (*http.Response, error) {
			return httpmock.NewStringResponse(502, `{"Status": "Service Not Available"}`), fmt.Errorf("Service Not Available")
		})

	httpmock.RegisterResponder("GET", "http://localhost:12345/testok",
		func(req *http.Request) (*http.Response, error) {
			return httpmock.NewStringResponder(200, `{"some": "data"}`)(req)
		})

	resp, err := c.R().Get("/testerr")
	err = WrapRestErr(ctx, resp, err, i18n.MsgConfigFailed)
	assert.Error(t, err)

	assert.Equal(t, onErrorCount, 1)
	assert.Equal(t, onSuccessCount, 0)

	resp, err = c.R().Get("/testerrhttp")
	err = WrapRestErr(ctx, resp, err, i18n.MsgConfigFailed)
	assert.Error(t, err)

	assert.Equal(t, onErrorCount, 2)
	assert.Equal(t, onSuccessCount, 0)

	resp, err = c.R().Get("/testok")
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode())
	assert.Equal(t, `{"some": "data"}`, resp.String())

	assert.Equal(t, 3, httpmock.GetTotalCallCount())

	assert.Equal(t, onErrorCount, 2)
	assert.Equal(t, onSuccessCount, 1)

}

func TestTrace(t *testing.T) {
	require.Equal(t, "plain", traceBody("plain"))
	require.Equal(t, "plain", traceBody([]byte("plain")))
	require.Equal(t, "", traceBody(nil))
	require.JSONEq(t, `{"some":"data"}`, traceBody(map[string]string{"some": "data"}))
	require.Equal(t, `(binary reader)`, traceBody(strings.NewReader("data to stream")))
}

func TestNewWithConfigResolverWired(t *testing.T) {
	// A programmatic resolver (e.g. built from the dns config subsection by ffdns) is
	// attached to the transport's dialer rather than forcing the all-or-nothing custom client.
	ctx := context.Background()
	c := NewWithConfig(ctx, Config{HTTPConfig: HTTPConfig{
		Resolver: &net.Resolver{PreferGo: true},
	}})
	require.NotNil(t, c)
	transport, ok := c.GetClient().Transport.(*http.Transport)
	require.True(t, ok)
	assert.NotNil(t, transport.DialContext)
}

func TestDialControlBlocksLoopbackWhenConfigured(t *testing.T) {
	// With an SSRF denylist configured, the client blocks loopback before connecting
	resetConf()
	ssrfDenylist := []string{
		"127.0.0.0/8",
	}
	utConf.SubSection("net").Set(ffnet.NetCIDRDenylist, ssrfDenylist)
	utConf.Set(HTTPConfigURL, "http://127.0.0.1:1")
	c, err := New(context.Background(), utConf)
	require.NoError(t, err)
	_, err = c.R().Get("/")
	assert.Regexp(t, "FF00261", err)
}

func TestGenerateConfigDNSResolver(t *testing.T) {
	// With dns.servers configured, GenerateConfig populates a resolver via ffdns
	resetConf()
	utConf.SubSection("dns").Set(ffdns.DNSServers, []string{"8.8.8.8"})
	cfg, err := GenerateConfig(context.Background(), utConf)
	assert.NoError(t, err)
	assert.NotNil(t, cfg.Resolver)

	// With no dns.servers, no resolver is built and Go's default selection stays in place
	resetConf()
	cfg, err = GenerateConfig(context.Background(), utConf)
	assert.NoError(t, err)
	assert.Nil(t, cfg.Resolver)
}
