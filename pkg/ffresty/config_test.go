// Copyright © 2023 Kaleido, Inc.
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
	"context"
	"testing"

	"github.com/hyperledger/firefly-common/pkg/ffdns"
	"github.com/hyperledger/firefly-common/pkg/fftls"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/stretchr/testify/assert"
)

func TestWSConfigGeneration(t *testing.T) {
	resetConf()

	utConf.Set(HTTPConfigURL, "http://test:12345")
	utConf.Set(HTTPConfigProxyURL, "http://proxy:12345")
	utConf.Set(HTTPConfigHeaders, map[string]interface{}{
		"custom-header": "custom value",
	})
	utConf.Set(HTTPConfigAuthUsername, "user")
	utConf.Set(HTTPConfigAuthPassword, "pass")
	utConf.Set(HTTPConfigRetryEnabled, true)
	utConf.Set(HTTPConfigRetryInitDelay, 1)
	utConf.Set(HTTPConfigRetryMaxDelay, 1)
	utConf.Set(HTTPConfigRetryCount, 1)
	utConf.Set(HTTPConfigRetryErrorStatusCodeRegex, "(?:429|503)")
	utConf.Set(HTTPConfigRequestTimeout, 1)
	utConf.Set(HTTPIdleTimeout, 1)
	utConf.Set(HTTPMaxIdleConns, 1)
	utConf.Set(HTTPConnectionTimeout, 1)
	utConf.Set(HTTPTLSHandshakeTimeout, 1)
	utConf.Set(HTTPExpectContinueTimeout, 1)
	utConf.Set(HTTPPassthroughHeadersEnabled, true)
	utConf.SubSection("net").Set(ffdns.DNSServers, []string{"8.8.8.8", "1.1.1.1:53"})

	ctx := context.Background()
	config, err := GenerateConfig(ctx, utConf)
	assert.NoError(t, err)

	assert.Equal(t, "http://test:12345", config.URL)
	assert.Equal(t, "http://proxy:12345", config.ProxyURL)
	assert.Equal(t, "user", config.AuthUsername)
	assert.Equal(t, "pass", config.AuthPassword)
	assert.Equal(t, fftypes.FFDuration(1000000), config.RetryInitialDelay)
	assert.Equal(t, fftypes.FFDuration(1000000), config.RetryMaximumDelay)
	assert.Equal(t, "(?:429|503)", config.RetryErrorStatusCodeRegex)
	assert.Equal(t, 1, config.RetryCount)
	assert.Equal(t, true, config.Retry)
	assert.Equal(t, true, config.HTTPPassthroughHeadersEnabled)
	assert.Equal(t, fftypes.FFDuration(1000000), config.HTTPExpectContinueTimeout)
	assert.Equal(t, fftypes.FFDuration(1000000), config.HTTPIdleConnTimeout)
	assert.Equal(t, fftypes.FFDuration(1000000), config.HTTPTLSHandshakeTimeout)
	assert.Equal(t, fftypes.FFDuration(1000000), config.HTTPConnectionTimeout)
	assert.Equal(t, 1, config.HTTPMaxIdleConns)
	assert.Equal(t, "custom value", config.HTTPHeaders.GetString("custom-header"))
	// dns.servers drives a programmatic resolver built via ffdns
	assert.NotNil(t, config.Resolver)
}

func TestWSConfigTLSGenerationFail(t *testing.T) {
	resetConf()

	tlsSection := utConf.SubSection("tls")
	tlsSection.Set(fftls.HTTPConfTLSEnabled, true)
	tlsSection.Set(fftls.HTTPConfTLSCAFile, "bad-ca")

	ctx := context.Background()
	_, err := GenerateConfig(ctx, utConf)
	assert.Regexp(t, "FF00153", err)
}

func TestCheckAllFieldsDocumented(t *testing.T) {

}
