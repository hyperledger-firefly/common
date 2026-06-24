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
	"testing"

	"github.com/hyperledger-firefly/common/pkg/auth/basic"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/stretchr/testify/assert"
)

func TestServerAuthBasicAuth(t *testing.T) {
	config.RootConfigReset()
	section := config.RootSection("http")
	InitHTTPConfig(section, 5000)
	authConfig := section.SubSection("auth")
	section.Set(HTTPAuthType, "basic")
	basicAuthConfig := authConfig.SubSection("basic")
	basicAuthConfig.Set(basic.PasswordFile, "../../test/data/test_users")
	handlerWithAuth, err := wrapAuthIfEnabled(context.Background(), authConfig, "basic", nil)
	assert.NoError(t, err)
	assert.NotNil(t, handlerWithAuth)
}

func TestServerAuthBasicAuthBadConfig(t *testing.T) {
	config.RootConfigReset()
	section := config.RootSection("http")
	InitHTTPConfig(section, 5000)
	authConfig := section.SubSection("auth")
	section.Set(HTTPAuthType, "basic")
	basicAuthConfig := authConfig.SubSection("basic")
	basicAuthConfig.Set(basic.PasswordFile, "missing")
	_, err := wrapAuthIfEnabled(context.Background(), authConfig, "basic", nil)
	assert.Regexp(t, "no such file or directory", err)
}

func TestServerAuthBadPluginName(t *testing.T) {
	config.RootConfigReset()
	section := config.RootSection("http")
	InitHTTPConfig(section, 5000)
	handlerWithAuth, err := wrapAuthIfEnabled(context.Background(), section, "banana", nil)
	assert.Regexp(t, "FF00168", err)
	assert.Nil(t, handlerWithAuth)
}
