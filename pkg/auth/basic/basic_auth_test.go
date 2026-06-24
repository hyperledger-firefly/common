// Copyright © 2022 Kaleido, Inc.
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

package basic

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/stretchr/testify/assert"
)

func getTestBasicAuth(t *testing.T) *Auth {
	config.RootConfigReset()
	section := config.RootSection("auth")
	a := &Auth{}
	a.InitConfig(section)
	section.Set(PasswordFile, "../../../test/data/test_users")
	err := a.Init(context.Background(), "basic", section)
	assert.NoError(t, err)
	return a
}

func TestName(t *testing.T) {
	a := getTestBasicAuth(t)
	assert.Equal(t, "basic", a.Name())
}

func TestAuthorize(t *testing.T) {
	a := getTestBasicAuth(t)
	authReq := &fftypes.AuthReq{
		Header: http.Header{
			"Authorization": []string{"Basic ZmlyZWZseTphd2Vzb21l"},
		},
	}
	url, err := url.Parse("http://localhost/api")
	assert.NoError(t, err)
	err = a.Authorize(context.Background(), authReq)
	assert.NoError(t, err)

	authReq = &fftypes.AuthReq{
		URL: url,
		Header: http.Header{
			"Authorization": []string{"Basic ZmlyZWZseTppbmNvcnJlY3Q="},
		},
	}
	err = a.Authorize(context.Background(), authReq)
	assert.Regexp(t, "FF00169", err)

	authReq = &fftypes.AuthReq{
		URL: url,
		Header: http.Header{
			"Authorization": []string{"Not base64"},
		},
	}
	err = a.Authorize(context.Background(), authReq)
	assert.Regexp(t, "FF00169", err)

	authReq = &fftypes.AuthReq{
		URL: url,
		Header: http.Header{
			"Authorization": []string{"Basic Not base64"},
		},
	}
	err = a.Authorize(context.Background(), authReq)
	assert.Regexp(t, "FF00169", err)

	authReq = &fftypes.AuthReq{
		URL:    url,
		Header: nil,
	}
	err = a.Authorize(context.Background(), authReq)
	assert.Regexp(t, "FF00169", err)
}

func TestReadPasswordFileMissing(t *testing.T) {
	config.RootConfigReset()
	section := config.RootSection("auth")
	a := &Auth{}
	a.InitConfig(section)
	section.Set(PasswordFile, "file_missing")
	err := a.Init(context.Background(), "basic", section)
	assert.Regexp(t, "no such file or directory", err)
}
