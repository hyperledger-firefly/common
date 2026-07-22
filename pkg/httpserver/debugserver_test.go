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

package httpserver

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/stretchr/testify/assert"
)

func TestDebugServerGoRoutineDump(t *testing.T) {

	ctx, cancelCtx := context.WithCancel(context.Background())

	config.RootConfigReset()
	utconf := config.RootSection("ut")
	InitDebugConfig(utconf)

	done := make(chan struct{})
	port := make(chan string)
	go func() {
		RunDebugServer(ctx, utconf, func(addr net.Addr) {
			port <- strings.Split(addr.String(), ":")[1]
		})
		close(done)
	}()

	res, err := resty.New().R().
		SetDoNotParseResponse(true).
		Get(fmt.Sprintf("http://localhost:%s/debug/pprof/goroutine?debug=2", <-port))
	assert.NoError(t, err)
	assert.True(t, res.IsSuccess())
	body, err := io.ReadAll(res.RawBody())
	assert.NoError(t, err)
	// We should find ourselves in the output
	assert.Regexp(t, "TestDebugServerGoRoutineDump", string(body))

	cancelCtx()
	<-done

}

func TestDebugServerDisabled(t *testing.T) {

	ctx, cancelCtx := context.WithCancel(context.Background())

	config.RootConfigReset()
	utconf := config.RootSection("ut")
	InitDebugConfig(utconf)

	done := make(chan struct{})
	go func() {
		RunDebugServer(ctx, utconf)
		close(done)
	}()

	cancelCtx()
	<-done

}

func TestDebugServerBadConfig(t *testing.T) {

	ctx, cancelCtx := context.WithCancel(context.Background())

	config.RootConfigReset()
	utconf := config.RootSection("ut")
	InitDebugConfig(utconf)
	utconf.Set(HTTPConfAddress, "-1")

	done := make(chan struct{})
	go func() {
		RunDebugServer(ctx, utconf)
		close(done)
	}()

	cancelCtx()
	<-done

}
