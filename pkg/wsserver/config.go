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

package wsserver

import (
	"time"

	"github.com/hyperledger-firefly/common/pkg/config"
)

type WebSocketServerConfig struct {
	AckTimeout      time.Duration
	ReadBufferSize  int64
	WriteBufferSize int64
}

const (
	ConfigAckTimeout      = "ackTimeout"
	ConfigReadBufferSize  = "readBufferSize"
	ConfigWriteBufferSize = "writeBufferSize"
)

func InitConfig(conf config.Section) {
	conf.AddKnownKey(ConfigAckTimeout, "2m")
	conf.AddKnownKey(ConfigReadBufferSize, "4KB")
	conf.AddKnownKey(ConfigWriteBufferSize, "4KB")
}

func GenerateConfig(conf config.Section) *WebSocketServerConfig {
	return &WebSocketServerConfig{
		AckTimeout:      conf.GetDuration(ConfigAckTimeout),
		ReadBufferSize:  conf.GetByteSize(ConfigReadBufferSize),
		WriteBufferSize: conf.GetByteSize(ConfigWriteBufferSize),
	}
}
