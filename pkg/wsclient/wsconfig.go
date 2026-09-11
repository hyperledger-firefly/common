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

package wsclient

import (
	"context"
	"net"
	"time"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/ffdns"
	"github.com/hyperledger-firefly/common/pkg/ffnet"
	"github.com/hyperledger-firefly/common/pkg/ffresty"
	"github.com/hyperledger-firefly/common/pkg/fftls"
)

const (
	defaultInitialConnectAttempts     = 5
	defaultBufferSize                 = "16Kb"
	defaultHeartbeatInterval          = "30s"            // up to a minute to detect a dead connection
	defaultConnectionTimeout          = 45 * time.Second // 45 seconds - the built in default for gorilla/websocket
	defaultRetryBackoffFactor         = 2.0
	defaultConnectionCycleQuiesceTime = "5s" // only used when connection cycling is enabled
)

const (
	// WSSpecificConfPrefix is the named sub-section of the http config options that contains websocket specific config
	WSSpecificConfPrefix = "ws"
	// WSConfigKeyWriteBufferSize is the write buffer size
	WSConfigKeyWriteBufferSize = "ws.writeBufferSize"
	// WSConfigKeyReadBufferSize is the read buffer size
	WSConfigKeyReadBufferSize = "ws.readBufferSize"
	// WSConfigKeyInitialConnectAttempts sets how many times the websocket should attempt to connect on startup, before failing (after initial connection, retry is indefinite)
	WSConfigKeyInitialConnectAttempts = "ws.initialConnectAttempts"
	// WSConfigKeyBackgroundConnect is recommended instead of initialConnectAttempts for new uses of this library, and makes initial connection and reconnection identical in behavior
	WSConfigKeyBackgroundConnect = "ws.backgroundConnect"
	// WSConfigKeyPath if set will define the path to connect to - allows sharing of the same URL between HTTP and WebSocket connection info
	WSConfigKeyPath = "ws.path"
	// WSConfigURL if set will be a completely separate URL for WebSockets (must be a ws: or wss: scheme)
	WSConfigURL = "ws.url"
	// WSConfigKeyHeartbeatInterval is the frequency of ping/pong requests, and also used for the timeout to receive a response to the heartbeat
	WSConfigKeyHeartbeatInterval = "ws.heartbeatInterval"
	// WSConnectionTimeout is the amount of time to wait while attempting to establish a connection (or automatic reconnection)
	WSConfigKeyConnectionTimeout = "ws.connectionTimeout"
	// WSConfigKeyConnectionCycleInterval when non-zero enables proactive cycling of the connection on this interval - the new connection is fully established before the old one is quiesced and closed
	WSConfigKeyConnectionCycleInterval = "ws.connectionCycleInterval"
	// WSConfigKeyConnectionCycleQuiesceTime is how long the old connection continues to deliver inbound messages after a connection cycle, before it is closed
	WSConfigKeyConnectionCycleQuiesceTime = "ws.connectionCycleQuiesceTime"
	// WSConfigDelayFactor the exponential backoff factor for delay
	WSConfigDelayFactor = "retry.factor"
)

// InitConfig ensures the config is initialized for HTTP too, as WS and HTTP
// can share the same tree of configuration (and all the HTTP options apply to the initial upgrade)
func InitConfig(conf config.Section) {
	ffresty.InitConfig(conf)
	conf.AddKnownKey(WSConfigKeyWriteBufferSize, defaultBufferSize)
	conf.AddKnownKey(WSConfigKeyReadBufferSize, defaultBufferSize)

	// Note that conf.SetDefault(WSConfigKeyBackgroundConnect, true) is recommended for implementations
	// that embed this library, which will cause continual exponential backoff retry connection
	// even on the initial connection.
	conf.AddKnownKey(WSConfigKeyBackgroundConnect, false)

	// Ignored if WSConfigKeyBackgroundConnect is true
	conf.AddKnownKey(WSConfigKeyInitialConnectAttempts, defaultInitialConnectAttempts)

	conf.AddKnownKey(WSConfigKeyPath)
	conf.AddKnownKey(WSConfigURL)
	conf.AddKnownKey(WSConfigKeyHeartbeatInterval, defaultHeartbeatInterval)
	conf.AddKnownKey(WSConfigKeyConnectionTimeout, defaultConnectionTimeout)
	conf.AddKnownKey(WSConfigKeyConnectionCycleInterval) // no default - connection cycling is disabled unless set
	conf.AddKnownKey(WSConfigKeyConnectionCycleQuiesceTime, defaultConnectionCycleQuiesceTime)
	conf.AddKnownKey(WSConfigDelayFactor, defaultRetryBackoffFactor)
	InitConfigWrap(conf)
}

func InitConfigWrap(conf config.Section) {
	conf.AddKnownKey(WSConfigKeyHeartbeatInterval, defaultHeartbeatInterval)
}

func GenerateConfig(ctx context.Context, conf config.Section) (*WSConfig, error) {
	wsConfig := &WSConfig{
		HTTPURL:                conf.GetString(ffresty.HTTPConfigURL),
		WebSocketURL:           conf.GetString(WSConfigURL),
		WSKeyPath:              conf.GetString(WSConfigKeyPath),
		ReadBufferSize:         int(conf.GetByteSize(WSConfigKeyReadBufferSize)),
		WriteBufferSize:        int(conf.GetByteSize(WSConfigKeyWriteBufferSize)),
		InitialDelay:           conf.GetDuration(ffresty.HTTPConfigRetryInitDelay),
		MaximumDelay:           conf.GetDuration(ffresty.HTTPConfigRetryMaxDelay),
		DelayFactor:            conf.GetFloat64(WSConfigDelayFactor),
		InitialConnectAttempts: conf.GetInt(WSConfigKeyInitialConnectAttempts),
		BackgroundConnect:      conf.GetBool(WSConfigKeyBackgroundConnect),
		HTTPHeaders:            conf.GetObject(ffresty.HTTPConfigHeaders),
		AuthUsername:           conf.GetString(ffresty.HTTPConfigAuthUsername),
		AuthPassword:           conf.GetString(ffresty.HTTPConfigAuthPassword),
		HeartbeatInterval:      conf.GetDuration(WSConfigKeyHeartbeatInterval),
		ConnectionTimeout:      conf.GetDuration(WSConfigKeyConnectionTimeout),

		ConnectionCycleInterval:    conf.GetDuration(WSConfigKeyConnectionCycleInterval),
		ConnectionCycleQuiesceTime: conf.GetDuration(WSConfigKeyConnectionCycleQuiesceTime),
	}
	tlsSection := conf.SubSection("tls")
	tlsClientConfig, err := fftls.ConstructTLSConfig(ctx, tlsSection, fftls.ClientType)
	if err != nil {
		return nil, err
	}

	wsConfig.TLSClientConfig = tlsClientConfig

	// Build the underlying TCP dialer with the custom DNS resolver and SSRF egress guard,
	// from the same "net" subsection that ffresty.InitConfig set up on this config tree.
	var netDialer *net.Dialer
	resolver := ffdns.NewResolver(conf.SubSection("dns"))
	netCfg, err := ffnet.GenerateConfig(conf.SubSection("net"))
	if err == nil {
		netDialer, err = ffnet.NewDialer(ctx, netCfg, resolver)
	}
	if err != nil {
		return nil, err
	}
	netDialer.Timeout = wsConfig.ConnectionTimeout
	wsConfig.NetDialer = netDialer

	return wsConfig, nil
}
