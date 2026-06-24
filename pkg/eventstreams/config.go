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

package eventstreams

import (
	"context"
	"crypto/tls"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/ffresty"
	"github.com/hyperledger-firefly/common/pkg/fftls"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/retry"
)

// DispatcherFactory is the interface to plug in a custom dispatcher, for example to provide
// local in-process processing of events (in addition to remote WebSocket/Webhook consumption).
// Generics:
// - CT is the Configuration Type - the custom extensions to the configuration schema
// - DT is the Data Type - the payload type that will be delivered to the application
type DispatcherFactory[CT EventStreamSpec, DT any] interface {
	Validate(ctx context.Context, conf *Config[CT, DT], spec CT, tlsConfigs map[string]*tls.Config, phase LifecyclePhase) error
	NewDispatcher(ctx context.Context, conf *Config[CT, DT], spec CT) Dispatcher[DT]
}

type Config[CT EventStreamSpec, DT any] struct {
	TLSConfigs        map[string]*fftls.Config `ffstruct:"EventStreamConfig" json:"tlsConfigs,omitempty"`
	Retry             *retry.Retry             `ffstruct:"EventStreamConfig" json:"retry,omitempty"`
	DisablePrivateIPs bool                     `ffstruct:"EventStreamConfig" json:"disabledPrivateIPs"`
	Checkpoints       CheckpointsTuningConfig  `ffstruct:"EventStreamConfig" json:"checkpoints"`
	Defaults          EventStreamDefaults      `ffstruct:"EventStreamConfig" json:"defaults,omitempty"`

	// Allow plugging in additional types (important that the embedding code adds the FFEnum doc entry for the EventStreamType)
	AdditionalDispatchers map[EventStreamType]DispatcherFactory[CT, DT] `json:"-"`
}

type CheckpointsTuningConfig struct {
	Asynchronous            bool  `ffstruct:"CheckpointsConfig" json:"asynchronous"`
	UnmatchedEventThreshold int64 `ffstruct:"CheckpointsConfig" json:"unmatchedEventThreshold"`
}

type EventStreamDefaults struct {
	ErrorHandling     ErrorHandlingType       `ffstruct:"EventStreamDefaults" json:"errorHandling"`
	BatchSize         int                     `ffstruct:"EventStreamDefaults" json:"batchSize"`
	BatchTimeout      fftypes.FFDuration      `ffstruct:"EventStreamDefaults" json:"batchTimeout"`
	RetryTimeout      fftypes.FFDuration      `ffstruct:"EventStreamDefaults" json:"retryTimeout"`
	BlockedRetryDelay fftypes.FFDuration      `ffstruct:"EventStreamDefaults" json:"blockedRetryDelay"`
	WebSocketDefaults ConfigWebsocketDefaults `ffstruct:"EventStreamDefaults" json:"webSockets,omitempty"`
	WebhookDefaults   ConfigWebhookDefaults   `ffstruct:"EventStreamDefaults" json:"webhooks,omitempty"`
}

type ConfigWebsocketDefaults struct {
	DefaultDistributionMode DistributionMode `ffstruct:"EventStreamConfig" json:"distributionMode"`
}

type ConfigWebhookDefaults struct {
	ffresty.HTTPConfig
}

const (
	ConfigTLSConfigName = "name"

	ConfigCheckpointsAsynchronous            = "asynchronous"
	ConfigCheckpointsUnmatchedEventThreshold = "unmatchedEventThreshold"

	ConfigDisablePrivateIPs = "disablePrivateIPs"

	ConfigWebhooksDefaultTLSConfig = "tlsConfigName"

	ConfigWebSocketsDistributionMode = "distributionMode"

	ConfigDefaultsErrorHandling     = "errorHandling"
	ConfigDefaultsBatchSize         = "batchSize"
	ConfigDefaultsBatchTimeout      = "batchTimeout"
	ConfigDefaultsRetryTimeout      = "retryTimeout"
	ConfigDefaultsBlockedRetryDelay = "blockedRetryDelay"
)

var RootConfig config.Section
var TLSConfigs config.ArraySection
var WebhookDefaultsConfig config.Section
var WebSocketsDefaultsConfig config.Section
var RetrySection config.Section
var CheckpointsConfig config.Section
var DefaultsConfig config.Section

// Due to how arrays work currently in the config system, this can only be initialized
// in one section for the whole process.
func InitConfig(conf config.Section) {

	RootConfig = conf
	TLSConfigs = RootConfig.SubArray("tlsConfigs")
	TLSConfigs.AddKnownKey(ConfigTLSConfigName)
	tlsSubSection := TLSConfigs.SubSection("tls")
	fftls.InitTLSConfig(tlsSubSection)
	tlsSubSection.SetDefault(fftls.HTTPConfTLSEnabled, true) // as it's a TLS config

	conf.AddKnownKey(ConfigDisablePrivateIPs)

	DefaultsConfig = conf.SubSection("defaults")

	CheckpointsConfig = conf.SubSection("checkpoints")
	CheckpointsConfig.AddKnownKey(ConfigCheckpointsAsynchronous, true)
	CheckpointsConfig.AddKnownKey(ConfigCheckpointsUnmatchedEventThreshold, 250)

	DefaultsConfig.AddKnownKey(ConfigDefaultsErrorHandling, "block")
	DefaultsConfig.AddKnownKey(ConfigDefaultsBatchSize, 50)
	DefaultsConfig.AddKnownKey(ConfigDefaultsBatchTimeout, "500ms")
	DefaultsConfig.AddKnownKey(ConfigDefaultsRetryTimeout, "5m")
	DefaultsConfig.AddKnownKey(ConfigDefaultsBlockedRetryDelay, "1m")

	WebhookDefaultsConfig = DefaultsConfig.SubSection("webhooks")
	ffresty.InitConfig(WebhookDefaultsConfig)

	WebSocketsDefaultsConfig = DefaultsConfig.SubSection("websockets")
	WebSocketsDefaultsConfig.AddKnownKey(ConfigWebSocketsDistributionMode, DistributionModeLoadBalance)

	RetrySection = conf.SubSection("retry")
	retry.InitConfig(RetrySection)
}

// Optional function to generate config directly from YAML configuration using the config package.
// You can also generate the configuration programmatically
func GenerateConfig[CT EventStreamSpec, DT any](ctx context.Context) *Config[CT, DT] {
	httpDefaults, _ := ffresty.GenerateConfig(ctx, WebhookDefaultsConfig)
	tlsConfigs := map[string]*fftls.Config{}
	for i := 0; i < TLSConfigs.ArraySize(); i++ {
		tlsConf := TLSConfigs.ArrayEntry(i)
		name := tlsConf.GetString(ConfigTLSConfigName)
		tlsConfigs[name] = fftls.GenerateConfig(tlsConf.SubSection("tls"))
	}
	return &Config[CT, DT]{
		TLSConfigs:        tlsConfigs,
		DisablePrivateIPs: RootConfig.GetBool(ConfigDisablePrivateIPs),
		Checkpoints: CheckpointsTuningConfig{
			Asynchronous:            CheckpointsConfig.GetBool(ConfigCheckpointsAsynchronous),
			UnmatchedEventThreshold: CheckpointsConfig.GetInt64(ConfigCheckpointsUnmatchedEventThreshold),
		},
		Defaults: EventStreamDefaults{
			ErrorHandling:     fftypes.FFEnum(DefaultsConfig.GetString(ConfigDefaultsErrorHandling)),
			BatchSize:         DefaultsConfig.GetInt(ConfigDefaultsBatchSize),
			BatchTimeout:      fftypes.FFDuration(DefaultsConfig.GetDuration(ConfigDefaultsBatchTimeout)),
			RetryTimeout:      fftypes.FFDuration(DefaultsConfig.GetDuration(ConfigDefaultsRetryTimeout)),
			BlockedRetryDelay: fftypes.FFDuration(DefaultsConfig.GetDuration(ConfigDefaultsBlockedRetryDelay)),
			WebSocketDefaults: ConfigWebsocketDefaults{
				DefaultDistributionMode: fftypes.FFEnum(WebSocketsDefaultsConfig.GetString(ConfigWebSocketsDistributionMode)),
			},
			WebhookDefaults: ConfigWebhookDefaults{
				HTTPConfig: httpDefaults.HTTPConfig,
			},
		},
		Retry: retry.NewFromConfig(RetrySection),
	}
}
