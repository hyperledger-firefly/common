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

package ffapi

import (
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/httpserver"
)

var (
	ConfMonitoringServerEnabled      = "enabled"
	ConfMonitoringServerMetricsPath  = "metricsPath"
	ConfMonitoringServerLivenessPath = "livenessPath"
	ConfMonitoringServerLoggingPath  = "loggingPath"

	ConfAPIDefaultFilterLimit     = "defaultFilterLimit"
	ConfAPIMaxFilterLimit         = "maxFilterLimit"
	ConfAPIMaxFilterSkip          = "maxFilterSkip"
	ConfAPIRequestTimeout         = "requestTimeout"
	ConfAPIRequestMaxTimeout      = "requestMaxTimeout"
	ConfAPIAlwaysPaginate         = "alwaysPaginate"
	ConfAPIDynamicPublicURLHeader = "dynamicPublicURLHeader"
)

func InitAPIServerConfig(apiConfig, monitoringConfig, corsConfig config.Section) {
	httpserver.InitHTTPConfig(apiConfig, 5000)
	apiConfig.AddKnownKey(ConfAPIDefaultFilterLimit, 25)
	apiConfig.AddKnownKey(ConfAPIMaxFilterLimit, 100)
	apiConfig.AddKnownKey(ConfAPIMaxFilterSkip, 100000)
	apiConfig.AddKnownKey(ConfAPIRequestTimeout, "30s")
	apiConfig.AddKnownKey(ConfAPIRequestMaxTimeout, "10m")
	apiConfig.AddKnownKey(ConfAPIAlwaysPaginate, false)
	apiConfig.AddKnownKey(ConfAPIDynamicPublicURLHeader)

	httpserver.InitCORSConfig(corsConfig)

	httpserver.InitHTTPConfig(monitoringConfig, 6000)
	monitoringConfig.AddKnownKey(ConfMonitoringServerEnabled, true)
	monitoringConfig.AddKnownKey(ConfMonitoringServerMetricsPath, "/metrics")
	monitoringConfig.AddKnownKey(ConfMonitoringServerLivenessPath, "/livez")
	monitoringConfig.AddKnownKey(ConfMonitoringServerLoggingPath, "/logging")
}
