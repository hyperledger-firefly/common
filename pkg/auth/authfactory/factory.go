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

package authfactory

import (
	"context"

	"github.com/hyperledger-firefly/common/pkg/auth"
	"github.com/hyperledger-firefly/common/pkg/auth/basic"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/i18n"
)

const (
	// ConfigName the name of the specific instance of a particular auth plugin
	ConfigName = "name"
	// ConfigType the auth plugin to use for the HTTP server
	ConfigType = "type"
)

var pluginsByType = map[string]func() auth.Plugin{
	basic.Name(): func() auth.Plugin { return &basic.Auth{} },
}

// Allows other code to register additional implementations of Auth plugins
// that can also be initialized with this factory
func RegisterPlugins(plugins map[string]func() auth.Plugin, conf config.Section) {
	for k, plugin := range plugins {
		plugin().InitConfig(conf)
		pluginsByType[k] = plugin
	}
}

// Initialize several different config sections with known keys
// Used in a list of plugins
func InitConfigArray(config config.ArraySection) {
	config.AddKnownKey(ConfigName)
	config.AddKnownKey(ConfigType)
	for name, plugin := range pluginsByType {
		plugin().InitConfig(config.SubSection(name))
	}
}

// Initialize a single config section with known keys
// Used as part of an HTTP server config section
func InitConfig(config config.Section) {
	config.AddKnownKey(ConfigType)
	for name, plugin := range pluginsByType {
		plugin().InitConfig(config.SubSection(name))
	}
}

func GetPlugin(ctx context.Context, pluginType string) (auth.Plugin, error) {
	plugin, ok := pluginsByType[pluginType]
	if !ok {
		return nil, i18n.NewError(ctx, i18n.MsgUnknownAuthPlugin, pluginType)
	}
	return plugin(), nil
}
