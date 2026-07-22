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

package httpserver

import (
	"context"
	"net/http"

	"github.com/hyperledger-firefly/common/pkg/auth"
	"github.com/hyperledger-firefly/common/pkg/auth/authfactory"
	"github.com/hyperledger-firefly/common/pkg/config"
)

func wrapAuthIfEnabled(ctx context.Context, conf config.Section, pluginName string, chain http.Handler) (http.Handler, error) {
	if pluginName != "" {
		authPlugin, err := authfactory.GetPlugin(ctx, pluginName)
		if err != nil {
			return nil, err
		}
		if err := authPlugin.Init(ctx, "", conf.SubSection(authPlugin.Name())); err != nil {
			return nil, err
		}
		handler := auth.NewHandler(authPlugin)
		return handler.Handler(chain), nil
	}
	return chain, nil
}
