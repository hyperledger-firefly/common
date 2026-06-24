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

package retry

import (
	"github.com/hyperledger-firefly/common/pkg/config"
)

const (
	ConfigInitialDelay = "initialDelay"
	ConfigMaximumDelay = "maxDelay"
	ConfigFactor       = "factor"
)

func InitConfig(conf config.Section) {
	conf.AddKnownKey(ConfigInitialDelay, "250ms")
	conf.AddKnownKey(ConfigMaximumDelay, "30s")
	conf.AddKnownKey(ConfigFactor, 2)
}

func NewFromConfig(conf config.Section) *Retry {
	return &Retry{
		InitialDelay: conf.GetDuration(ConfigInitialDelay),
		MaximumDelay: conf.GetDuration(ConfigMaximumDelay),
		Factor:       conf.GetFloat64(ConfigFactor),
	}
}
