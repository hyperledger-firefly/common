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

package version

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"

	"github.com/ghodss/yaml"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/spf13/cobra"
)

var shortened, output = false, "yaml"

type Info struct {
	*cobra.Command `json:"-"`
	Version        string `json:"Version,omitempty" yaml:"Version,omitempty"`
	Commit         string `json:"Commit,omitempty" yaml:"Commit,omitempty"`
	Date           string `json:"Date,omitempty" yaml:"Date,omitempty"`
	License        string `json:"License,omitempty" yaml:"License,omitempty"`
}

func setBuildInfo(info *Info, buildInfo *debug.BuildInfo, ok bool) {
	if ok {
		info.Version = buildInfo.Main.Version
	}
}

func (info *Info) LogVersion(ctx context.Context) {
	log.L(ctx).Infof("Version: %s", info.Version)
	log.L(ctx).Infof("Build date: %s", info.Date)
}

func NewInfo(buildDate, buildCommit, buildVersionOverride, license string) *Info {
	info := &Info{
		Date:    buildDate,
		Commit:  buildCommit,
		Version: buildVersionOverride,
		License: license,
	}
	// Where you are using go install, we will get good version information usefully from Go
	// When we're in go-releaser in a Github action, we will have the version passed in explicitly
	if info.Version == "" {
		buildInfo, ok := debug.ReadBuildInfo()
		setBuildInfo(info, buildInfo, ok)
	}
	info.Command = &cobra.Command{
		Use:   "version",
		Short: "Prints the version info",
		Long:  "Prints the version info in plain, JSON or YAML formats",
		RunE: func(_ *cobra.Command, _ []string) error {
			if shortened {
				fmt.Println(info.Version)
			} else {

				var (
					bytes []byte
					err   error
				)

				switch output {
				case "json":
					bytes, err = json.MarshalIndent(info, "", "  ")
				case "yaml":
					bytes, err = yaml.Marshal(info)
				default:
					err = i18n.NewError(context.Background(), i18n.MsgInvalidOutputOption, output)
				}
				if err != nil {
					return err
				}

				fmt.Println(string(bytes))
			}

			return nil
		},
	}
	info.Command.Flags().BoolVarP(&shortened, "short", "s", false, "Prints only the version number")
	info.Command.Flags().StringVarP(&output, "output", "o", "json", "output format (\"yaml\"|\"json\")")
	return info
}
