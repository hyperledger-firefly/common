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

package basic

import (
	"bufio"
	"context"
	"encoding/base64"
	"os"
	"strings"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"golang.org/x/crypto/bcrypt"
)

const (
	authHeaderName        = "Authorization"
	basicAuthHeaderPrefix = "Basic "
)

type Auth struct {
	users map[string][]byte
}

func Name() string {
	return "basic"
}

func (a *Auth) Name() string {
	return Name()
}

func (a *Auth) Init(ctx context.Context, name string, config config.Section) error {
	passwordFile := config.GetString(PasswordFile)
	users, err := readPasswordFile(passwordFile)
	if err != nil {
		return err
	}
	a.users = users
	log.L(ctx).Infof("basic auth plugin enabled (name=%s)", name)
	return nil
}

func (a *Auth) Authorize(ctx context.Context, req *fftypes.AuthReq) error {
	if strings.HasPrefix(req.Header.Get(authHeaderName), basicAuthHeaderPrefix) {
		authHeader := strings.TrimPrefix(req.Header.Get(authHeaderName), basicAuthHeaderPrefix)
		if authHeader != "" {
			decodedAuthHeader, err := base64.StdEncoding.DecodeString(authHeader)
			if err != nil {
				return i18n.NewError(ctx, i18n.MsgUnauthorized)
			}
			split := strings.Split(string(decodedAuthHeader), ":")
			username := split[0]
			password := split[1]
			if hash, ok := a.users[username]; ok {
				if err := bcrypt.CompareHashAndPassword(hash, []byte(password)); err != nil {
					log.L(ctx).Warnf("user authentication failed: %s", err.Error())
				} else {
					return nil
				}
			}
		}
	}
	return i18n.NewError(ctx, i18n.MsgUnauthorized)
}

func readPasswordFile(path string) (map[string][]byte, error) {
	users := map[string][]byte{}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	s := bufio.NewScanner(f)
	s.Split(bufio.ScanLines)
	for s.Scan() {
		l := s.Text()
		split := strings.Split(l, ":")
		users[split[0]] = []byte(split[1])
	}
	f.Close()
	return users, nil
}
