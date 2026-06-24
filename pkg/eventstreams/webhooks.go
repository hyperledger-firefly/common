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
	"database/sql/driver"
	"net"
	"net/http"
	"net/url"

	"github.com/go-resty/resty/v2"
	"github.com/hyperledger-firefly/common/pkg/ffresty"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
)

type WebhookConfig struct {
	URL           *string             `ffstruct:"whconfig" json:"url,omitempty"`
	Method        *string             `ffstruct:"whconfig" json:"method,omitempty"`
	TLSConfigName *string             `ffstruct:"whconfig" json:"tlsConfigName,omitempty"`
	HTTP          *ffresty.HTTPConfig `ffstruct:"whconfig" json:"http,omitempty"`
	validated     bool
	tlsConfig     *tls.Config
}

// Store in DB as JSON
func (wc *WebhookConfig) Scan(src interface{}) error {
	return fftypes.JSONScan(src, wc)
}

// Store in DB as JSON
func (wc *WebhookConfig) Value() (driver.Value, error) {
	return fftypes.JSONValue(wc)
}

type webhookDispatcherFactory[CT EventStreamSpec, DT any] struct{}

// validate initializes the config ready for use
func (wdf *webhookDispatcherFactory[CT, DT]) Validate(ctx context.Context, _ *Config[CT, DT], spec CT, tlsConfigs map[string]*tls.Config, _ LifecyclePhase) error {
	whc := spec.WebhookConf()
	if whc.URL == nil || *whc.URL == "" {
		return i18n.NewError(ctx, i18n.MsgMissingWebhookURL)
	}
	if whc.TLSConfigName != nil && *whc.TLSConfigName != "" {
		tlsConfig, ok := tlsConfigs[*whc.TLSConfigName]
		if !ok {
			return i18n.NewError(ctx, i18n.MsgUnknownTLSConfiguration, *whc.TLSConfigName)
		}
		whc.tlsConfig = tlsConfig
	}
	whc.validated = true
	return nil
}

type webhookAction[CT any, DT any] struct {
	disablePrivateIPs bool
	spec              *WebhookConfig
	client            *resty.Client
}

func (wdf *webhookDispatcherFactory[CT, DT]) NewDispatcher(ctx context.Context, conf *Config[CT, DT], spec CT) Dispatcher[DT] {
	whc := spec.WebhookConf()
	httpConf := whc.HTTP
	if httpConf == nil {
		httpConf = &conf.Defaults.WebhookDefaults.HTTPConfig
	}
	httpConf.TLSClientConfig = whc.tlsConfig
	client := ffresty.NewWithConfig(ctx, ffresty.Config{
		URL:        *whc.URL,
		HTTPConfig: *httpConf,
	})
	return &webhookAction[CT, DT]{
		spec:              whc,
		disablePrivateIPs: conf.DisablePrivateIPs,
		client:            client,
	}
}

func (w *webhookAction[CT, DT]) AttemptDispatch(ctx context.Context, attempt int, batch *EventBatch[DT]) error {
	// We perform DNS resolution before each attempt, to exclude private IP address ranges from the target
	u, _ := url.Parse(*w.spec.URL)
	addr, err := net.ResolveIPAddr("ip4", u.Hostname())
	if err != nil {
		return i18n.NewError(ctx, i18n.MsgInvalidHost, u.Hostname())
	}
	if w.isAddressBlocked(addr) {
		return i18n.NewError(ctx, i18n.MsgBlockWebhookAddress, addr, u.Hostname())
	}
	method := http.MethodPost
	if w.spec.Method != nil && len(*w.spec.Method) > 0 {
		method = *w.spec.Method
	}
	var resBody []byte
	req := w.client.R().
		SetContext(ctx).
		SetBody(batch).
		SetResult(&resBody).
		SetError(&resBody)
	req.Header.Set("Content-Type", "application/json")

	res, err := req.Execute(method, u.String())
	if err != nil {
		log.L(ctx).Errorf("Webhook %s (%s) batch=%d attempt=%d: %s", *w.spec.URL, u, batch.BatchNumber, attempt, err)
		return i18n.NewError(ctx, i18n.MsgWebhookErr, err)
	}
	if res.IsError() {
		log.L(ctx).Errorf("Webhook %s (%s) [%d] batch=%d attempt=%d: %s", *w.spec.URL, u, res.StatusCode(), batch.BatchNumber, attempt, resBody)
		err = i18n.NewError(ctx, i18n.MsgWebhookFailedStatus, res.StatusCode())
	}
	return err
}

// isAddressBlocked allows blocking of all of the "private" address blocks defined by IPv4
func (w *webhookAction[CT, DT]) isAddressBlocked(ip *net.IPAddr) bool {
	ip4 := ip.IP.To4()
	return w.disablePrivateIPs &&
		(ip4[0] == 0 ||
			ip4[0] >= 224 ||
			ip4[0] == 127 ||
			ip4[0] == 10 ||
			(ip4[0] == 172 && ip4[1] >= 16 && ip4[1] < 32) ||
			(ip4[0] == 192 && ip4[1] == 168))
}
