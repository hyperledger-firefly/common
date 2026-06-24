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

package httpserver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/fftls"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
)

type remoteAddrContextKey struct{}
type localAddrContextKey struct{}

func RemoteAddr(ctx context.Context) net.Addr {
	remoteAddr := ctx.Value(remoteAddrContextKey{})
	if remoteAddr == nil {
		return nil
	}
	return remoteAddr.(net.Addr)
}

func LocalAddr(ctx context.Context) net.Addr {
	localAddr := ctx.Value(localAddrContextKey{})
	if localAddr == nil {
		return nil
	}
	return localAddr.(net.Addr)
}

type HTTPServer interface {
	ServeHTTP(ctx context.Context)
	Addr() net.Addr
}

type GoHTTPServer interface {
	Close() error
	Serve(l net.Listener) error
	ServeTLS(l net.Listener, certFile, keyFile string) error
	Shutdown(ctx context.Context) error
}

type httpServer struct {
	name            string
	s               GoHTTPServer
	l               net.Listener
	conf            config.Section
	corsConf        config.Section
	options         ServerOptions
	onClose         chan error
	tlsEnabled      bool
	tlsCertFile     string
	tlsKeyFile      string
	shutdownTimeout time.Duration
}

// ServerOptions are config parameters that are not set from the config, but rather the context
// in which the HTTP server is being used
type ServerOptions struct {
	MaximumRequestTimeout time.Duration
}

func NewHTTPServer(ctx context.Context, name string, r *mux.Router, onClose chan error, conf config.Section, corsConf config.Section, opts ...*ServerOptions) (is HTTPServer, err error) {
	tlsSubSection := conf.SubSection("tls")

	hs := &httpServer{
		name:            name,
		onClose:         onClose,
		conf:            conf,
		corsConf:        corsConf,
		tlsEnabled:      tlsSubSection.GetBool(fftls.HTTPConfTLSEnabled),
		tlsCertFile:     tlsSubSection.GetString(fftls.HTTPConfTLSCertFile),
		tlsKeyFile:      tlsSubSection.GetString(fftls.HTTPConfTLSKeyFile),
		shutdownTimeout: conf.GetDuration(HTTPConfShutdownTimeout),
	}

	for _, o := range opts {
		hs.options = *o
	}
	hs.l, err = createListener(ctx, hs.name, hs.conf)
	if err == nil {
		hs.s, err = hs.createServer(ctx, r)
	}
	return hs, err
}

func (hs *httpServer) Addr() net.Addr {
	return hs.l.Addr()
}

func createListener(ctx context.Context, name string, conf config.Section) (net.Listener, error) {
	listenAddr := fmt.Sprintf("%s:%d", conf.GetString(HTTPConfAddress), conf.GetUint(HTTPConfPort))
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, i18n.WrapError(ctx, err, i18n.MsgAPIServerStartFailed, listenAddr)
	}
	log.L(ctx).Infof("%s listening on HTTP %s", name, listener.Addr())
	return listener, err
}

func (hs *httpServer) createServer(ctx context.Context, r *mux.Router) (srv *http.Server, err error) {
	tlsConfig, err := fftls.ConstructTLSConfig(ctx, hs.conf.SubSection("tls"), "server")
	if err != nil {
		return nil, err
	}

	authConfig := hs.conf.SubSection("auth")
	authPluginName := hs.conf.GetString(HTTPAuthType)
	handler, err := wrapAuthIfEnabled(ctx, authConfig, authPluginName, r)
	if err != nil {
		return nil, err
	}
	handler = WrapCorsIfEnabled(ctx, hs.corsConf, handler)

	// Where a maximum request timeout is set, it does not make sense for either the
	// read timeout (time to read full body), or the write timeout (time to write the
	// response after processing the request) to be less than that
	readTimeout := hs.conf.GetDuration(HTTPConfReadTimeout)
	if readTimeout < hs.options.MaximumRequestTimeout {
		readTimeout = hs.options.MaximumRequestTimeout + 1*time.Second
	}
	writeTimeout := hs.conf.GetDuration(HTTPConfWriteTimeout)
	if writeTimeout < hs.options.MaximumRequestTimeout {
		writeTimeout = hs.options.MaximumRequestTimeout + 1*time.Second
	}

	log.L(ctx).Tracef("HTTP Server Timeouts (%s): read=%s write=%s request=%s", hs.l.Addr(), readTimeout, writeTimeout, hs.options.MaximumRequestTimeout)
	srv = &http.Server{
		Handler:           handler,
		WriteTimeout:      writeTimeout,
		ReadTimeout:       readTimeout,
		ReadHeaderTimeout: hs.conf.GetDuration(HTTPConfReadTimeout), // safe for this to always be the read timeout - should be short
		TLSConfig:         tlsConfig,
		ConnContext: func(newCtx context.Context, c net.Conn) context.Context {
			remoteAddr := c.RemoteAddr()
			localAddr := c.LocalAddr()
			l := log.L(ctx).WithField("req", fftypes.ShortID())
			newCtx = context.WithValue(newCtx, remoteAddrContextKey{}, remoteAddr)
			newCtx = context.WithValue(newCtx, localAddrContextKey{}, localAddr)
			newCtx = log.WithLogger(newCtx, l)
			log.L(log.WithLogFieldsMap(newCtx, map[string]string{"remote": remoteAddr.String(), "local": localAddr.String()})).Trace("New HTTP connection")
			return newCtx
		},
	}
	return srv, nil
}

func (hs *httpServer) ServeHTTP(ctx context.Context) {
	serverEnded := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			log.L(ctx).Infof("API server context canceled - shutting down")
			shutdownContext, cancel := context.WithTimeout(context.Background(), hs.shutdownTimeout)
			defer cancel()
			if err := hs.s.Shutdown(shutdownContext); err != nil {
				hs.onClose <- err
				return
			}
		case <-serverEnded:
			return
		}
	}()

	var err error
	if hs.tlsEnabled {
		err = hs.s.ServeTLS(hs.l, hs.tlsCertFile, hs.tlsKeyFile)
	} else {
		err = hs.s.Serve(hs.l)
	}
	if err == http.ErrServerClosed {
		err = nil
	}
	close(serverEnded)
	log.L(ctx).Infof("API server complete")

	hs.onClose <- err
}
