// Copyright 2026 Google LLC
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

// Package server serves the actor JWT issuer's OpenID discovery document and
// JWK set.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/agent-substrate/substrate/internal/localjwtauthority"
	"github.com/agent-substrate/substrate/internal/oidcdiscovery"
)

const wellKnownPath = "/.well-known/openid-configuration"

// KeySource supplies the verification keys to publish.
// *localjwtauthority.RefreshingPool is one.
type KeySource interface {
	VerificationKeys() ([]*localjwtauthority.VerificationKey, error)
}

// Server serves the discovery document and JWK set under the issuer's path,
// plus /healthz and /readyz. It builds both documents from its KeySource on
// every request. When that fails it serves the last documents it built, and
// until it has built any it reports not ready and answers document requests
// with 503.
type Server struct {
	issuer        string
	discoveryPath string
	jwksPath      string
	keys          KeySource

	mu         sync.Mutex
	discovery  []byte
	jwks       []byte
	lastErrMsg string
}

// New returns a Server that publishes keys for issuer.
func New(issuer string, keys KeySource) (*Server, error) {
	if err := oidcdiscovery.ValidateIssuer(issuer); err != nil {
		return nil, err
	}
	u, err := url.Parse(issuer)
	if err != nil {
		return nil, err
	}
	// Clients and proxies may normalize dot segments, so routes match the
	// cleaned path.
	base := strings.TrimSuffix(path.Clean("/"+u.Path), "/")
	return &Server{
		issuer:        issuer,
		discoveryPath: base + wellKnownPath,
		jwksPath:      base + oidcdiscovery.JWKSPath,
		keys:          keys,
	}, nil
}

// documents returns the discovery document and JWK set for the current keys,
// or the last ones built if the keys cannot be published. Both are nil until a
// build succeeds.
func (s *Server) documents(ctx context.Context) (discovery, jwks []byte) {
	discovery, jwks, err := s.build()

	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		// A failing source fails every request, so log each distinct
		// failure once.
		if msg := err.Error(); msg != s.lastErrMsg {
			slog.WarnContext(ctx, "Cannot publish the actor JWT keys; serving the last good key set", slog.Any("err", err))
			s.lastErrMsg = msg
		}
		return s.discovery, s.jwks
	}
	s.discovery, s.jwks, s.lastErrMsg = discovery, jwks, ""
	return discovery, jwks
}

func (s *Server) build() (discovery, jwks []byte, err error) {
	verificationKeys, err := s.keys.VerificationKeys()
	if err != nil {
		return nil, nil, fmt.Errorf("reading verification keys: %w", err)
	}
	algorithms := make([]string, 0, len(verificationKeys))
	for _, vk := range verificationKeys {
		algorithms = append(algorithms, vk.Algorithm)
	}
	if jwks, err = localjwtauthority.JWKS(verificationKeys); err != nil {
		return nil, nil, err
	}
	if discovery, err = oidcdiscovery.DiscoveryDocument(s.issuer, algorithms); err != nil {
		return nil, nil, err
	}
	return discovery, jwks, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch route := path.Clean(r.URL.Path); route {
	case "/healthz":
		w.WriteHeader(http.StatusOK)
	case "/readyz":
		if _, jwks := s.documents(r.Context()); jwks == nil {
			http.Error(w, "key set not loaded", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	case s.discoveryPath, s.jwksPath:
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		discovery, jwks := s.documents(r.Context())
		if route == s.discoveryPath {
			serveDocument(w, r, discovery)
		} else {
			serveDocument(w, r, jwks)
		}
	default:
		http.NotFound(w, r)
	}
}

func serveDocument(w http.ResponseWriter, r *http.Request, doc []byte) {
	if doc == nil {
		http.Error(w, "key set not loaded", http.StatusServiceUnavailable)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("Cache-Control", "public, max-age=60")
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Content-Length", strconv.Itoa(len(doc)))
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(doc)
}
