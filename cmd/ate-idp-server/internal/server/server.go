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
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

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
// plus /healthz. It builds both documents from its KeySource on every request
// and answers 500 when that fails.
type Server struct {
	issuer        string
	discoveryPath string
	jwksPath      string
	keys          KeySource
}

// New returns a Server that publishes keys for issuer. It fails if the keys
// cannot be published.
func New(issuer string, keys KeySource) (*Server, error) {
	if err := oidcdiscovery.ValidateIssuer(issuer); err != nil {
		return nil, err
	}
	u, err := url.Parse(issuer)
	if err != nil {
		return nil, err
	}
	// The escaped path keeps characters such as { from reading as pattern
	// wildcards. Routes use the cleaned path because ServeMux redirects
	// requests with dot segments to it.
	base := strings.TrimSuffix(path.Clean("/"+u.EscapedPath()), "/")
	s := &Server{
		issuer:        issuer,
		discoveryPath: base + wellKnownPath,
		jwksPath:      base + oidcdiscovery.JWKSPath,
		keys:          keys,
	}
	if _, _, err := s.build(); err != nil {
		return nil, fmt.Errorf("cannot publish the keys: %w", err)
	}
	return s, nil
}

// Register adds the server's routes to mux.
func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET "+s.discoveryPath, func(w http.ResponseWriter, r *http.Request) {
		discovery, _, err := s.build()
		serveDocument(w, r, discovery, err)
	})
	mux.HandleFunc("GET "+s.jwksPath, func(w http.ResponseWriter, r *http.Request) {
		_, jwks, err := s.build()
		serveDocument(w, r, jwks, err)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
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

func serveDocument(w http.ResponseWriter, r *http.Request, doc []byte, err error) {
	if err != nil {
		slog.ErrorContext(r.Context(), "Cannot publish the actor JWT keys", slog.Any("err", err))
		http.Error(w, "cannot publish the key set", http.StatusInternalServerError)
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
