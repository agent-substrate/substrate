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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/oidcdiscovery"
)

const wellKnownPath = "/.well-known/openid-configuration"

// Server serves the discovery document and JWK set under the issuer's path,
// plus /healthz and /readyz. It reports not ready, and answers document
// requests with 503, until Load accepts a key set.
type Server struct {
	issuer        string
	discoveryPath string
	jwksPath      string

	mu        sync.RWMutex
	discovery []byte
	jwks      []byte
}

// New returns a Server for issuer.
func New(issuer string) (*Server, error) {
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
	}, nil
}

// Load replaces the served JWK set with jwks and rebuilds the discovery
// document from its keys' algorithms. On error the previous key set is kept.
func (s *Server) Load(jwks []byte) error {
	var set struct {
		Keys []struct {
			Algorithm string `json:"alg"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(jwks, &set); err != nil {
		return fmt.Errorf("parsing key set: %w", err)
	}
	if len(set.Keys) == 0 {
		return errors.New("key set has no keys")
	}
	algs := make([]string, 0, len(set.Keys))
	for i, key := range set.Keys {
		if key.Algorithm == "" {
			return fmt.Errorf("key %d has no alg", i)
		}
		algs = append(algs, key.Algorithm)
	}
	discovery, err := oidcdiscovery.DiscoveryDocument(s.issuer, algs)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.discovery = discovery
	s.jwks = bytes.Clone(jwks)
	return nil
}

// WatchFile loads the key set from file, then reloads it whenever its contents
// change, checking every interval until ctx is done. Errors are logged and the
// last good key set stays in place.
func (s *Server) WatchFile(ctx context.Context, file string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var last []byte
	var lastReadErr string
	for {
		data, err := os.ReadFile(file)
		switch {
		case err != nil:
			// The file is absent until the ConfigMap exists; log each
			// distinct failure once instead of every interval.
			if err.Error() != lastReadErr {
				slog.WarnContext(ctx, "Cannot read key set", slog.String("file", file), slog.Any("err", err))
				lastReadErr = err.Error()
			}
		case !bytes.Equal(data, last):
			if err := s.Load(data); err != nil {
				slog.WarnContext(ctx, "Ignoring invalid key set", slog.String("file", file), slog.Any("err", err))
			} else {
				slog.InfoContext(ctx, "Loaded key set", slog.String("file", file))
			}
			last, lastReadErr = data, ""
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	discovery, jwks := s.discovery, s.jwks
	s.mu.RUnlock()

	switch path.Clean(r.URL.Path) {
	case "/healthz":
		w.WriteHeader(http.StatusOK)
	case "/readyz":
		if jwks == nil {
			http.Error(w, "key set not loaded", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	case s.discoveryPath:
		serveDocument(w, r, discovery)
	case s.jwksPath:
		serveDocument(w, r, jwks)
	default:
		http.NotFound(w, r)
	}
}

func serveDocument(w http.ResponseWriter, r *http.Request, doc []byte) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
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
