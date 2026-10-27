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
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"slices"

	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/internal/localjwtauthority"
)

// KeySource supplies the verification keys to publish.
// *localjwtauthority.RefreshingPool is one.
type KeySource interface {
	VerificationKeys() ([]*localjwtauthority.VerificationKey, error)
}

// Server serves the discovery document at /.well-known/openid-configuration
// and the JWK set at /openid/v1/jwks, plus /healthz. It builds both documents
// from its KeySource on every request and answers 500 when that fails.
type Server struct {
	issuer string
	keys   KeySource
}

// New returns a Server that publishes keys for issuer, which must be
// https://<host>. It fails if the keys cannot be published.
func New(issuer string, keys KeySource) (*Server, error) {
	if err := actoridjwt.ValidateIssuer(issuer); err != nil {
		return nil, err
	}
	s := &Server{issuer: issuer, keys: keys}
	if _, _, err := s.build(); err != nil {
		return nil, fmt.Errorf("cannot publish the keys: %w", err)
	}
	return s, nil
}

// Register adds the server's routes to mux.
func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		discovery, _, err := s.build()
		serveDocument(w, r, discovery, err)
	})
	mux.HandleFunc("GET /openid/v1/jwks", func(w http.ResponseWriter, r *http.Request) {
		_, jwks, err := s.build()
		serveDocument(w, r, jwks, err)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

type discoveryDocument struct {
	Issuer                           string   `json:"issuer"`
	JWKSURI                          string   `json:"jwks_uri"`
	ResponseTypesSupported           []string `json:"response_types_supported"`
	SubjectTypesSupported            []string `json:"subject_types_supported"`
	IDTokenSigningAlgValuesSupported []string `json:"id_token_signing_alg_values_supported"`
	ClaimsSupported                  []string `json:"claims_supported"`
}

func (s *Server) build() (discovery, jwks []byte, err error) {
	verificationKeys, err := s.keys.VerificationKeys()
	if err != nil {
		return nil, nil, fmt.Errorf("reading verification keys: %w", err)
	}
	if jwks, err = localjwtauthority.JWKS(verificationKeys); err != nil {
		return nil, nil, err
	}
	algorithms := make([]string, 0, len(verificationKeys))
	for _, vk := range verificationKeys {
		algorithms = append(algorithms, vk.Algorithm)
	}
	slices.Sort(algorithms)
	discovery, err = json.Marshal(discoveryDocument{
		Issuer:                           s.issuer,
		JWKSURI:                          s.issuer + "/openid/v1/jwks",
		ResponseTypesSupported:           []string{"id_token"},
		SubjectTypesSupported:            []string{"public"},
		IDTokenSigningAlgValuesSupported: slices.Compact(algorithms),
		ClaimsSupported:                  []string{"iss", "sub", "aud", "exp", "nbf", "iat", "jti", "actor.ate.dev", "worker-for-actor.ate.dev"},
	})
	if err != nil {
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
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(doc)
}
