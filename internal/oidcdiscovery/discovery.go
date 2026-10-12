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

package oidcdiscovery

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
)

// JWKSPath is the path of the key set, relative to the issuer.
const JWKSPath = "/openid/v1/jwks"

// JWKSURI returns the URL of issuer's key set.
func JWKSURI(issuer string) string {
	return strings.TrimSuffix(issuer, "/") + JWKSPath
}

type discoveryDocument struct {
	Issuer                           string   `json:"issuer"`
	JWKSURI                          string   `json:"jwks_uri"`
	ResponseTypesSupported           []string `json:"response_types_supported"`
	SubjectTypesSupported            []string `json:"subject_types_supported"`
	IDTokenSigningAlgValuesSupported []string `json:"id_token_signing_alg_values_supported"`
	ClaimsSupported                  []string `json:"claims_supported"`
}

// DiscoveryDocument returns the JSON served at
// <issuer>/.well-known/openid-configuration. The issuer is copied verbatim and
// the document advertises each distinct algorithm in algorithms.
func DiscoveryDocument(issuer string, algorithms []string) ([]byte, error) {
	if err := ValidateIssuer(issuer); err != nil {
		return nil, err
	}
	if len(algorithms) == 0 {
		return nil, errors.New("at least one signing algorithm is required")
	}
	algs := slices.Clone(algorithms)
	slices.Sort(algs)
	algs = slices.Compact(algs)

	return json.Marshal(discoveryDocument{
		Issuer:                           issuer,
		JWKSURI:                          JWKSURI(issuer),
		ResponseTypesSupported:           []string{"id_token"},
		SubjectTypesSupported:            []string{"public"},
		IDTokenSigningAlgValuesSupported: algs,
		ClaimsSupported:                  []string{"iss", "sub", "aud", "exp", "nbf", "iat", "jti", "ate.dev"},
	})
}
