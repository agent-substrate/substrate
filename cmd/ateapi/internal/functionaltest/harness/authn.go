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

package harness

import (
	"context"
	"errors"

	"go.ate.dev/substrate/cmd/ateapi/internal/apiauthn"
)

const (
	// clientTrustDomain is the SPIFFE trust domain of the identities the
	// harness's Control clients authenticate as.
	clientTrustDomain = "clients.harness.local"
	// clientName is who the default Control client authenticates as. It is a
	// bootstrap owner, so it may call every RPC.
	clientName = "harness-client"
)

// ClientID is the identity the client ClientAs(name) returns authenticates as:
// the SPIFFE ID its certificate carries, as an in-cluster component's does.
func ClientID(name string) string {
	return "spiffe://" + clientTrustDomain + "/" + name
}

// Member is how access policies name the client ClientAs(name) returns.
func Member(name string) string {
	return "user:" + ClientID(name)
}

// noTokens is ate-api's token authentication: a provider for an issuer no
// client uses, rejecting every token. Clients authenticate with certificates,
// as in-cluster components do, and ate-api requires at least one provider. The
// bearer-token path is covered by apiauthn's and oidcjwt's own tests.
func noTokens() apiauthn.ServerConfig {
	return apiauthn.ServerConfig{JWTProviders: []apiauthn.JWTProvider{{
		Name:   "none",
		Issuer: "https://no-tokens." + clientTrustDomain,
		Verify: func(context.Context, string) (string, error) {
			return "", errors.New("harness clients authenticate with certificates")
		},
	}}}
}
