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

package actoridjwt

import (
	"encoding/json"
	"fmt"
	"time"
)

type Claims struct {
	// Claims from RFC7519
	Issuer     string
	Subject    string
	Audiences  []string
	Expiration time.Time
	NotBefore  time.Time
	IssuedAt   time.Time
	JTI        string

	// Claims from ADK's session model
	Substrate SubstrateClaims
}

type SubstrateClaims struct {
	Atespace  string
	ActorName string
	ActorUID  string
}

type WireClaims struct {
	// Claims from RFC7519
	Issuer     string          `json:"iss,omitempty"`
	Subject    string          `json:"sub,omitempty"`
	Audiences  json.RawMessage `json:"aud,omitempty"`
	Expiration float64         `json:"exp,omitempty"`
	NotBefore  float64         `json:"nbf,omitempty"`
	IssuedAt   float64         `json:"iat,omitempty"`
	JTI        string          `json:"jti,omitempty"`

	// Claims from ADK's session model.
	Substrate WireSubstrateClaims `json:"ate.dev,omitempty"`
}

type WireSubstrateClaims struct {
	Atespace  string `json:"atespace,omitempty"`
	ActorName string `json:"actorName,omitempty"`
	ActorUID  string `json:"actorUID,omitempty"`
}

func ClaimsToWire(claims *Claims) (*WireClaims, error) {
	rawAudiences, err := json.Marshal(claims.Audiences)
	if err != nil {
		return nil, fmt.Errorf("while marshaling audience: %w", err)
	}

	wire := &WireClaims{
		Issuer:     claims.Issuer,
		Subject:    claims.Subject,
		Audiences:  rawAudiences,
		Expiration: float64(claims.Expiration.Unix()),
		NotBefore:  float64(claims.NotBefore.Unix()),
		IssuedAt:   float64(claims.IssuedAt.Unix()),
		JTI:        claims.JTI,
		Substrate: WireSubstrateClaims{
			Atespace:  claims.Substrate.Atespace,
			ActorName: claims.Substrate.ActorName,
			ActorUID:  claims.Substrate.ActorUID,
		},
	}

	return wire, nil
}
