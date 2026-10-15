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

// Kind is who a token speaks for. The two kinds carry the same fields but
// under different subjects and claim names, so a relying party can tell an
// actor's own code from a Substrate component acting on the actor's behalf
// and grant them different access.
type Kind int

const (
	// KindUnspecified is the zero value. ClaimsToWire rejects it, so a Claims
	// that forgets to set Kind cannot mint a token of either kind.
	KindUnspecified Kind = iota
	// KindActor is the actor itself. Subject "actor/<atespace>/<name>",
	// claims under "actor.ate.dev".
	KindActor
	// KindAteomForActor is an ateom, or another Substrate component, acting
	// on behalf of the actor. Subject "ateom-for-actor/<atespace>/<name>",
	// claims under "ateom-for-actor.ate.dev".
	KindAteomForActor
)

// SubjectPrefix is the first segment of the sub claim for this kind.
func (k Kind) SubjectPrefix() string {
	switch k {
	case KindActor:
		return "actor"
	case KindAteomForActor:
		return "ateom-for-actor"
	}
	return ""
}

// ClaimName is the JWT claim the Substrate fields are stored under for this
// kind.
func (k Kind) ClaimName() string {
	switch k {
	case KindActor:
		return "actor.ate.dev"
	case KindAteomForActor:
		return "ateom-for-actor.ate.dev"
	}
	return ""
}

type Claims struct {
	// Kind selects the subject prefix and the claim name the Substrate
	// fields are stored under.
	Kind Kind

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

	// Substrate claims, under the claim name for the token's kind. Exactly
	// one is set.
	Actor         *WireSubstrateClaims `json:"actor.ate.dev,omitempty"`
	AteomForActor *WireSubstrateClaims `json:"ateom-for-actor.ate.dev,omitempty"`
}

// Substrate returns the token's kind and its Substrate claims, or ok false if
// the token carries neither claim or both.
func (w *WireClaims) Substrate() (kind Kind, claims *WireSubstrateClaims, ok bool) {
	switch {
	case w.Actor != nil && w.AteomForActor == nil:
		return KindActor, w.Actor, true
	case w.AteomForActor != nil && w.Actor == nil:
		return KindAteomForActor, w.AteomForActor, true
	}
	return 0, nil, false
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
	}
	substrate := &WireSubstrateClaims{
		Atespace:  claims.Substrate.Atespace,
		ActorName: claims.Substrate.ActorName,
		ActorUID:  claims.Substrate.ActorUID,
	}
	switch claims.Kind {
	case KindActor:
		wire.Actor = substrate
	case KindAteomForActor:
		wire.AteomForActor = substrate
	default:
		return nil, fmt.Errorf("token kind %d is not one of KindActor or KindAteomForActor", claims.Kind)
	}

	return wire, nil
}
