//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package controlapi

import (
	"crypto/rand"
	"fmt"
	"os"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/actoridjwt"
	"github.com/agent-substrate/substrate/internal/localjwtauthority"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// actorIdentityTokenIssuer mirrors the issuer actoridentity.MintJWT stamps;
// the TODO there about making it a real, OIDC-discoverable DNS name applies
// here identically — the two must move together so verifiers see one issuer.
const actorIdentityTokenIssuer = "https://api.ate-system.svc"

// defaultActorIdentityTokenTTL matches the CRD default for
// expirationSeconds; the guard here covers templates admitted before the
// defaulting webhook stamped them.
const defaultActorIdentityTokenTTL = 3600 * time.Second

// mintActorIdentityTokens fills the Token bytes of every actorIdentityToken
// data source in workloadSpec, minting one JWT per source with the
// template's audience and TTL bound into the claims alongside the actor's
// identity (atespace, name, uid — the same claim shape actoridentity.MintJWT
// produces, so verifiers need one code path).
//
// Minting happens here — on the resume path, immediately before the spec is
// sent to atelet — rather than in atelet, because the mint IS part of the
// activation: ateapi is placing this actor at this moment, so the token is
// inherently bound to the activation without a separate authorization
// exchange. Tokens therefore refresh on every Run/Restore; live renewal for
// long-running actors is deliberately out of scope here and lands with the
// live-refresh mechanism (#932 PR 2 tracks the transport).
//
// Fails closed, naming the problem, when the deployment has no signing pool:
// an actor that declared a token must not start without one.
func mintActorIdentityTokens(jwtPoolFile string, template *atev1alpha1.ActorTemplate, workloadSpec *ateletpb.WorkloadSpec, actor *ateapipb.Actor) error {
	if template == nil {
		return nil
	}

	// Wire entries indexed by (volume name, path) for filling in place.
	type key struct{ volume, path string }
	wire := map[key]*ateletpb.ActorIdentityTokenDataSource{}
	for _, vol := range workloadSpec.GetVolumes() {
		for _, ds := range vol.GetSystemInfo().GetDataSources() {
			if tok := ds.GetActorIdentityToken(); tok != nil {
				wire[key{vol.GetName(), tok.GetPath()}] = tok
			}
		}
	}
	if len(wire) == 0 {
		return nil
	}

	if jwtPoolFile == "" {
		return fmt.Errorf("this deployment does not issue actor identity tokens (ateapi runs without --actor-id-jwt-pool), required by this actor's SystemInfo volumes")
	}
	poolBytes, err := os.ReadFile(jwtPoolFile)
	if err != nil {
		return fmt.Errorf("while reading the actor JWT signing pool: %w", err)
	}
	pool, err := localjwtauthority.Unmarshal(poolBytes)
	if err != nil {
		return fmt.Errorf("while unmarshaling the actor JWT signing pool: %w", err)
	}
	if len(pool.Authorities) == 0 {
		return fmt.Errorf("the actor JWT signing pool contains no authorities")
	}
	authority := pool.Authorities[0]

	meta := actor.GetMetadata()
	now := time.Now()
	for _, vol := range template.Spec.Volumes {
		if vol.VolumeSource.SystemInfo == nil {
			continue
		}
		for _, ds := range vol.VolumeSource.SystemInfo.DataSources {
			if ds.ActorIdentityToken == nil {
				continue
			}

			ttl := defaultActorIdentityTokenTTL
			if ds.ActorIdentityToken.ExpirationSeconds != nil {
				ttl = time.Duration(*ds.ActorIdentityToken.ExpirationSeconds) * time.Second
			}
			claims := &actoridjwt.Claims{
				Issuer:     actorIdentityTokenIssuer,
				Subject:    fmt.Sprintf("atespaces:%s:actors:%s", meta.GetAtespace(), meta.GetName()),
				Audiences:  []string{ds.ActorIdentityToken.Audience},
				Expiration: now.Add(ttl),
				NotBefore:  now.Add(-5 * time.Minute),
				IssuedAt:   now,
				JTI:        rand.Text(),
				Substrate: actoridjwt.SubstrateClaims{
					Atespace:  meta.GetAtespace(),
					ActorName: meta.GetName(),
					ActorUID:  meta.GetUid(),
				},
			}
			wireClaims, err := actoridjwt.ClaimsToWire(claims)
			if err != nil {
				return fmt.Errorf("while building actor identity token claims (volume %q): %w", vol.Name, err)
			}
			token, err := actoridjwt.Sign(wireClaims, authority.SigningKey, authority.Algorithm, authority.ID)
			if err != nil {
				return fmt.Errorf("while signing actor identity token (volume %q): %w", vol.Name, err)
			}

			entry, ok := wire[key{vol.Name, ds.ActorIdentityToken.Path}]
			if !ok {
				// The wire spec is built from this same template, so a missing
				// entry means the two views diverged — a bug, not user error.
				return fmt.Errorf("internal error: no wire entry for actor identity token at volume %q path %q", vol.Name, ds.ActorIdentityToken.Path)
			}
			entry.Token = []byte(token)
		}
	}
	return nil
}
