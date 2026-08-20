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
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/localjwtauthority"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/utils/ptr"
)

// writeTestJWTPool generates a single-authority signing pool on disk, the
// shape `kubectl-ate admin make-jwt-pool` provisions for ateapi.
func writeTestJWTPool(t *testing.T) string {
	t.Helper()
	authority, err := localjwtauthority.GenerateECDSAP256Authority("test-authority")
	if err != nil {
		t.Fatalf("generating JWT authority: %v", err)
	}
	poolBytes, err := localjwtauthority.Marshal(&localjwtauthority.Pool{Authorities: []*localjwtauthority.Authority{authority}})
	if err != nil {
		t.Fatalf("marshaling JWT pool: %v", err)
	}
	path := filepath.Join(t.TempDir(), "pool.json")
	if err := os.WriteFile(path, poolBytes, 0o600); err != nil {
		t.Fatalf("writing JWT pool: %v", err)
	}
	return path
}

func tokenTemplate(volumeName, audience, path string, expirationSeconds *int64) *atev1alpha1.ActorTemplate {
	return &atev1alpha1.ActorTemplate{
		Spec: atev1alpha1.ActorTemplateSpec{
			Volumes: []atev1alpha1.Volume{{
				Name: volumeName,
				VolumeSource: atev1alpha1.VolumeSource{
					SystemInfo: &atev1alpha1.SystemInfoVolumeSource{
						DataSources: []atev1alpha1.SystemInfoDataSource{
							{ActorIdentityToken: &atev1alpha1.ActorIdentityTokenDataSource{
								Audience:          audience,
								ExpirationSeconds: expirationSeconds,
								Path:              path,
							}},
						},
					},
				},
			}},
		},
	}
}

// decodeJWTPayload returns the (unverified) claims of a compact JWS. The mint
// test asserts claim contents; signature correctness is actoridjwt's own
// test territory.
func decodeJWTPayload(t *testing.T, token string) map[string]any {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token is not a compact JWS (%d parts)", len(parts))
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decoding JWT payload: %v", err)
	}
	claims := map[string]any{}
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatalf("unmarshaling JWT claims: %v", err)
	}
	return claims
}

func TestMintActorIdentityTokens(t *testing.T) {
	poolFile := writeTestJWTPool(t)
	template := tokenTemplate("system-info", "verifier.example.com", "identity/token", ptr.To(int64(900)))
	actor := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{
		Atespace: "team-a", Name: "actor-1", Uid: "uid-1",
	}}

	spec, err := workloadSpecFromActorTemplate(template, nil)
	if err != nil {
		t.Fatalf("workloadSpecFromActorTemplate: %v", err)
	}

	t.Run("mints a token into the wire spec with the requested binding", func(t *testing.T) {
		before := time.Now()
		if err := mintActorIdentityTokens(poolFile, template, spec, actor); err != nil {
			t.Fatalf("mintActorIdentityTokens: %v", err)
		}
		entry := spec.GetVolumes()[0].GetSystemInfo().GetDataSources()[0].GetActorIdentityToken()
		if entry.GetPath() != "identity/token" {
			t.Errorf("path = %q, want %q", entry.GetPath(), "identity/token")
		}
		if len(entry.GetToken()) == 0 {
			t.Fatal("token is empty")
		}

		claims := decodeJWTPayload(t, string(entry.GetToken()))
		if got := claims["sub"]; got != "atespaces:team-a:actors:actor-1" {
			t.Errorf("sub = %v, want atespaces:team-a:actors:actor-1", got)
		}
		aud, _ := claims["aud"].([]any)
		if len(aud) != 1 || aud[0] != "verifier.example.com" {
			t.Errorf("aud = %v, want [verifier.example.com]", claims["aud"])
		}
		exp, _ := claims["exp"].(float64)
		iat, _ := claims["iat"].(float64)
		if got := exp - iat; got != 900 {
			t.Errorf("exp-iat = %v, want the requested 900s TTL", got)
		}
		if got := time.Unix(int64(iat), 0); got.Before(before.Add(-time.Minute)) || got.After(time.Now().Add(time.Minute)) {
			t.Errorf("iat = %v, want approximately now", got)
		}
		// The substrate claims ride under the "ate.dev" key (see
		// actoridjwt.WireClaims), the same shape MintJWT produces, so
		// verifiers need one code path for both.
		sub, _ := claims["ate.dev"].(map[string]any)
		if sub == nil {
			t.Fatalf("no ate.dev claims object found in %v", claims)
		}
		if sub["actorUID"] != "uid-1" {
			t.Errorf("ate.dev actorUID = %v, want uid-1", sub["actorUID"])
		}
		if sub["atespace"] != "team-a" || sub["actorName"] != "actor-1" {
			t.Errorf("ate.dev identity = %v/%v, want team-a/actor-1", sub["atespace"], sub["actorName"])
		}
	})

	t.Run("two mints for the same actor differ (fresh JTI per activation)", func(t *testing.T) {
		specA, _ := workloadSpecFromActorTemplate(template, nil)
		specB, _ := workloadSpecFromActorTemplate(template, nil)
		if err := mintActorIdentityTokens(poolFile, template, specA, actor); err != nil {
			t.Fatalf("mint A: %v", err)
		}
		if err := mintActorIdentityTokens(poolFile, template, specB, actor); err != nil {
			t.Fatalf("mint B: %v", err)
		}
		a := specA.GetVolumes()[0].GetSystemInfo().GetDataSources()[0].GetActorIdentityToken().GetToken()
		b := specB.GetVolumes()[0].GetSystemInfo().GetDataSources()[0].GetActorIdentityToken().GetToken()
		if string(a) == string(b) {
			t.Error("two mints produced identical tokens; JTI should make every mint unique")
		}
	})

	t.Run("no token sources is a no-op even without a pool", func(t *testing.T) {
		plain := &atev1alpha1.ActorTemplate{}
		spec, _ := workloadSpecFromActorTemplate(plain, nil)
		if err := mintActorIdentityTokens("", plain, spec, actor); err != nil {
			t.Fatalf("mintActorIdentityTokens: %v", err)
		}
	})

	t.Run("token sources without a pool fail closed naming the flag", func(t *testing.T) {
		spec, _ := workloadSpecFromActorTemplate(template, nil)
		err := mintActorIdentityTokens("", template, spec, actor)
		if err == nil || !strings.Contains(err.Error(), "actor-id-jwt-pool") {
			t.Errorf("error = %v, want no-signing-pool error naming the flag", err)
		}
	})

	t.Run("unreadable pool fails closed", func(t *testing.T) {
		spec, _ := workloadSpecFromActorTemplate(template, nil)
		err := mintActorIdentityTokens(filepath.Join(t.TempDir(), "missing.json"), template, spec, actor)
		if err == nil || !strings.Contains(err.Error(), "signing pool") {
			t.Errorf("error = %v, want unreadable-pool error", err)
		}
	})
}
