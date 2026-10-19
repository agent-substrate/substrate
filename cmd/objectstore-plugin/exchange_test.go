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

package main

import (
	"path/filepath"
	"strings"
	"testing"

	exchangepb "go.ate.dev/substrate/internal/proto/objectstoreexchangepb"
)

func TestNewExchangeService(t *testing.T) {
	t.Run("S3 answers unimplemented", func(t *testing.T) {
		t.Setenv("ATE_STORAGE_BACKEND", "s3")
		svc, release, err := newExchangeService(t.Context(), exchangeConfig{})
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		if _, ok := svc.(exchangepb.UnimplementedCredentialExchangeServer); !ok {
			t.Fatalf("S3 exchange is %T, want the unimplemented server", svc)
		}
	})
	for _, tc := range []struct {
		name string
		cfg  exchangeConfig
		want string
	}{
		{"GCS needs the issuer", exchangeConfig{jwksFile: "jwks.json"}, "--actor-jwt-issuer"},
		{"GCS needs the JWKS file", exchangeConfig{issuer: "https://idp.ate-system.svc"}, "--actor-jwt-jwks-file"},
		{"GCS needs a readable JWKS file", exchangeConfig{issuer: "https://idp.ate-system.svc", jwksFile: filepath.Join(t.TempDir(), "absent.json")}, "key set"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ATE_STORAGE_BACKEND", "gcs")
			_, _, err := newExchangeService(t.Context(), tc.cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to name %s", err, tc.want)
			}
		})
	}
}

func TestExchangeTransportCredentials_MissingBundle(t *testing.T) {
	if _, err := exchangeTransportCredentials(filepath.Join(t.TempDir(), "absent.pem")); err == nil {
		t.Fatal("a missing serving bundle was accepted")
	}
}
