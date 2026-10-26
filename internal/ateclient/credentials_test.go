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

package ateclient

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/credentials"
	"k8s.io/client-go/kubernetes/fake"
)

func authorization(t *testing.T, creds credentials.PerRPCCredentials) string {
	t.Helper()
	md, err := creds.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata: %v", err)
	}
	return md["authorization"]
}

func TestMintedTokenCreds(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mints := 0
	var mintErr error
	creds := newMintedTokenCreds(func(context.Context) (string, time.Time, error) {
		if mintErr != nil {
			return "", time.Time{}, mintErr
		}
		mints++
		return fmt.Sprintf("token-%d", mints), now.Add(time.Hour), nil
	})
	creds.now = func() time.Time { return now }

	if got := authorization(t, creds); got != "Bearer token-1" {
		t.Errorf("authorization = %q, want Bearer token-1", got)
	}
	now = now.Add(58 * time.Minute)
	if got := authorization(t, creds); got != "Bearer token-1" {
		t.Errorf("authorization before the expiry margin = %q, want Bearer token-1", got)
	}

	now = now.Add(time.Minute)
	mintErr = errors.New("apiserver unavailable")
	if got := authorization(t, creds); got != "Bearer token-1" {
		t.Errorf("authorization when minting fails before expiry = %q, want the unexpired Bearer token-1", got)
	}
	now = now.Add(time.Minute)
	if _, err := creds.GetRequestMetadata(context.Background()); !errors.Is(err, mintErr) {
		t.Errorf("GetRequestMetadata when minting fails at expiry: error = %v, want %v", err, mintErr)
	}

	mintErr = nil
	if got := authorization(t, creds); got != "Bearer token-2" {
		t.Errorf("authorization once minting succeeds = %q, want Bearer token-2", got)
	}
}

func TestClientCredentials(t *testing.T) {
	clientset := fake.NewClientset()

	creds, err := clientCredentials(DialOptions{TokenFile: "/token"}, clientset)
	if _, ok := creds.(fileBearerTokenCreds); err != nil || !ok {
		t.Errorf("with a token file: %T, %v; want the token file", creds, err)
	}
	creds, err = clientCredentials(DialOptions{}, clientset)
	if _, ok := creds.(*mintedTokenCreds); err != nil || !ok {
		t.Errorf("without a token file: %T, %v; want a minted token", creds, err)
	}
}
