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
	"testing"
)

func TestBearerTokenCreds(t *testing.T) {
	md, err := bearerTokenCreds("some-token").GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata: %v", err)
	}
	if got, want := md["authorization"], "Bearer some-token"; got != want {
		t.Errorf("authorization=%q want %q", got, want)
	}

	if _, err := bearerTokenCreds("").GetRequestMetadata(context.Background()); err == nil {
		t.Error("GetRequestMetadata with empty token: want error, got nil")
	}

	if !bearerTokenCreds("some-token").RequireTransportSecurity() {
		t.Error("RequireTransportSecurity() = false, want true")
	}
}
