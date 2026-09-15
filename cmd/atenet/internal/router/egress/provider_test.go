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

package egress

import (
	"context"
	"testing"
)

// Missing TLS material is an error, never a silent plaintext downgrade; going
// without TLS takes the explicit Insecure opt-in.
func TestDialProviderRequiresTLSMaterial(t *testing.T) {
	if _, err := DialProvider(context.Background(), ProviderDialConfig{Address: "provider:50051"}); err == nil {
		t.Fatal("DialProvider() error = nil, want an error when the CA and client credential bundle are missing")
	}

	// grpc.NewClient is lazy, so the opt-in path needs no listening server.
	conn, err := DialProvider(context.Background(), ProviderDialConfig{Address: "provider:50051", Insecure: true})
	if err != nil {
		t.Fatalf("DialProvider(Insecure) error = %v, want the explicit opt-in to dial", err)
	}
	conn.Close()
}
