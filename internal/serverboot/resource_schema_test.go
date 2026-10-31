// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package serverboot

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/resource"
)

// TestNewResourceKeepsSchemaURL pins that the serverboot resource keeps the
// schema URL the OTel SDK's own detectors emit, rather than dropping it on a
// Schema URL conflict.
func TestNewResourceKeepsSchemaURL(t *testing.T) {
	t.Parallel()

	res, err := newResource(context.Background(), "test-service")
	if err != nil {
		t.Fatalf("newResource: %v", err)
	}
	want := resource.Default().SchemaURL()
	if want == "" {
		t.Fatal("resource.Default() has no schema URL; the SDK detector schema is unknown")
	}
	if got := res.SchemaURL(); got != want {
		t.Errorf("newResource SchemaURL = %q, want %q", got, want)
	}
}
