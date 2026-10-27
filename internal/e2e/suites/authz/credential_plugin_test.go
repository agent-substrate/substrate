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

package authz

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientauthv1 "k8s.io/client-go/pkg/apis/clientauthentication/v1"

	"go.ate.dev/substrate/internal/ateclient"
)

// testCredentialPlugin checks that a client with a credential plugin
// authenticates as the ServiceAccount whose token the plugin prints. The
// stranger would be allowed if the client fell back to the shared identity.
func testCredentialPlugin(t *testing.T, ctx context.Context, f *fixture) {
	viewer := newPrincipal(t, ctx, f.clients, f.atespace, "plugin-viewer")
	stranger := newPrincipal(t, ctx, f.clients, f.atespace, "plugin-stranger")
	for _, p := range []*principal{viewer, stranger} {
		p.api = pluginClient(t, ctx, mintToken(t, ctx, f.clients, f.atespace, p.name))
	}
	setAtespacePolicy(t, ctx, f.clients, f.atespace, binding("viewer", viewer))
	cleanupAtespacePolicy(t, f.clients, f.atespace)

	expectAll(t, ctx, []call{
		{"GetActor", viewer, getActor(f.actor), allowed},
		{"GetActor", stranger, getActor(f.actor), denied},
	})
}

// pluginClient returns an ateapi client whose credential plugin prints an
// ExecCredential carrying token.
func pluginClient(t *testing.T, ctx context.Context, token string) *ateclient.Client {
	t.Helper()
	cred, err := json.Marshal(clientauthv1.ExecCredential{
		TypeMeta: metav1.TypeMeta{APIVersion: "client.authentication.k8s.io/v1", Kind: "ExecCredential"},
		Status:   &clientauthv1.ExecCredentialStatus{Token: token},
	})
	if err != nil {
		t.Fatal(err)
	}
	credFile := filepath.Join(t.TempDir(), "credential.json")
	if err := os.WriteFile(credFile, cred, 0o600); err != nil {
		t.Fatalf("writing credential: %v", err)
	}
	return dial(t, ctx, ateclient.DialOptions{CredentialPlugin: &ateclient.CredentialPlugin{
		Command: "cat",
		Args:    []string{credFile},
	}})
}
