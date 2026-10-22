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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientauthv1 "k8s.io/client-go/pkg/apis/clientauthentication/v1"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// testWhoAmI checks the principal ateapi reports for one ServiceAccount's
// token, sent from a token file and from a credential plugin.
func testWhoAmI(t *testing.T, ctx context.Context, f *fixture) {
	const name = "whoami"
	p := newPrincipal(t, ctx, f.clients, f.atespace, name)
	plugin := pluginClient(t, ctx, mintToken(t, ctx, f.clients, f.atespace, name))

	want := strings.TrimPrefix(p.member, "user:")
	for _, c := range []struct {
		how string
		api *ateclient.Client
	}{
		{"token file", p.api},
		{"credential plugin", plugin},
	} {
		resp, err := c.api.Authentication().WhoAmI(ctx, &ateapipb.WhoAmIRequest{})
		if err != nil {
			t.Errorf("%s: WhoAmI: %v", c.how, err)
			continue
		}
		if resp.GetId() != want || resp.GetKind() != ateapipb.PrincipalKind_PRINCIPAL_KIND_JWT || resp.GetIssuer() == "" {
			t.Errorf("%s: WhoAmI = %v, want id %q, kind JWT, and an issuer", c.how, resp, want)
		}
	}
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
	return dial(t, ctx, ateclient.DialOptions{CredentialPlugin: &clientcmdapi.ExecConfig{
		APIVersion:      "client.authentication.k8s.io/v1",
		Command:         "cat",
		Args:            []string{credFile},
		InteractiveMode: clientcmdapi.NeverExecInteractiveMode,
	}})
}
