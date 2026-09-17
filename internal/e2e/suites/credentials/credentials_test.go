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

package credentials

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestKubernetesCredentialInjection uses the Helm values in values.yaml, real
// actors, and a local HTTP origin with the installed gateway configuration and RBAC.
func TestKubernetesCredentialInjection(t *testing.T) {
	if os.Getenv("E2E_CREDENTIAL_PROVIDER") == "" {
		t.Skip("requires credential E2E namespace grants and E2E_CREDENTIAL_PROVIDER=1")
	}
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	require.NoError(t, err)
	ctx := t.Context()
	clients := e2e.GetClients()
	namespace, template := e2e.DeployProbe(t, env["BUCKET_NAME"], "credentials")
	deniedAtespace, deniedTemplate := e2e.DeployProbe(t, env["BUCKET_NAME"], "credentials-denied")
	otherNamespace := e2e.CreateNamespace(t).Name

	for _, secret := range []struct{ namespace, name string }{
		{namespace, "allowed"}, {otherNamespace, "allowed"},
	} {
		_, err := clients.K8s.CoreV1().Secrets(secret.namespace).Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: secret.name},
			Data:       map[string][]byte{"token": []byte("e2e-credential-token")},
		}, metav1.CreateOptions{})
		require.NoError(t, err)
	}

	e2e.DeployServerPod(t, ctx, e2e.ServerPod{
		Name: "credential-origin", Namespace: namespace,
		ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"http", "--authorization-file=/run/token/token"},
		Port:       80, TargetPort: 8080,
		Volumes: []corev1.Volume{
			{Name: "token", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "allowed"}}},
		},
		VolumeMounts: []corev1.VolumeMount{{Name: "token", MountPath: "/run/token", ReadOnly: true}},
	})
	host := "credential-origin." + namespace + ".svc"
	router, err := e2e.NewRouterClient(ctx)
	require.NoError(t, err)
	t.Cleanup(router.Close)
	// Keep the successful actor alive through the cache-isolation check.
	suite := t
	for _, tc := range []struct {
		name, secretNamespace, secret, want string
	}{
		{"without-injection", "", "", "401"},
		{"allowed", namespace, "allowed", "204"},
		{"atespace-denied", namespace, "allowed", "403"},
		{"namespace-denied", otherNamespace, "allowed", "403"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			atespace, actorTemplate := namespace, template
			actorName := tc.name
			if tc.name == "atespace-denied" {
				// Use the already-fetched URI from an ungranted atespace so an
				// incorrectly shared gateway cache cannot bypass authorization.
				atespace, actorTemplate = deniedAtespace, deniedTemplate
				actorName = "allowed"
			}
			actor := &ateapipb.ObjectRef{Atespace: atespace, Name: actorName}
			_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actor})
			_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: actor})
			_, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: actorName},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: atespace, Name: actorTemplate.GetMetadata().GetName()},
			}})
			require.NoError(t, err)
			cleanupTest := t
			if tc.name == "allowed" {
				cleanupTest = suite
			}
			cleanupTest.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapipb.SuspendActorRequest{Actor: actor})
				_, err := clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: actor})
				if err != nil {
					cleanupTest.Errorf("delete actor %s/%s: %v", atespace, actorName, err)
				}
			})
			rule := e2e.EgressAllowHostnames(host)
			if tc.secret != "" {
				rule.Hostnames.Effects = &ateapipb.EgressRuleEffects{InjectStaticHeaders: []*ateapipb.CredentialHeaderInjection{{
					Header: "authorization", Prefix: "Bearer ",
					CredentialUri: fmt.Sprintf("ate-secret://kubernetes.io/%s/%s/token", tc.secretNamespace, tc.secret),
				}}}
			}
			e2e.EnsureEgressPolicy(t, ctx, clients, actor, rule)
			_, err = clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actor})
			require.NoError(t, err)
			path := "/fetch?roots=system&url=" + url.QueryEscape("http://"+host+"/credential")
			// Route discovery is asynchronous. Retries require the precise status;
			// transport errors never pass.
			deadline := time.Now().Add(90 * time.Second)
			for {
				resp, err := router.Get(ctx, resources.ActorRef{Atespace: atespace, Name: actorName}, path)
				var body []byte
				if err == nil {
					body, err = io.ReadAll(resp.Body)
					resp.Body.Close()
					var result struct{ Status, Error string }
					if err == nil && resp.StatusCode == http.StatusOK && json.Unmarshal(body, &result) == nil && result.Error == "" && result.Status == tc.want {
						break
					}
				}
				if time.Now().After(deadline) {
					t.Fatalf("want origin status %s; last response: %s; error: %v", tc.want, body, err)
				}
				time.Sleep(2 * time.Second)
			}
		})
	}
}
