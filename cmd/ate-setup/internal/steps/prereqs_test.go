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

package steps

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/testing/protocmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/pkg/proto/ateconfigpb"
)

// A deploy without --ateapi-log-level must not undo a level set by hand or by
// an earlier deploy, but an explicit level always wins.
func TestEnsureAPIConfig(t *testing.T) {
	const handEdited = `logging { level: "warn" }`
	existing := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: ConfigMapAPIConfig},
		Data:       map[string]string{"apiconfig.textproto": handEdited},
	}

	for _, tc := range []struct {
		name      string
		existing  runtime.Object
		logLevel  string
		wantLevel string
	}{
		{name: "new install defaults to info", wantLevel: "info"},
		{name: "new install takes the requested level", logLevel: "debug", wantLevel: "debug"},
		{name: "existing config is kept", existing: existing, wantLevel: "warn"},
		{name: "requested level overwrites existing config", existing: existing, logLevel: "error", wantLevel: "error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// NewClientset, unlike the NewSimpleClientset behind fakeKube,
			// supports the server-side apply CreateAPIConfig uses.
			objects := []runtime.Object{}
			if tc.existing != nil {
				objects = append(objects, tc.existing)
			}
			e := &Env{
				Cfg:  &config.Config{APILogLevel: tc.logLevel},
				Kube: &kube.Client{Typed: fake.NewClientset(objects...)},
			}

			if err := e.ensureAPIConfig(t.Context()); err != nil {
				t.Fatalf("ensureAPIConfig() error = %v", err)
			}

			cm, err := e.Kube.GetConfigMap(t.Context(), NamespaceAteSystem, ConfigMapAPIConfig)
			if err != nil {
				t.Fatalf("GetConfigMap() error = %v", err)
			}
			if cm == nil {
				t.Fatalf("ConfigMap %s was not created", ConfigMapAPIConfig)
			}
			got := &ateconfigpb.APIConfig{}
			if err := prototext.Unmarshal([]byte(cm.Data["apiconfig.textproto"]), got); err != nil {
				t.Fatalf("apiconfig.textproto does not parse: %v", err)
			}
			want := &ateconfigpb.APIConfig{Logging: &ateconfigpb.APILogging{Level: tc.wantLevel}}
			if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
				t.Errorf("apiconfig.textproto mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
