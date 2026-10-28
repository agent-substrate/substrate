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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
)

// fakeKube builds a kube.Client backed by the fake clientset, seeded with
// objects. A nil object is skipped, so callers can pass a helper's "absent"
// result straight through.
func fakeKube(t *testing.T, objects ...runtime.Object) *kube.Client {
	t.Helper()
	present := make([]runtime.Object, 0, len(objects))
	for _, o := range objects {
		if o != nil {
			present = append(present, o)
		}
	}
	return &kube.Client{Typed: fake.NewSimpleClientset(present...)}
}

// apiServerEnvVarsConfigMap seeds the ate-api-server-envvars ConfigMap. Nil
// data means the ConfigMap does not exist.
func apiServerEnvVarsConfigMap(data map[string]string) runtime.Object {
	if data == nil {
		return nil
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: ConfigMapAPIEnvVars},
		Data:       data,
	}
}
