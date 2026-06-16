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

package controllers

import (
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

func TestBuildDeploymentApplyConfigPodTemplate(t *testing.T) {
	wp := &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool", Namespace: "default", UID: "uid"},
		Spec: atev1alpha1.WorkerPoolSpec{
			Replicas:   2,
			AteomImage: "ateom:v1",
			Template:   sampleWorkerPoolPodTemplate(),
		},
	}

	depAC := buildDeploymentApplyConfig(wp)
	data, err := json.Marshal(depAC)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var dep map[string]any
	if err := json.Unmarshal(data, &dep); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	spec := dep["spec"].(map[string]any)
	template := spec["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)

	nodeSelector := podSpec["nodeSelector"].(map[string]any)
	if nodeSelector["workload"] != "substrate" {
		t.Fatalf("unexpected nodeSelector: %v", nodeSelector)
	}
	if podSpec["priorityClassName"] != "substrate-workers" {
		t.Fatalf("unexpected priorityClassName: %v", podSpec["priorityClassName"])
	}

	containers := podSpec["containers"].([]any)
	container := containers[0].(map[string]any)
	if _, ok := container["resources"]; ok {
		t.Fatalf("unexpected resources field: %v", container["resources"])
	}
}

func TestBuildDeploymentApplyConfigClearsNodeSelector(t *testing.T) {
	wp := &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool", Namespace: "default", UID: "uid"},
		Spec: atev1alpha1.WorkerPoolSpec{
			Replicas:   1,
			AteomImage: "ateom:v1",
			Template:   &atev1alpha1.WorkerPoolPodTemplate{},
		},
	}

	depAC := buildDeploymentApplyConfig(wp)
	data, err := json.Marshal(depAC)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var dep map[string]any
	if err := json.Unmarshal(data, &dep); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	podSpec := dep["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	nodeSelector, ok := podSpec["nodeSelector"].(map[string]any)
	if ok && len(nodeSelector) != 0 {
		t.Fatalf("expected empty nodeSelector, got %v", nodeSelector)
	}
}
