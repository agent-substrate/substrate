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
	"fmt"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

// resolveSandboxConfig picks the SandboxConfig for one sandbox class entry.
// inUse is the config the pool's status already uses for this class, or "".
// On failure it returns nil plus the condition reason and message.
//
// Rules, in order:
//   - configRef set: use that config. It must exist and be of the same class.
//   - No configRef: use the newest config of the class annotated as the
//     class default. Same creation time: the name that sorts first wins.
//   - No default: keep inUse, so a running pool never loses its config.
//   - A config being deleted takes no new pools, only ones already on it.
func resolveSandboxConfig(entry atev1alpha1.WorkerPoolSandboxClass, inUse string, configs []atev1alpha1.SandboxConfig) (*atev1alpha1.SandboxConfig, string, string) {
	if entry.ConfigRef != nil {
		name := entry.ConfigRef.Name
		i := slices.IndexFunc(configs, func(sc atev1alpha1.SandboxConfig) bool { return sc.Name == name })
		if i < 0 {
			return nil, atev1alpha1.WorkerPoolReasonSandboxConfigNotFound,
				fmt.Sprintf("SandboxConfig %q not found", name)
		}
		sc := &configs[i]
		if sc.Spec.SandboxClass != entry.Name {
			return nil, atev1alpha1.WorkerPoolReasonSandboxClassMismatch,
				fmt.Sprintf("SandboxConfig %q is for sandbox class %q, not %q", name, sc.Spec.SandboxClass, entry.Name)
		}
		// Being deleted: only a pool already on it keeps it. Its finalizer
		// holds it until that pool moves or is deleted.
		if !sc.DeletionTimestamp.IsZero() && sc.Name != inUse {
			return nil, atev1alpha1.WorkerPoolReasonSandboxConfigDeleting,
				fmt.Sprintf("SandboxConfig %q is being deleted", name)
		}
		return sc, "", ""
	}

	var best *atev1alpha1.SandboxConfig
	for i := range configs {
		sc := &configs[i]
		// Skip other classes, non-defaults, and configs being deleted.
		if sc.Spec.SandboxClass != entry.Name ||
			sc.Annotations[atev1alpha1.SandboxConfigClassDefaultAnnotation] != "true" ||
			!sc.DeletionTimestamp.IsZero() {
			continue
		}
		if best == nil || newerSandboxConfig(sc, best) {
			best = sc
		}
	}
	// No default: it was deleted or lost its annotation. Keep what the pool
	// already runs until another config becomes the default.
	if best == nil && inUse != "" {
		i := slices.IndexFunc(configs, func(sc atev1alpha1.SandboxConfig) bool { return sc.Name == inUse })
		if i >= 0 && configs[i].Spec.SandboxClass == entry.Name {
			return &configs[i], "", ""
		}
	}
	if best == nil {
		return nil, atev1alpha1.WorkerPoolReasonDefaultConfigNotFound,
			fmt.Sprintf("no SandboxConfig of sandbox class %q is annotated %s=true", entry.Name, atev1alpha1.SandboxConfigClassDefaultAnnotation)
	}
	return best, "", ""
}

// newerSandboxConfig reports whether a takes precedence over b as a class
// default.
func newerSandboxConfig(a, b *atev1alpha1.SandboxConfig) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return b.CreationTimestamp.Before(&a.CreationTimestamp)
	}
	return a.Name < b.Name
}

// resolveSandboxClasses resolves every entry of wp.Spec.SandboxClasses. On
// success it returns the status entries and a True condition; otherwise nil
// and a False condition for the first entry that does not resolve.
func resolveSandboxClasses(wp *atev1alpha1.WorkerPool, configs []atev1alpha1.SandboxConfig) ([]atev1alpha1.WorkerPoolSandboxClassStatus, metav1.Condition) {
	statuses := make([]atev1alpha1.WorkerPoolSandboxClassStatus, 0, len(wp.Spec.SandboxClasses))
	for _, entry := range wp.Spec.SandboxClasses {
		sc, reason, message := resolveSandboxConfig(entry, sandboxConfigInUse(wp, entry.Name), configs)
		if sc == nil {
			return nil, metav1.Condition{
				Type:    atev1alpha1.WorkerPoolConditionSandboxConfigResolved,
				Status:  metav1.ConditionFalse,
				Reason:  reason,
				Message: message,
			}
		}
		statuses = append(statuses, atev1alpha1.WorkerPoolSandboxClassStatus{
			Name:      entry.Name,
			ConfigRef: atev1alpha1.SandboxConfigReference{Name: sc.Name},
		})
	}
	return statuses, metav1.Condition{
		Type:    atev1alpha1.WorkerPoolConditionSandboxConfigResolved,
		Status:  metav1.ConditionTrue,
		Reason:  atev1alpha1.WorkerPoolReasonResolved,
		Message: "every sandbox class resolved to a SandboxConfig",
	}
}

// sandboxConfigInUse returns the SandboxConfig wp's status records for class,
// or "".
func sandboxConfigInUse(wp *atev1alpha1.WorkerPool, class atev1alpha1.SandboxClass) string {
	for _, s := range wp.Status.SandboxClasses {
		if s.Name == class {
			return s.ConfigRef.Name
		}
	}
	return ""
}

// sandboxConfigsInUse returns the names of the SandboxConfigs wp's status
// records as in use.
func sandboxConfigsInUse(wp *atev1alpha1.WorkerPool) []string {
	names := make([]string, 0, len(wp.Status.SandboxClasses))
	for _, s := range wp.Status.SandboxClasses {
		names = append(names, s.ConfigRef.Name)
	}
	return names
}
