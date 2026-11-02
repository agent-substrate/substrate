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

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
)

// resolveLatestSandboxConfig picks the SandboxConfig the spec of one sandbox
// class entry asks for. It does not fall back to the config the pool already
// uses. On failure it returns nil plus the condition reason and message.
//
//   - With configRef, that config is used. It must exist, belong to the same
//     class, and not be being deleted.
//   - Without configRef, the newest config of the class that is annotated as
//     the class default and is not being deleted is used. If two were created
//     at the same time, the name that sorts first wins.
func resolveLatestSandboxConfig(entry atev1.WorkerPoolSandboxClass, configs []atev1.SandboxConfig) (*atev1.SandboxConfig, string, string) {
	if entry.ConfigRef != nil {
		name := entry.ConfigRef.Name
		i := slices.IndexFunc(configs, func(sc atev1.SandboxConfig) bool { return sc.Name == name })
		if i < 0 {
			return nil, atev1.WorkerPoolReasonSandboxConfigNotFound,
				fmt.Sprintf("SandboxConfig %q not found", name)
		}
		sc := &configs[i]
		if sc.Spec.SandboxClass != entry.Name {
			return nil, atev1.WorkerPoolReasonSandboxClassMismatch,
				fmt.Sprintf("SandboxConfig %q is for sandbox class %q, not %q", name, sc.Spec.SandboxClass, entry.Name)
		}
		if !sc.DeletionTimestamp.IsZero() {
			return nil, atev1.WorkerPoolReasonSandboxConfigDeleting,
				fmt.Sprintf("SandboxConfig %q is being deleted", name)
		}
		return sc, "", ""
	}

	isDefault := func(sc atev1.SandboxConfig) bool {
		return sc.Spec.SandboxClass == entry.Name &&
			sc.Annotations[atev1.SandboxConfigClassDefaultAnnotation] == "true"
	}
	var best *atev1.SandboxConfig
	for i := range configs {
		sc := &configs[i]
		if isDefault(*sc) && sc.DeletionTimestamp.IsZero() && (best == nil || newerSandboxConfig(sc, best)) {
			best = sc
		}
	}
	if best != nil {
		return best, "", ""
	}
	isDeletingDefault := func(sc atev1.SandboxConfig) bool {
		return isDefault(sc) && !sc.DeletionTimestamp.IsZero()
	}
	if slices.ContainsFunc(configs, isDeletingDefault) {
		return nil, atev1.WorkerPoolReasonSandboxConfigDeleting,
			fmt.Sprintf("the default SandboxConfig for sandbox class %q is being deleted", entry.Name)
	}
	return nil, atev1.WorkerPoolReasonDefaultConfigNotFound,
		fmt.Sprintf("no SandboxConfig of sandbox class %q is annotated %s=true", entry.Name, atev1.SandboxConfigClassDefaultAnnotation)
}

// resolveSandboxConfig returns the SandboxConfig one sandbox class entry uses
// and its Resolved condition. inUse is the config the pool's status already
// uses for this class, or "". When the spec does not resolve, the condition is
// False and the pool keeps inUse. See keepSandboxConfig.
func resolveSandboxConfig(entry atev1.WorkerPoolSandboxClass, inUse string, configs []atev1.SandboxConfig) (*atev1.SandboxConfig, metav1.Condition) {
	sc, reason, message := resolveLatestSandboxConfig(entry, configs)
	if sc != nil {
		return sc, metav1.Condition{
			Type:    atev1.WorkerPoolSandboxClassConditionResolved,
			Status:  metav1.ConditionTrue,
			Reason:  atev1.WorkerPoolReasonResolved,
			Message: fmt.Sprintf("resolved to SandboxConfig %q", sc.Name),
		}
	}
	return keepSandboxConfig(entry.Name, inUse, configs, reason, message)
}

// keepSandboxConfig returns the config a class keeps when its spec does not
// resolve, and a False Resolved condition with reason and message.
//
// The class keeps inUse, so a running pool never loses its config. The result
// is nil only when inUse is "" or no longer names a config of the class. A
// config in use that is being deleted carries the protection finalizer while
// any pool's status names it. One without the finalizer is about to
// disappear, so it is not kept.
func keepSandboxConfig(class atev1.SandboxClass, inUse string, configs []atev1.SandboxConfig, reason, message string) (*atev1.SandboxConfig, metav1.Condition) {
	var kept *atev1.SandboxConfig
	if inUse != "" {
		i := slices.IndexFunc(configs, func(sc atev1.SandboxConfig) bool { return sc.Name == inUse })
		if i >= 0 && configs[i].Spec.SandboxClass == class &&
			(configs[i].DeletionTimestamp.IsZero() || controllerutil.ContainsFinalizer(&configs[i], atev1.SandboxConfigWorkerPoolProtectionFinalizer)) {
			kept = &configs[i]
			message = fmt.Sprintf("%s; keeping %q", message, kept.Name)
		}
	}
	return kept, metav1.Condition{
		Type:    atev1.WorkerPoolSandboxClassConditionResolved,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: message,
	}
}

// newerSandboxConfig reports whether a takes precedence over b as a class
// default.
func newerSandboxConfig(a, b *atev1.SandboxConfig) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return b.CreationTimestamp.Before(&a.CreationTimestamp)
	}
	return a.Name < b.Name
}

// resolveSandboxClasses resolves every entry of wp.Spec.SandboxClasses. It
// returns one status entry per class, and whether every class resolved. Each
// entry carries the config the class uses, nil if it has none, and its
// Resolved condition.
func resolveSandboxClasses(wp *atev1.WorkerPool, configs []atev1.SandboxConfig) ([]atev1.WorkerPoolSandboxClassStatus, bool) {
	statuses := make([]atev1.WorkerPoolSandboxClassStatus, 0, len(wp.Spec.SandboxClasses))
	resolved := true
	for _, entry := range wp.Spec.SandboxClasses {
		prev := sandboxClassStatus(wp, entry.Name)
		inUse := ""
		if prev != nil && prev.ConfigRef != nil {
			inUse = prev.ConfigRef.Name
		}
		sc, cond := resolveSandboxConfig(entry, inUse, configs)
		if cond.Status != metav1.ConditionTrue {
			resolved = false
		}
		statuses = append(statuses, sandboxClassStatusFor(wp, entry.Name, sc, cond))
	}
	return statuses, resolved
}

// sandboxClassStatusFor builds the status entry for class from the config it
// uses, nil if none, and its Resolved condition. The entry starts from the
// class's previous conditions in wp, so lastTransitionTime only moves when the
// condition's status does.
func sandboxClassStatusFor(wp *atev1.WorkerPool, class atev1.SandboxClass, sc *atev1.SandboxConfig, cond metav1.Condition) atev1.WorkerPoolSandboxClassStatus {
	cond.ObservedGeneration = wp.Generation
	status := atev1.WorkerPoolSandboxClassStatus{Name: class}
	if prev := sandboxClassStatus(wp, class); prev != nil {
		status.Conditions = slices.Clone(prev.Conditions)
	}
	meta.SetStatusCondition(&status.Conditions, cond)
	if sc != nil {
		status.ConfigRef = &atev1.SandboxConfigReference{Name: sc.Name}
	}
	return status
}

// switchesSandboxConfig reports whether class, as resolved, uses a config
// other than the one wp's status uses for it.
func switchesSandboxConfig(wp *atev1.WorkerPool, class atev1.WorkerPoolSandboxClassStatus) bool {
	return switchesSandboxConfigFrom(wp.Status.SandboxClasses, class)
}

// switchesSandboxConfigFrom reports whether class, as resolved, uses a config
// other than the one its entry in prev uses.
func switchesSandboxConfigFrom(prev []atev1.WorkerPoolSandboxClassStatus, class atev1.WorkerPoolSandboxClassStatus) bool {
	if class.ConfigRef == nil {
		return false
	}
	p := findSandboxClassStatus(prev, class.Name)
	return p == nil || p.ConfigRef == nil || p.ConfigRef.Name != class.ConfigRef.Name
}

// claimSandboxConfigs returns the status that claims every config a class in
// classes switches to, or nil if no class switches. The claim keeps each
// class's status in wp as it is and names the new config in pendingConfigRef.
// That protects the new config like configRef does while
// confirmSandboxConfigSwitch checks it.
func claimSandboxConfigs(wp *atev1.WorkerPool, classes []atev1.WorkerPoolSandboxClassStatus) []atev1.WorkerPoolSandboxClassStatus {
	claim := make([]atev1.WorkerPoolSandboxClassStatus, 0, len(classes))
	claimed := false
	for _, class := range classes {
		status := atev1.WorkerPoolSandboxClassStatus{Name: class.Name}
		if prev := sandboxClassStatus(wp, class.Name); prev != nil {
			status = *prev.DeepCopy()
			status.PendingConfigRef = nil
		}
		if switchesSandboxConfig(wp, class) {
			status.PendingConfigRef = class.ConfigRef.DeepCopy()
			claimed = true
		}
		claim = append(claim, status)
	}
	if !claimed {
		return nil
	}
	return claim
}

// confirmSandboxConfigSwitch returns the status entry for a class that
// switches to a new config. live is that config as read from the API server
// after wp's status claimed it, or nil if it does not exist. The switch stands
// if the config belongs to the class and is not being deleted. Otherwise the
// class keeps the config wp's status uses, with a False Resolved condition.
// configs is the cached list keepSandboxConfig checks that config against.
func confirmSandboxConfigSwitch(wp *atev1.WorkerPool, class atev1.WorkerPoolSandboxClassStatus, live *atev1.SandboxConfig, configs []atev1.SandboxConfig) atev1.WorkerPoolSandboxClassStatus {
	name := class.ConfigRef.Name
	var reason, message string
	switch {
	case live == nil:
		reason, message = atev1.WorkerPoolReasonSandboxConfigNotFound,
			fmt.Sprintf("SandboxConfig %q not found", name)
	case live.Spec.SandboxClass != class.Name:
		reason, message = atev1.WorkerPoolReasonSandboxClassMismatch,
			fmt.Sprintf("SandboxConfig %q is for sandbox class %q, not %q", name, live.Spec.SandboxClass, class.Name)
	case !live.DeletionTimestamp.IsZero():
		reason, message = atev1.WorkerPoolReasonSandboxConfigDeleting,
			fmt.Sprintf("SandboxConfig %q is being deleted", name)
	default:
		return class
	}
	inUse := ""
	if prev := sandboxClassStatus(wp, class.Name); prev != nil && prev.ConfigRef != nil {
		inUse = prev.ConfigRef.Name
	}
	sc, cond := keepSandboxConfig(class.Name, inUse, configs, reason, message)
	return sandboxClassStatusFor(wp, class.Name, sc, cond)
}

// sandboxClassStatus returns the status entry wp records for class, or nil.
func sandboxClassStatus(wp *atev1.WorkerPool, class atev1.SandboxClass) *atev1.WorkerPoolSandboxClassStatus {
	return findSandboxClassStatus(wp.Status.SandboxClasses, class)
}

// findSandboxClassStatus returns the entry of class in statuses, or nil.
func findSandboxClassStatus(statuses []atev1.WorkerPoolSandboxClassStatus, class atev1.SandboxClass) *atev1.WorkerPoolSandboxClassStatus {
	for i := range statuses {
		if statuses[i].Name == class {
			return &statuses[i]
		}
	}
	return nil
}

// sandboxConfigsInUse returns the names of the SandboxConfigs wp's status
// records as in use. That is every configRef and every pendingConfigRef.
func sandboxConfigsInUse(wp *atev1.WorkerPool) []string {
	names := make([]string, 0, len(wp.Status.SandboxClasses))
	for _, s := range wp.Status.SandboxClasses {
		if s.ConfigRef != nil {
			names = append(names, s.ConfigRef.Name)
		}
		if s.PendingConfigRef != nil {
			names = append(names, s.PendingConfigRef.Name)
		}
	}
	return names
}
