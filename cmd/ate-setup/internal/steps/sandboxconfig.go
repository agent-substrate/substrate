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
	"context"
	"encoding/json"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
	"go.ate.dev/substrate/cmd/ate-setup/internal/render"
)

// DeploySandboxConfig applies the SandboxConfig admission policy, then this
// release's gVisor SandboxConfig, substrate-<version>-gvisor, rendered from
// sandboxconfig-gvisor.yaml.tmpl. With addDefaultAnnotation it also adds the
// class-default annotation to the config.
func (e *Env) DeploySandboxConfig(ctx context.Context, addDefaultAnnotation bool) error {
	log.Step("deploy_sandboxconfig")

	if err := e.EnsureCRDs(ctx); err != nil {
		return err
	}

	// Enforce per-class SandboxConfig asset requirements. This is applied
	// before any SandboxConfig so the config below is validated too.
	if err := e.Kube.ApplyPath(ctx, e.Cfg.Manifest("sandboxconfig-validation.yaml")); err != nil {
		return err
	}

	name, err := e.checkSandboxConfig(ctx)
	if err != nil {
		return err
	}

	manifest, err := render.Template(e.Cfg.Manifest("sandboxconfig-gvisor.yaml.tmpl"), map[string]string{"SANDBOXCONFIG_NAME": name}, nil)
	if err != nil {
		return err
	}
	if err := e.Kube.ApplyBytes(ctx, manifest); err != nil {
		return err
	}
	log.Infof("applied SandboxConfig %s", name)
	if addDefaultAnnotation {
		// A merge patch rather than part of the apply above. The apply then
		// never owns the annotation, so a later run without
		// --add-default-annotation leaves it in place.
		patch, err := json.Marshal(map[string]any{
			"metadata": map[string]any{"annotations": map[string]any{atev1.SandboxConfigClassDefaultAnnotation: "true"}},
		})
		if err != nil {
			return fmt.Errorf("while building the patch for SandboxConfig %s: %w", name, err)
		}
		if _, err := e.sandboxConfigs().Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{FieldManager: kube.FieldManager}); err != nil {
			return fmt.Errorf("while annotating SandboxConfig %s: %w", name, err)
		}
		log.Infof("annotated SandboxConfig %s %s=true", name, atev1.SandboxConfigClassDefaultAnnotation)
	}
	// Checked after the apply, against the config's real creation time and
	// annotations.
	configs, err := e.listSandboxConfigs(ctx)
	if err != nil {
		return err
	}
	warning, err := checkClassDefault(configs, name, atev1.SandboxClassGvisor)
	if warning != "" {
		log.Warnf("%s", warning)
	}
	return err
}

// checkSandboxConfig returns the name of this release's gVisor SandboxConfig.
// It fails when checkClassDefault does.
func (e *Env) checkSandboxConfig(ctx context.Context) (string, error) {
	class := atev1.SandboxClassGvisor
	name, err := sandboxConfigName(e.SubstrateVersion(), class)
	if err != nil {
		return "", err
	}
	configs, err := e.listSandboxConfigs(ctx)
	if err != nil {
		return "", err
	}
	// The warning is about the cluster before the apply.
	if _, err := checkClassDefault(configs, name, class); err != nil {
		return "", err
	}
	return name, nil
}

// listSandboxConfigs returns the SandboxConfigs in the cluster. A cluster
// without the SandboxConfig CRD has none.
func (e *Env) listSandboxConfigs(ctx context.Context) ([]atev1.SandboxConfig, error) {
	list, err := e.sandboxConfigs().List(ctx, metav1.ListOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("while listing SandboxConfigs: %w", err)
	}
	configs := make([]atev1.SandboxConfig, len(list.Items))
	for i := range list.Items {
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(list.Items[i].Object, &configs[i]); err != nil {
			return nil, fmt.Errorf("while decoding SandboxConfig %s: %w", list.Items[i].GetName(), err)
		}
	}
	return configs, nil
}

func (e *Env) sandboxConfigs() dynamic.ResourceInterface {
	return e.Kube.Dynamic.Resource(atev1.GroupVersion.WithResource("sandboxconfigs"))
}

// sandboxConfigName returns the name of the SandboxConfig of class that
// Substrate version installs: substrate-<version>-<class>, with the version
// lower-cased and every character a name cannot hold replaced by '-'.
func sandboxConfigName(version string, class atev1.SandboxClass) (string, error) {
	v := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '.', r == '-':
			return r
		case r >= 'A' && r <= 'Z':
			return r - 'A' + 'a'
		}
		return '-'
	}, version)
	name := fmt.Sprintf("substrate-%s-%s", v, class)
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		return "", fmt.Errorf("version %q gives the invalid SandboxConfig name %q: %s", version, name, strings.Join(errs, "; "))
	}
	return name, nil
}

// checkClassDefault checks the SandboxConfig named name, of class. configs
// are the SandboxConfigs in the cluster.
//
//   - It returns an error when the config is being deleted. Applying it would
//     only update the terminating object.
//   - It warns when the config has the class-default annotation but is not
//     the class default, because a newer default exists. That happens when an
//     older release's config is applied again.
//   - It warns when the class has no default, so pools of the class without a
//     configRef resolve none.
func checkClassDefault(configs []atev1.SandboxConfig, name string, class atev1.SandboxClass) (string, error) {
	var existing *atev1.SandboxConfig
	for i := range configs {
		if configs[i].Name == name {
			existing = &configs[i]
		}
	}
	newest := atev1.ClassDefaultSandboxConfig(configs, class)
	if existing != nil && !existing.DeletionTimestamp.IsZero() {
		return "", fmt.Errorf("SandboxConfig %s is being deleted; wait until it is gone, then run this again", name)
	}
	if newest == nil {
		return fmt.Sprintf("no SandboxConfig of sandbox class %q has the %s annotation, so WorkerPools of the class without a configRef resolve no config",
			class, atev1.SandboxConfigClassDefaultAnnotation), nil
	}
	if existing != nil && existing.Annotations[atev1.SandboxConfigClassDefaultAnnotation] == "true" && newest.Name != name {
		return fmt.Sprintf("SandboxConfig %s has the %s annotation but is not the %s class default, because %s is newer",
			name, atev1.SandboxConfigClassDefaultAnnotation, class, newest.Name), nil
	}
	return "", nil
}
