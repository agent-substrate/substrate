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

package controlapi

import (
	"fmt"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

// resolveTemplateSandboxConfig resolves the SandboxConfig the ActorTemplate
// names via sandbox_config.config_name and checks that its class matches the
// template's sandbox_class.
func resolveTemplateSandboxConfig(
	sandboxConfigLister listersv1alpha1.SandboxConfigLister,
	templateSandbox *ateapipb.SandboxConfig,
) (*atev1alpha1.SandboxConfig, error) {
	name := templateSandbox.GetConfigName()
	sc, err := sandboxConfigLister.Get(name)
	if k8serrors.IsNotFound(err) {
		return nil, status.Errorf(codes.FailedPrecondition, "SandboxConfig %q not found", name)
	}
	if err != nil {
		return nil, fmt.Errorf("while getting SandboxConfig %q: %w", name, err)
	}
	if class := sandboxClassString(templateSandbox.GetSandboxClass()); string(sc.Spec.SandboxClass) != class {
		return nil, status.Errorf(codes.FailedPrecondition,
			"SandboxConfig %q has class %q but sandbox_config.sandbox_class is %q", name, sc.Spec.SandboxClass, class)
	}
	return sc, nil
}

// resolveSandboxAssets determines the sandbox binaries and pause image an actor
// should boot with and projects them onto the ateletpb.SandboxAssets atelet
// fetches: the SandboxConfig the ActorTemplate names via
// sandbox_config.config_name (required; enforced by CreateActorTemplate),
// checked against the template's sandbox_class.
func resolveSandboxAssets(
	sandboxConfigLister listersv1alpha1.SandboxConfigLister,
	templateSandbox *ateapipb.SandboxConfig,
) (*ateletpb.SandboxAssets, error) {
	if sandboxClassString(templateSandbox.GetSandboxClass()) == "" {
		return nil, fmt.Errorf("ActorTemplate names unrecognized sandbox_class %v", templateSandbox.GetSandboxClass())
	}
	if templateSandbox.GetConfigName() == "" {
		return nil, fmt.Errorf("ActorTemplate names no sandbox_config.config_name")
	}

	sc, err := resolveTemplateSandboxConfig(sandboxConfigLister, templateSandbox)
	if err != nil {
		return nil, err
	}
	return sandboxAssetsProto(sc), nil
}

// sandboxAssetsProto converts a resolved SandboxConfig into the proto atelet
// consumes.
func sandboxAssetsProto(sc *atev1alpha1.SandboxConfig) *ateletpb.SandboxAssets {
	out := &ateletpb.SandboxAssets{
		SandboxClass: string(sc.Spec.SandboxClass),
		PauseImage:   sc.Spec.PauseImage,
		Assets:       make(map[string]*ateletpb.ArchAssets, len(sc.Spec.Assets)),
	}
	for arch, files := range sc.Spec.Assets {
		archAssets := &ateletpb.ArchAssets{Files: make(map[string]*ateletpb.AssetFile, len(files))}
		for name, f := range files {
			archAssets.Files[name] = &ateletpb.AssetFile{Url: f.URL, Sha256: f.SHA256}
		}
		out.Assets[arch] = archAssets
	}
	return out
}
