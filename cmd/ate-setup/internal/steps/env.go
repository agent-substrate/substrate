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

// Package steps holds the install and teardown operations that the shell
// installer implemented as bash functions. Each exported function corresponds
// to one of those and keeps its name in the [step] log line.
package steps

import (
	"context"
	"fmt"
	"time"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/ko"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kustomize"
)

// Timeouts carried over from the --timeout values in the shell installer.
const (
	NamespaceTimeout = 60 * time.Second
	// BootstrapTimeout covers the waits the scripts fixed at 120s rather than
	// deriving from --rollout-timeout: the podcertificate controller and the
	// CSI drivers. Pass it through Config.WaitTimeout, which lets the flag
	// raise it without its shorter default lowering it.
	BootstrapTimeout = 120 * time.Second
	// DemoTimeout is longer because a cold cluster pays one-time costs on the
	// first ActorTemplate: downloading runsc, the first gVisor pod start, and
	// image pulls.
	DemoTimeout = 300 * time.Second
)

// Well-known namespaces.
const (
	NamespaceAteSystem = "ate-system"
	NamespacePodCert   = "podcertificate-controller-system"
)

// Env is the shared execution context for every step: resolved configuration
// plus the clients needed to act on the cluster.
type Env struct {
	Cfg  *config.Config
	Kube *kube.Client

	// ko is created lazily. Steps that only apply static manifests must work
	// on a machine with no container registry credentials configured.
	ko *ko.Runner
}

// NewEnv connects to the cluster described by cfg.
func NewEnv(cfg *config.Config) (*Env, error) {
	client, err := kube.New(cfg.Kubeconfig, cfg.Context)
	if err != nil {
		return nil, err
	}
	return &Env{Cfg: cfg, Kube: client}, nil
}

// Ko returns the ko runner, creating it on first use.
func (e *Env) Ko() (*ko.Runner, error) {
	if e.ko != nil {
		return e.ko, nil
	}
	runner, err := ko.New(e.Cfg.Root, e.Cfg.KoEnv())
	if err != nil {
		return nil, err
	}
	e.ko = runner
	return e.ko, nil
}

// KoApply resolves a manifest path with ko and applies the result. This is the
// run_ko apply of the shell scripts, split into its two real steps.
func (e *Env) KoApply(ctx context.Context, path string) error {
	manifest, err := e.KoResolve(ctx, path)
	if err != nil {
		return err
	}
	return e.Kube.ApplyBytes(ctx, manifest)
}

// KoResolve builds and publishes the images in a manifest path, returning the
// digest-pinned manifest.
func (e *Env) KoResolve(ctx context.Context, path string) ([]byte, error) {
	runner, err := e.Ko()
	if err != nil {
		return nil, err
	}
	return runner.ResolvePath(ctx, path)
}

// KoResolveBytes resolves an in-memory manifest, such as kustomize output.
func (e *Env) KoResolveBytes(ctx context.Context, manifest []byte) ([]byte, error) {
	runner, err := e.Ko()
	if err != nil {
		return nil, err
	}
	return runner.ResolveBytes(ctx, manifest)
}

// KoApplyBytes resolves an in-memory manifest with ko and applies the result.
func (e *Env) KoApplyBytes(ctx context.Context, manifest []byte) error {
	resolved, err := e.KoResolveBytes(ctx, manifest)
	if err != nil {
		return err
	}
	return e.Kube.ApplyBytes(ctx, resolved)
}

// Kustomize renders an overlay directory under the repository root.
func (e *Env) Kustomize(overlay string) ([]byte, error) {
	return kustomize.Build(e.Cfg.Path(overlay))
}

// KustomizeResolve renders an overlay and pipes it through ko, the
// `kubectl kustomize ... | run_ko resolve -f -` pipeline.
func (e *Env) KustomizeResolve(ctx context.Context, overlay string) ([]byte, error) {
	built, err := e.Kustomize(overlay)
	if err != nil {
		return nil, err
	}
	return e.KoResolveBytes(ctx, built)
}

// EnsureAteSystemNamespace applies the ate-system namespace manifest and waits
// for it to go Active. Every deploy path starts here so that RBAC, ConfigMaps,
// and workloads have somewhere to land.
func (e *Env) EnsureAteSystemNamespace(ctx context.Context) error {
	if err := e.Kube.ApplyPath(ctx, e.Cfg.Manifest("ate-system-namespace.yaml")); err != nil {
		return err
	}
	return e.Kube.WaitNamespaceActive(ctx, NamespaceAteSystem, NamespaceTimeout)
}

// RequireKind fails a step that only makes sense on a local Kind cluster. The
// Kind-only demos, which live outside this package, use it too.
func (e *Env) RequireKind(what string) error {
	if !e.Cfg.Kind {
		return fmt.Errorf("%s is only supported for Kind installations; re-run with --kind", what)
	}
	return nil
}
