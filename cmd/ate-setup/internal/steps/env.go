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
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/images"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/ko"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kustomize"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
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

// imageResolver turns a manifest whose image references are ko:// import paths
// into one whose references are pullable. *ko.Runner builds and publishes them
// from source; *images.Prebuilt maps them onto an already-published release.
type imageResolver interface {
	ResolvePath(ctx context.Context, path string) ([]byte, error)
	ResolveBytes(ctx context.Context, manifest []byte) ([]byte, error)
}

// Env is the shared execution context for every step: resolved configuration
// plus the clients needed to act on the cluster.
type Env struct {
	Cfg  *config.Config
	Kube *kube.Client

	// resolver is created lazily. Steps that only apply static manifests must
	// work on a machine with no container registry credentials configured, and
	// the build-from-source resolver fails at construction without them.
	resolver imageResolver

	// substrateVersion caches the derived build version and its object-name
	// suffix; see SubstrateVersion.
	substrateVersion       string
	substrateVersionSuffix string
}

// NewEnv connects to the cluster described by cfg.
func NewEnv(cfg *config.Config) (*Env, error) {
	client, err := kube.New(cfg.Kubeconfig, cfg.Context)
	if err != nil {
		return nil, err
	}
	return &Env{Cfg: cfg, Kube: client}, nil
}

// imageResolver returns the image resolver, creating it on first use.
func (e *Env) imageResolver() (imageResolver, error) {
	if e.resolver != nil {
		return e.resolver, nil
	}

	log.Stepf("images: %s", e.Cfg.Images.Describe())
	if e.Cfg.Images.IsPrebuilt() {
		e.resolver = images.NewPrebuilt(e.Cfg.Images, images.RemoteDigest)
		return e.resolver, nil
	}

	runner, err := ko.New(e.Cfg.Root, e.Cfg.KoEnv())
	if err != nil {
		return nil, err
	}
	e.resolver = runner
	return e.resolver, nil
}

// koRunner returns the resolver as the ko runner it is, for the steps that
// build an image rather than only resolve a reference to one. Building is not
// part of imageResolver because the pre-built resolver cannot do it:
// --image-repo names images someone else published, and the tag it installs
// need not correspond to anything in this checkout.
func (e *Env) koRunner() (*ko.Runner, error) {
	if e.Cfg.Images.IsPrebuilt() {
		return nil, fmt.Errorf("publishing images builds them from source, which --image-repo does not do; " +
			"drop it to build and push from this checkout")
	}
	resolver, err := e.imageResolver()
	if err != nil {
		return nil, err
	}
	runner, ok := resolver.(*ko.Runner)
	if !ok {
		return nil, fmt.Errorf("image resolver is %T, not a ko runner", resolver)
	}
	return runner, nil
}

// ResolveAndApply resolves the images in a manifest path and applies the
// result. This is the run_ko apply of the shell scripts, split into its two
// real steps.
func (e *Env) ResolveAndApply(ctx context.Context, path string) error {
	manifest, err := e.ResolveManifest(ctx, path)
	if err != nil {
		return err
	}
	return e.Kube.ApplyBytes(ctx, manifest)
}

// ResolveManifest turns the ko:// references in a manifest path into pullable
// image references.
func (e *Env) ResolveManifest(ctx context.Context, path string) ([]byte, error) {
	resolver, err := e.imageResolver()
	if err != nil {
		return nil, err
	}
	return resolver.ResolvePath(ctx, path)
}

// ResolveManifestBytes resolves an in-memory manifest, such as kustomize output.
func (e *Env) ResolveManifestBytes(ctx context.Context, manifest []byte) ([]byte, error) {
	resolver, err := e.imageResolver()
	if err != nil {
		return nil, err
	}
	return resolver.ResolveBytes(ctx, manifest)
}

// ResolveAndApplyBytes resolves an in-memory manifest and applies the result.
func (e *Env) ResolveAndApplyBytes(ctx context.Context, manifest []byte) error {
	resolved, err := e.ResolveManifestBytes(ctx, manifest)
	if err != nil {
		return err
	}
	return e.Kube.ApplyBytes(ctx, resolved)
}

// Kustomize renders an overlay directory under the repository root.
func (e *Env) Kustomize(overlay string) ([]byte, error) {
	return kustomize.Build(e.Cfg.Path(overlay))
}

// KustomizeResolve renders an overlay and resolves its images, the
// `kubectl kustomize ... | run_ko resolve -f -` pipeline.
func (e *Env) KustomizeResolve(ctx context.Context, overlay string) ([]byte, error) {
	built, err := e.Kustomize(overlay)
	if err != nil {
		return nil, err
	}
	return e.ResolveManifestBytes(ctx, built)
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
