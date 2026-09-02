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

package images

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
)

// koRef matches a whole ko:// reference: the prefix plus everything up to the
// first delimiter that can end a YAML scalar.
//
// Matching the whole reference is what makes the rewrite safe. Replacing the
// table's literals directly would rewrite a prefix of a longer reference, so
// ko://<module>/cmd/atelet-sidecar would silently become the atelet image with
// "-sidecar" left dangling on the tag. Requiring a character after the prefix
// also keeps the "# ko:// image reference" prose in the demo templates from
// matching.
var koRef = regexp.MustCompile(`ko://[^\s"',\]}]+`)

// imageByRef maps a ko:// reference to the image name it publishes as.
// Rewriting is a lookup in this table.
var imageByRef = func() map[string]string {
	m := make(map[string]string, len(Components))
	for _, pkg := range Components {
		m[KoReference(pkg)] = ImageName(pkg)
	}
	return m
}()

// Prebuilt rewrites ko:// references to point at already-published images.
//
// It is the drop-in counterpart to ko.Runner: same two methods, no builds, no
// registry writes, and no repository checkout beyond the manifests themselves.
// What it does need is read access to the registry, to pin each tag to a
// digest.
type Prebuilt struct {
	src    Source
	digest Digester

	// pinned caches a resolved reference by its tagged form, so a component
	// several manifests name costs one registry lookup per install.
	pinned map[string]string
}

// NewPrebuilt returns a resolver for an already-published image set. digest
// turns a tag into the image it names; callers outside tests pass RemoteDigest.
func NewPrebuilt(src Source, digest Digester) *Prebuilt {
	return &Prebuilt{src: src, digest: digest, pinned: make(map[string]string)}
}

// ResolvePath rewrites the manifests a path covers.
func (p *Prebuilt) ResolvePath(ctx context.Context, path string) ([]byte, error) {
	manifest, err := kube.ReadPath(path)
	if err != nil {
		return nil, err
	}
	out, err := p.rewrite(ctx, manifest)
	if err != nil {
		return nil, fmt.Errorf("in %s: %w", path, err)
	}
	return out, nil
}

// ResolveBytes rewrites an in-memory manifest, such as kustomize output.
func (p *Prebuilt) ResolveBytes(ctx context.Context, manifest []byte) ([]byte, error) {
	return p.rewrite(ctx, manifest)
}

// pin returns the reference to install an image by: its tag, and the digest
// that tag currently names.
//
// The digest is not decoration. An ActorTemplate's container image, an image
// volume's reference, and a SandboxConfig's pauseImage each carry the CEL rule
// self.contains('@'), so a tag on its own is rejected by admission and every
// demo would fail to deploy. Pinning also restores what ko gave the control
// plane deployments for free: an install that cannot shift under a tag someone
// moves later. Keeping the tag alongside the digest is ko's own output shape,
// and leaves the reference legible in `kubectl get`.
func (p *Prebuilt) pin(ctx context.Context, image string) (string, error) {
	tagged := p.src.Repo + "/" + image + ":" + p.src.Tag
	if ref, ok := p.pinned[tagged]; ok {
		return ref, nil
	}
	digest, err := p.digest(ctx, tagged)
	if err != nil {
		return "", err
	}
	ref := tagged + "@" + digest
	p.pinned[tagged] = ref
	return ref, nil
}

// rewrite replaces every ko:// reference with the image it maps to.
//
// Substitution is textual, the same way SubstituteVersion fills
// ${SUBSTRATE_VERSION}. References appear in CRD fields as well as pod specs (a
// WorkerPool's spec.workerImage, an ActorTemplate's image), so a schema walk
// would need to know every such field, and rewriting the bytes leaves
// everything else -- the multi-kilobyte Envoy configuration blocks in
// particular -- exactly as committed.
func (p *Prebuilt) rewrite(ctx context.Context, manifest []byte) ([]byte, error) {
	// Collect the failures rather than stopping at the first. Diagnosing them
	// one install attempt at a time is slow, and they are usually related: a
	// registry that is unreachable or a tag that was never pushed fails for
	// every component at once.
	unknown := make(map[string]bool)
	unpinned := make(map[string]error)

	out := koRef.ReplaceAllFunc(manifest, func(match []byte) []byte {
		image, ok := imageByRef[string(match)]
		if !ok {
			unknown[string(match)] = true
			return match
		}
		ref, err := p.pin(ctx, image)
		if err != nil {
			unpinned[image] = err
			return match
		}
		return []byte(ref)
	})

	var errs []error
	if len(unknown) > 0 {
		// One line per reference, then the package list once. Repeating the
		// list per reference buries the references themselves.
		for _, ref := range slices.Sorted(maps.Keys(unknown)) {
			errs = append(errs, fmt.Errorf("%s has no published image", ref))
		}
		errs = append(errs, fmt.Errorf("installable packages under %s are %s; a reference that is "+
			"templated has to be rendered before it can be resolved",
			ModulePath, strings.Join(Components, ", ")))
	}
	for _, image := range slices.Sorted(maps.Keys(unpinned)) {
		errs = append(errs, unpinned[image])
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return out, nil
}
