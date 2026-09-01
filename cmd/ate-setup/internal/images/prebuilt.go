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
	"regexp"
	"sort"
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
// It is the drop-in counterpart to ko.Runner: same two methods, no registry
// writes, no builds, and no repository checkout beyond the manifests
// themselves.
type Prebuilt struct {
	src Source
}

// NewPrebuilt returns a resolver for an already-published image set.
func NewPrebuilt(src Source) *Prebuilt {
	return &Prebuilt{src: src}
}

// ResolvePath rewrites the manifests a path covers.
func (p *Prebuilt) ResolvePath(_ context.Context, path string) ([]byte, error) {
	manifest, err := kube.ReadPath(path)
	if err != nil {
		return nil, err
	}
	out, err := p.rewrite(manifest)
	if err != nil {
		return nil, fmt.Errorf("in %s: %w", path, err)
	}
	return out, nil
}

// ResolveBytes rewrites an in-memory manifest, such as kustomize output.
func (p *Prebuilt) ResolveBytes(_ context.Context, manifest []byte) ([]byte, error) {
	return p.rewrite(manifest)
}

// rewrite replaces every ko:// reference with the image it maps to.
//
// Substitution is textual, the same way SubstituteVersion fills
// ${SUBSTRATE_VERSION}. References appear in CRD fields as well as pod specs (a
// WorkerPool's spec.workerImage, an ActorTemplate's image), so a schema walk
// would need to know every such field, and rewriting the bytes leaves
// everything else -- the multi-kilobyte Envoy configuration blocks in
// particular -- exactly as committed.
func (p *Prebuilt) rewrite(manifest []byte) ([]byte, error) {
	// Report every unmappable reference at once. Fixing them one install
	// attempt at a time is slow, and the failures are usually related.
	unknown := make(map[string]bool)

	out := koRef.ReplaceAllFunc(manifest, func(match []byte) []byte {
		image, ok := imageByRef[string(match)]
		if !ok {
			unknown[string(match)] = true
			return match
		}
		return []byte(p.src.Repo + "/" + image + ":" + p.src.Tag)
	})

	if len(unknown) > 0 {
		refs := make([]string, 0, len(unknown))
		for ref := range unknown {
			refs = append(refs, ref)
		}
		sort.Strings(refs)

		// One line per reference, then the package list once. Repeating the
		// list per reference buries the references themselves.
		errs := make([]error, 0, len(refs)+1)
		for _, ref := range refs {
			errs = append(errs, fmt.Errorf("%s has no published image", ref))
		}
		errs = append(errs, fmt.Errorf("installable packages under %s are %s; a reference that is "+
			"templated has to be rendered before it can be resolved",
			ModulePath, strings.Join(Components, ", ")))
		return nil, errors.Join(errs...)
	}
	return out, nil
}
