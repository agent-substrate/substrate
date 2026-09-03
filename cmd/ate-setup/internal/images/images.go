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

// Package images decides where ate-setup gets container images from.
//
// The manifests name images as ko:// import paths, which ko builds and
// publishes on every install. That needs a checkout, a Go toolchain, and a
// registry to push to. Installing an already-published release instead means
// mapping those same references onto images that exist, which is what Source
// describes and Prebuilt performs.
package images

import (
	"fmt"
	"path"
)

// ModulePath is this repository's module path. A ko:// reference is it joined
// with a component's package.
const ModulePath = "github.com/agent-substrate/substrate"

// Components is every package the installer can deploy a published image for,
// as an import path within the module.
//
// The list is closed on purpose. A reference that is not in it names something
// this build has no published image for, and failing beats guessing at a name
// that was never pushed. TestComponentsCoverTheManifests keeps it in step with
// the manifests, which is half of it: a new component also has to be published
// under the same repository and tag as the rest before a pre-built install can
// use it.
var Components = []string{
	"cmd/ateapi",
	"cmd/atecontroller",
	"cmd/atelet",
	"cmd/atenet",
	"cmd/ateom-gvisor",
	"cmd/ateom-microvm",
	"cmd/podcertcontroller",
	"demos/counter",
	"demos/egress",
	"demos/multi-template/fspersist",
	"demos/sandbox",
}

// KoReference is the ko:// reference the manifests name pkg by.
//
// ko resolves a manifest reference as written, so a "./"-relative import path
// works there too. Naming packages in full is this repository's convention
// rather than ko's rule, and only the full form maps to an image here:
// TestComponentsCoverTheManifests holds the convention, and a reference spelled
// any other way fails the install rather than resolving.
func KoReference(pkg string) string { return "ko://" + ModulePath + "/" + pkg }

// ImageName is the image name pkg is published under: the last element of the
// import path, which is ko's --base-import-paths naming. ko lowercases it as
// well, which changes nothing while every package here already is.
func ImageName(pkg string) string { return path.Base(pkg) }

// Source describes where images come from.
//
// The zero value builds and publishes them from source with ko. Setting Repo
// switches to images already published in a registry, and ko is never invoked.
type Source struct {
	// Repo is the registry path holding the component images, without a
	// trailing slash, e.g. "registry.example.com/substrate".
	Repo string
	// Tag is the tag every component image carries. Releases tag all
	// components together, so one tag covers the set.
	Tag string
}

// IsPrebuilt reports whether images should be pulled rather than built.
func (s Source) IsPrebuilt() bool { return s.Repo != "" }

// Validate checks a Source is usable before anything reaches the cluster.
//
// Both halves are checked. A tag on its own would otherwise be discarded in
// silence, leaving a build from source that looks like the release the tag
// names.
func (s Source) Validate() error {
	switch {
	case s.IsPrebuilt() && s.Tag == "":
		return fmt.Errorf("--image-repo (or ATE_IMAGE_REPO) requires --image-tag (or ATE_IMAGE_TAG)")
	case !s.IsPrebuilt() && s.Tag != "":
		return fmt.Errorf("--image-tag (or ATE_IMAGE_TAG) requires --image-repo (or ATE_IMAGE_REPO); without one, images are built from source and the tag names nothing")
	}
	return nil
}

// Describe renders the source for the install log.
func (s Source) Describe() string {
	if !s.IsPrebuilt() {
		return "built from source with ko"
	}
	return fmt.Sprintf("pre-built from %s, tag %s", s.Repo, s.Tag)
}
