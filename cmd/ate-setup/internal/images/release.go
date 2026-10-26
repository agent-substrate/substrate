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
	"net/http"
	"regexp"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
)

// SourceLabel is the OCI image label naming the repository an image was built
// from. ghcr.io connects a new package carrying it to that repository, which
// lets the repository's workflows push later versions with their GITHUB_TOKEN.
const SourceLabel = "org.opencontainers.image.source"

// SourceURL is the SourceLabel value for images built from this repository:
// its GitHub URL, which ghcr.io matches. It cannot be derived from ModulePath,
// which is a vanity import path.
const SourceURL = "https://github.com/agent-substrate/substrate"

// RevisionLabel is the OCI image label naming the commit an image was built
// from.
const RevisionLabel = "org.opencontainers.image.revision"

// releaseTag matches a release tag, vMAJOR.MINOR.PATCH, or a release candidate,
// vMAJOR.MINOR.PATCH-PRERELEASE. hack/release/draft-release.sh accepts the same.
var releaseTag = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$`)

// CheckReleaseVersion returns an error unless version is a release tag.
func CheckReleaseVersion(version string) error {
	if !releaseTag.MatchString(version) {
		return fmt.Errorf("%q is not a release tag (vMAJOR.MINOR.PATCH[-PRERELEASE])", version)
	}
	return nil
}

// CheckReleaseTag returns an error unless tag is a release tag that makes a
// valid image reference under repo. It runs before anything is built, so a
// stray tag or a value ko would split, such as "v1.2.3,latest", publishes
// nothing.
func CheckReleaseTag(repo, tag string) error {
	if err := CheckReleaseVersion(tag); err != nil {
		return err
	}
	if _, err := name.NewTag(strings.TrimSuffix(repo, "/")+"/image:"+tag, name.StrictValidation); err != nil {
		return fmt.Errorf("%s with tag %s is not a valid image reference: %w", repo, tag, err)
	}
	return nil
}

// StaleTags returns a line for each of tagged whose tag does not name the
// digest pushed for its repository. pushed are digest references, one per
// repository, as ko and buildx print them (repo/name:tag@sha256:... or
// repo/name@sha256:...). It catches a tag that still names an earlier push,
// which a platform check alone would pass.
func StaleTags(ctx context.Context, tagged, pushed []string) ([]string, error) {
	want := make(map[string]string, len(pushed))
	for _, ref := range pushed {
		d, err := name.NewDigest(ref)
		if err != nil {
			return nil, fmt.Errorf("%s is not a digest reference: %w", ref, err)
		}
		want[d.Context().Name()] = d.DigestStr()
	}
	var stale []string
	for _, ref := range tagged {
		t, err := name.NewTag(ref)
		if err != nil {
			return nil, fmt.Errorf("%s is not a tagged reference: %w", ref, err)
		}
		expected, ok := want[t.Context().Name()]
		if !ok {
			stale = append(stale, fmt.Sprintf("%s: nothing was pushed for it", ref))
			continue
		}
		got, err := RemoteDigest(ctx, ref)
		if err != nil {
			return nil, err
		}
		if got != expected {
			stale = append(stale, fmt.Sprintf("%s names %s, not the pushed %s", ref, got, expected))
		}
	}
	return stale, nil
}

// Platforms returns the platforms a build produces, the way ko chooses them:
// KO_DEFAULTPLATFORMS, else defaultPlatforms from the ko config, else
// linux/amd64. Dockerfile-built images use the same set.
func Platforms(rootDir, koDefaultPlatforms string) ([]string, error) {
	platforms, err := dockerfilePlatforms(rootDir, koDefaultPlatforms)
	if err != nil {
		return nil, err
	}
	return strings.Split(platforms, ","), nil
}

// Incomplete is an image reference that is missing from its registry, or is
// present without every wanted platform.
type Incomplete struct {
	Ref string
	// Missing lists the wanted platforms the reference lacks. A reference that
	// does not exist lacks all of them.
	Missing []string
}

func (i Incomplete) String() string {
	return fmt.Sprintf("%s lacks %s", i.Ref, strings.Join(i.Missing, ", "))
}

// CheckPublished returns the refs that do not hold an image for each of
// platforms. An error is returned only when a registry
// cannot answer (network, credentials), since that says nothing about whether
// an image exists; treating it as missing would lead a caller to rebuild and
// replace images that are already published.
func CheckPublished(ctx context.Context, refs, platforms []string) ([]Incomplete, error) {
	want := make([]v1.Platform, 0, len(platforms))
	for _, p := range platforms {
		parsed, err := v1.ParsePlatform(p)
		if err != nil {
			return nil, fmt.Errorf("%q is not a platform: %w", p, err)
		}
		want = append(want, *parsed)
	}

	var incomplete []Incomplete
	for _, ref := range refs {
		have, err := indexPlatforms(ctx, ref)
		if err != nil {
			return nil, err
		}
		var missing []string
		for i, w := range want {
			if !satisfiedBy(w, have) {
				missing = append(missing, platforms[i])
			}
		}
		if len(missing) > 0 {
			incomplete = append(incomplete, Incomplete{Ref: ref, Missing: missing})
		}
	}
	return incomplete, nil
}

// indexPlatforms returns the platforms of the images ref names: each image in
// an index, or the one image of a plain manifest, which ko pushes when a build
// targets a single platform. It returns none, without an error, if ref does
// not exist.
func indexPlatforms(ctx context.Context, ref string) ([]v1.Platform, error) {
	parsed, err := name.ParseReference(ref)
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid image reference: %w", ref, err)
	}
	desc, err := remote.Get(parsed,
		remote.WithContext(ctx),
		remote.WithAuthFromKeychain(keychain),
	)
	if isNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("while inspecting %s: %w", ref, err)
	}
	if !desc.MediaType.IsIndex() {
		img, err := desc.Image()
		if err != nil {
			return nil, fmt.Errorf("while reading the image %s names: %w", ref, err)
		}
		config, err := img.ConfigFile()
		if err != nil {
			return nil, fmt.Errorf("while reading the config of %s: %w", ref, err)
		}
		return []v1.Platform{{OS: config.OS, Architecture: config.Architecture, Variant: config.Variant}}, nil
	}
	index, err := desc.ImageIndex()
	if err != nil {
		return nil, fmt.Errorf("while reading the index of %s: %w", ref, err)
	}
	manifest, err := index.IndexManifest()
	if err != nil {
		return nil, fmt.Errorf("while reading the index of %s: %w", ref, err)
	}
	var have []v1.Platform
	for _, m := range manifest.Manifests {
		if m.Platform != nil {
			have = append(have, *m.Platform)
		}
	}
	return have, nil
}

// isNotFound reports whether err is a registry saying the repository or the
// tag does not exist.
func isNotFound(err error) bool {
	var terr *transport.Error
	if !errors.As(err, &terr) {
		return false
	}
	if terr.StatusCode == http.StatusNotFound {
		return true
	}
	for _, d := range terr.Errors {
		if d.Code == transport.ManifestUnknownErrorCode || d.Code == transport.NameUnknownErrorCode {
			return true
		}
	}
	return false
}

// satisfiedBy reports whether any of have is an image for want. A want without
// a variant, such as linux/arm64, accepts any variant.
func satisfiedBy(want v1.Platform, have []v1.Platform) bool {
	for _, h := range have {
		if h.Satisfies(want) {
			return true
		}
	}
	return false
}
