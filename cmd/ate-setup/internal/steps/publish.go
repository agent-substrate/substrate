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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"

	"go.ate.dev/substrate/cmd/ate-setup/internal/images"
	"go.ate.dev/substrate/cmd/ate-setup/internal/ko"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
)

// releaseBinary is the binary a release attaches.
const releaseBinary = "kubectl-ate"

// releaseBinaryPlatforms returns the GOOS/GOARCH pairs releaseBinary is built
// for. Windows is not among them.
func releaseBinaryPlatforms() []string {
	return []string{"darwin/amd64", "darwin/arm64", "linux/amd64", "linux/arm64"}
}

// releaseBinaryName is the file name of the release binary for platform, e.g.
// kubectl-ate-linux-amd64.
func releaseBinaryName(platform string) string {
	return releaseBinary + "-" + strings.ReplaceAll(platform, "/", "-")
}

// PublishReleaseBinaries builds kubectl-ate for every releaseBinaryPlatforms
// entry into outDir, statically (CGO_ENABLED=0) and stamped with the release
// version, commit, and commit time, and writes beside each binary a .sha256
// file that `shasum -a 256 -c` checks. It writes the paths it produced to w.
// The version has to be a release tag, and the checkout has to have a commit,
// since a binary without one reports the wrong version.
func (e *Env) PublishReleaseBinaries(ctx context.Context, w io.Writer, outDir string) error {
	return e.publishReleaseBinaries(ctx, w, outDir, releaseBinaryPlatforms())
}

// publishReleaseBinaries is PublishReleaseBinaries for the given GOOS/GOARCH
// pairs.
func (e *Env) publishReleaseBinaries(ctx context.Context, w io.Writer, outDir string, platforms []string) error {
	version := e.SubstrateVersion()
	if err := images.CheckReleaseVersion(version); err != nil {
		return fmt.Errorf("the build version: %w; set VERSION to the release tag", err)
	}
	if _, _, ok := ko.CommitStamp(e.Cfg.Root); !ok {
		return fmt.Errorf("release binaries are stamped with the commit they were built from, and %s has none checked out", e.Cfg.Root)
	}
	log.Stepf("publish_release_binaries (%s)", version)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("while creating %s: %w", outDir, err)
	}
	ldflags := strings.Join(ko.Ldflags(e.Cfg.Root), " ")
	var written []string
	for _, platform := range platforms {
		goos, goarch, _ := strings.Cut(platform, "/")
		out, err := filepath.Abs(filepath.Join(outDir, releaseBinaryName(platform)))
		if err != nil {
			return err
		}
		build := exec.CommandContext(ctx, "go", "build", "-trimpath", "-ldflags="+ldflags, "-o", out, "./cmd/"+releaseBinary)
		build.Dir = e.Cfg.Root
		build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS="+goos, "GOARCH="+goarch)
		build.Stdout = os.Stderr
		build.Stderr = os.Stderr
		if err := build.Run(); err != nil {
			return fmt.Errorf("while building %s for %s: %w", releaseBinary, platform, err)
		}
		sum, err := writeChecksum(out)
		if err != nil {
			return err
		}
		written = append(written, out, sum)
	}
	if _, err := fmt.Fprintf(w, "\nRelease binaries for %s:\n", version); err != nil {
		return err
	}
	for _, path := range written {
		if _, err := fmt.Fprintln(w, path); err != nil {
			return err
		}
	}
	return nil
}

// writeChecksum writes path's SHA-256 to path.sha256 in the "<hex>  <name>"
// form `shasum -a 256 -c` and `sha256sum -c` read, naming the file without
// its directory so the check runs wherever both are downloaded. It returns
// the checksum file's path.
func writeChecksum(path string) (string, error) {
	digest, err := fileSHA256(path)
	if err != nil {
		return "", fmt.Errorf("while hashing %s: %w", path, err)
	}
	sum := path + ".sha256"
	line := digest + "  " + filepath.Base(path) + "\n"
	if err := os.WriteFile(sum, []byte(line), 0o644); err != nil {
		return "", fmt.Errorf("while writing %s: %w", sum, err)
	}
	return sum, nil
}

// workerImages are the worker images a WorkerPool points at through
// workerImage, one per sandbox class. No manifest references them, so the
// install never publishes them as a side effect.
var workerImages = []string{"ateworker-gvisor", "ateworker-microvm"}

// PublishWorkerImages builds and pushes the worker images for this build and
// writes their pushed references to w, one "<binary>: <ref>" line per image,
// after every build has finished so the refs sit together below ko's build
// output. A WorkerPool moves to this build by pointing its workerImage at
// the ref.
func (e *Env) PublishWorkerImages(ctx context.Context, w io.Writer) error {
	version := e.SubstrateVersion()
	log.Stepf("publish_worker_images (%s)", version)
	runner, err := e.koRunner()
	if err != nil {
		return err
	}
	refs := make([]string, 0, len(workerImages))
	for _, img := range workerImages {
		ref, err := runner.Build(ctx, "./cmd/"+img)
		if err != nil {
			return err
		}
		refs = append(refs, img+": "+ref)
	}
	if _, err := fmt.Fprintf(w, "\nWorker images for %s:\n", version); err != nil {
		return err
	}
	for _, line := range refs {
		if _, err := fmt.Fprintln(w, line); err != nil {
			return err
		}
	}
	return nil
}

// PublishReleaseOptions are the publish release-images settings.
type PublishReleaseOptions struct {
	// Overwrite rebuilds and replaces images the registry already holds for
	// the tag. Without it, a complete set is left alone.
	Overwrite bool
	// DigestsFile, when set, receives one "<sha256 hex>  <repo>/<image>" line
	// per image this run pushed, in the form sha256sum writes. A run that
	// builds nothing removes the file.
	DigestsFile string
	// ListPublished writes the images the registry holds for the tag to
	// DigestsFile instead, and builds nothing.
	ListPublished bool
}

// releaseImageRefs returns the tagged reference of every image a release
// publishes to repo: one per images.ReleaseComponents and
// images.ReleaseDockerfileImages entry.
func releaseImageRefs(repo, tag string) []string {
	repo = strings.TrimSuffix(repo, "/")
	var refs []string
	for _, pkg := range images.ReleaseComponents() {
		refs = append(refs, repo+"/"+images.ImageName(pkg)+":"+tag)
	}
	for _, img := range images.ReleaseDockerfileImages() {
		refs = append(refs, repo+"/"+img.Name+":"+tag)
	}
	return refs
}

// PublishReleaseImages builds and pushes every image a release publishes, the
// ko images in images.ReleaseComponents and the images.ReleaseDockerfileImages
// built with docker buildx, all tagged with the build version, and writes
// their pushed references to w. The version has to be a release tag.
//
// If the registry already holds every image for the tag on every platform,
// nothing is built unless opts.Overwrite is set, so rerunning a release never
// replaces published digests by accident. An incomplete set, left by a failed
// run, is rebuilt in full. After pushing, every tag is checked to name the
// digest just pushed, on every platform.
func (e *Env) PublishReleaseImages(ctx context.Context, w io.Writer, opts PublishReleaseOptions) error {
	if e.Cfg.KODockerRepo == "" {
		return fmt.Errorf("publishing release images needs a registry to push to; set KO_DOCKER_REPO or --ko-docker-repo")
	}
	tag := e.SubstrateVersion()
	if err := images.CheckReleaseTag(e.Cfg.KODockerRepo, tag); err != nil {
		return fmt.Errorf("the build version: %w; set VERSION to the release tag", err)
	}
	tagged := releaseImageRefs(e.Cfg.KODockerRepo, tag)
	if opts.ListPublished {
		if opts.DigestsFile == "" {
			return fmt.Errorf("--list-published writes to --digests-file; set it")
		}
		published, err := publishedDigests(ctx, tagged)
		if err != nil {
			return err
		}
		return writeDigestsFile(opts.DigestsFile, published)
	}
	log.Stepf("publish_release_images (%s)", tag)
	platforms, err := images.Platforms(e.Cfg.Root, e.Cfg.KODefaultPlatforms)
	if err != nil {
		return err
	}
	if !opts.Overwrite {
		incomplete, err := images.CheckPublished(ctx, tagged, platforms)
		if err != nil {
			return err
		}
		if len(incomplete) == 0 {
			log.Infof("%s already holds every image for %s; nothing built. Pass --overwrite to rebuild and replace them.",
				e.Cfg.KODockerRepo, tag)
			if opts.DigestsFile == "" {
				return nil
			}
			if err := os.Remove(opts.DigestsFile); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("while removing %s: %w", opts.DigestsFile, err)
			}
			return nil
		}
		for _, i := range incomplete {
			log.Infof("to build: %s", i)
		}
	}

	runner, err := e.koRunner()
	if err != nil {
		return err
	}
	var pkgs []string
	for _, pkg := range images.ReleaseComponents() {
		pkgs = append(pkgs, "./"+pkg)
	}
	revision, _, ok := ko.CommitStamp(e.Cfg.Root)
	if !ok {
		return fmt.Errorf("release images are labeled with the commit they were built from, and %s has none checked out", e.Cfg.Root)
	}
	labels := []string{
		images.SourceLabel + "=" + images.SourceURL,
		images.RevisionLabel + "=" + revision,
		images.VersionLabel + "=" + tag,
	}
	// The Dockerfile images are slower to build and likelier to fail, so they
	// go first: a failure there publishes nothing under the tag. They ignore
	// DOCKER_BUILD_FLAGS, so a release restores no build cache.
	var dockerFlags []string
	for _, label := range labels {
		dockerFlags = append(dockerFlags, "--label="+label)
	}
	var refs []string
	for _, img := range images.ReleaseDockerfileImages() {
		ref, err := images.PublishDockerfileImage(ctx, e.Cfg.Root, e.Cfg.KODockerRepo, img.Name, tag,
			e.Cfg.Path(img.Context), e.Cfg.KODefaultPlatforms, dockerFlags)
		if err != nil {
			return err
		}
		refs = append(refs, ref)
	}
	koRefs, err := runner.BuildEach(ctx, tag, labels, pkgs...)
	if err != nil {
		return err
	}
	refs = append(refs, koRefs...)

	incomplete, err := images.CheckPublished(ctx, tagged, platforms)
	if err != nil {
		return err
	}
	if len(incomplete) > 0 {
		lines := make([]string, 0, len(incomplete))
		for _, i := range incomplete {
			lines = append(lines, i.String())
		}
		return fmt.Errorf("after publishing, %d image(s) are incomplete:\n  %s", len(incomplete), strings.Join(lines, "\n  "))
	}
	stale, err := images.StaleTags(ctx, tagged, refs)
	if err != nil {
		return err
	}
	if len(stale) > 0 {
		return fmt.Errorf("after publishing, %d tag(s) do not name the pushed image:\n  %s", len(stale), strings.Join(stale, "\n  "))
	}
	if opts.DigestsFile != "" {
		if err := writeDigestsFile(opts.DigestsFile, refs); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "\nRelease images for %s:\n", tag); err != nil {
		return err
	}
	for _, ref := range refs {
		if _, err := fmt.Fprintln(w, ref); err != nil {
			return err
		}
	}
	return nil
}

// publishedDigests returns a digest reference for each of tagged, as the
// registry resolves it. A tag the registry does not hold is an error.
func publishedDigests(ctx context.Context, tagged []string) ([]string, error) {
	refs := make([]string, 0, len(tagged))
	for _, ref := range tagged {
		digest, err := images.RemoteDigest(ctx, ref)
		if err != nil {
			return nil, err
		}
		refs = append(refs, ref+"@"+digest)
	}
	return refs, nil
}

// writeDigestsFile writes one "<sha256 hex>  <repository>" line per digest
// reference in refs to path, sorted by repository, creating the directory.
// refs are repo/name:tag@sha256:... or repo/name@sha256:..., as ko prints them.
func writeDigestsFile(path string, refs []string) error {
	type line struct{ repo, hex string }
	lines := make([]line, 0, len(refs))
	for _, ref := range refs {
		d, err := name.NewDigest(ref)
		if err != nil {
			return fmt.Errorf("%s is not a digest reference: %w", ref, err)
		}
		algorithm, hex, ok := strings.Cut(d.DigestStr(), ":")
		if !ok || algorithm != "sha256" {
			return fmt.Errorf("%s is not a sha256 digest reference", ref)
		}
		lines = append(lines, line{repo: d.Context().Name(), hex: hex})
	}
	slices.SortFunc(lines, func(a, b line) int { return strings.Compare(a.repo, b.repo) })
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("while creating %s: %w", filepath.Dir(path), err)
	}
	var b strings.Builder
	for _, l := range lines {
		fmt.Fprintf(&b, "%s  %s\n", l.hex, l.repo)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("while writing %s: %w", path, err)
	}
	return nil
}
