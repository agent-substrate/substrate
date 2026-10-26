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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

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
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("while hashing %s: %w", path, err)
	}
	sum := path + ".sha256"
	line := hex.EncodeToString(h.Sum(nil)) + "  " + filepath.Base(path) + "\n"
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
}

// releaseImageRefs returns the tagged reference of every image a release
// publishes to repo: one per images.ReleaseComponents entry.
func releaseImageRefs(repo, tag string) []string {
	repo = strings.TrimSuffix(repo, "/")
	pkgs := images.ReleaseComponents()
	refs := make([]string, 0, len(pkgs))
	for _, pkg := range pkgs {
		refs = append(refs, repo+"/"+images.ImageName(pkg)+":"+tag)
	}
	return refs
}

// PublishReleaseImages builds and pushes every image a release publishes, the
// ko images in images.ReleaseComponents, all tagged with the build version,
// and writes their pushed references to w. The version has to be a release
// tag. Releases do not publish envoy-dataplane, the one Dockerfile-built
// image, so nothing here needs docker.
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
	log.Stepf("publish_release_images (%s)", tag)
	platforms, err := images.Platforms(e.Cfg.Root, e.Cfg.KODefaultPlatforms)
	if err != nil {
		return err
	}
	tagged := releaseImageRefs(e.Cfg.KODockerRepo, tag)
	if !opts.Overwrite {
		incomplete, err := images.CheckPublished(ctx, tagged, platforms)
		if err != nil {
			return err
		}
		if len(incomplete) == 0 {
			log.Infof("%s already holds every image for %s; nothing built. Pass --overwrite to rebuild and replace them.",
				e.Cfg.KODockerRepo, tag)
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
	}
	refs, err := runner.BuildTagged(ctx, tag, labels, pkgs...)
	if err != nil {
		return err
	}

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
