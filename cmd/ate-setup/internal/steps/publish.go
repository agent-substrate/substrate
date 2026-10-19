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
	"fmt"
	"io"
	"strings"

	"go.ate.dev/substrate/cmd/ate-setup/internal/images"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
)

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
	revision, err := images.Revision(ctx, e.Cfg.Root)
	if err != nil {
		return err
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
