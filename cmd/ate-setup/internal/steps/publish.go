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
	"slices"
	"strings"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/images"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// workerImages are the ateom images a WorkerPool points at through
// workerImage, one per sandbox class. No manifest references them, so the
// install never publishes them as a side effect.
var workerImages = []string{"ateom-gvisor", "ateom-microvm"}

// PublishWorkerImages builds and pushes the ateom images for this build and
// writes their pushed references to w, one "<binary>: <ref>" line per image,
// after every build has finished so the refs sit together below ko's build
// output. A WorkerPool moves to this build by pointing its workerImage at
// the ref.
func (e *Env) PublishWorkerImages(ctx context.Context, w io.Writer) error {
	version, _, err := e.SubstrateVersion()
	if err != nil {
		return err
	}
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
	// Force rebuilds and replaces images the registry already holds for the
	// tag. Without it, a complete set is left alone.
	Force bool
}

// releaseImageRefs returns the tagged reference of every image a release
// publishes to repo: one per images.Components entry, plus envoy-dataplane.
func releaseImageRefs(repo, tag string) []string {
	repo = strings.TrimSuffix(repo, "/")
	refs := make([]string, 0, len(images.Components)+1)
	for _, pkg := range images.Components {
		refs = append(refs, repo+"/"+images.ImageName(pkg)+":"+tag)
	}
	return append(refs, repo+"/"+envoyDataplaneImage+":"+tag)
}

// PublishReleaseImages builds and pushes every image a pre-built install
// needs, all tagged with the build version, and writes their pushed references
// to w. That is what `deploy --image-repo KO_DOCKER_REPO --image-tag VERSION`
// installs: the ko images in images.Components, plus envoy-dataplane, which is
// built from a Dockerfile.
//
// If the registry already holds every image for the tag on every platform,
// nothing is built unless opts.Force is set, so rerunning a release never
// replaces published digests by accident. An incomplete set, left by a failed
// run, is rebuilt in full. After pushing, every image is checked for every
// platform.
func (e *Env) PublishReleaseImages(ctx context.Context, w io.Writer, opts PublishReleaseOptions) error {
	if e.Cfg.KODockerRepo == "" {
		return fmt.Errorf("publishing release images needs a registry to push to; set KO_DOCKER_REPO or --ko-docker-repo")
	}
	tag, _, err := e.SubstrateVersion()
	if err != nil {
		return err
	}
	log.Stepf("publish_release_images (%s)", tag)
	platforms, err := images.Platforms(e.Cfg.Root, e.Cfg.KODefaultPlatforms)
	if err != nil {
		return err
	}
	tagged := releaseImageRefs(e.Cfg.KODockerRepo, tag)
	if !opts.Force {
		incomplete, err := images.CheckPublished(ctx, tagged, platforms)
		if err != nil {
			return err
		}
		if len(incomplete) == 0 {
			log.Infof("%s already holds every image for %s; nothing built. Pass --force to rebuild and replace them.",
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
	pkgs := make([]string, 0, len(images.Components))
	for _, pkg := range images.Components {
		pkgs = append(pkgs, "./"+pkg)
	}
	source := images.SourceLabel + "=" + images.SourceURL
	refs, err := runner.BuildTagged(ctx, tag, []string{source}, pkgs...)
	if err != nil {
		return err
	}
	dockerFlags := append(slices.Clone(e.Cfg.DockerBuildFlags), "--label="+source)
	envoy, err := images.PublishDockerfileImage(ctx, e.Cfg.Root, e.Cfg.KODockerRepo, envoyDataplaneImage, tag,
		e.Cfg.Path(envoyDataplaneDockefile), e.Cfg.KODefaultPlatforms, dockerFlags)
	if err != nil {
		return err
	}
	refs = append(refs, envoy)

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
