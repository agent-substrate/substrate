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

// Package combinedvolumes exercises the three volume sources -- image volumes,
// durable dirs, and external volumes -- mounted together on a single actor,
// along with the mount semantics they share, against a live cluster.
package combinedvolumes

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	atespace = "combinedvolumes"

	// mountPath must not collide with anything the probe's own image ships.
	mountPath = "/mnt/ate-image-volume"
	// mountPathAlias is a second mount of the same image volume: one volume
	// may be mounted at multiple paths.
	mountPathAlias = "/mnt/ate-image-volume-alias"

	// The scratch durable-dir volume is mounted at both of these paths; a
	// write through one must be readable through the other.
	scratchPathA = "/mnt/ate-scratch-a"
	scratchPathB = "/mnt/ate-scratch-b"

	// The external (CSI-backed) volume is mounted at both of these paths.
	// Unlike the durable dir it is provisioned per actor and detached on
	// suspend, so it must come back attached once and visible at both.
	extVolume   = "external"
	extPathA    = "/mnt/ate-external-a"
	extPathB    = "/mnt/ate-external-b"
	extCapacity = "1Gi"

	// probeWrittenContent is the fixed string the probe's /writefile writes.
	probeWrittenContent = "written by probe"

	payloadName    = "payload.txt"
	payloadContent = "delivered by an image volume"
	// shadowedName is written by two layers; the upper one must win.
	shadowedName    = "shadowed.txt"
	shadowedContent = "from the middle layer"
	// deletedName is shipped by the bottom layer and whited out by the top.
	deletedName = "deleted.txt"
)

const probeName = e2e.ProbeName

// tarLayer builds a layer from a set of paths to contents. A path whose base
// name starts with ".wh." is an OCI whiteout for the same-named lower path.
func tarLayer(t *testing.T, files map[string]string) v1.Layer {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, body := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o444, Size: int64(len(body))}); err != nil {
			t.Fatalf("writing tar header for %q: %v", name, err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatalf("writing tar body for %q: %v", name, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("closing tar: %v", err)
	}

	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(buf.Bytes())), nil
	})
	if err != nil {
		t.Fatalf("building layer: %v", err)
	}
	return layer
}

// buildFixtureImage pushes a three-layer image and returns its digest-pinned
// reference.
func buildFixtureImage(t *testing.T, repo string) string {
	t.Helper()

	img, err := mutate.AppendLayers(empty.Image,
		tarLayer(t, map[string]string{
			payloadName:  "from-the-bottom-layer",
			shadowedName: "from-the-bottom-layer",
			deletedName:  "should-not-survive",
		}),
		tarLayer(t, map[string]string{shadowedName: shadowedContent}),
		tarLayer(t, map[string]string{
			payloadName:          payloadContent,
			".wh." + deletedName: "",
		}),
	)
	if err != nil {
		t.Fatalf("appending layers: %v", err)
	}

	// A unique tag per run: some registries refuse to overwrite an existing
	// tag. The returned reference is digest-pinned, so the tag itself is
	// throwaway.
	ref := fmt.Sprintf("%s/e2e-combinedvolumes-fixture:%d", strings.TrimSuffix(repo, "/"), time.Now().UnixNano())
	tag, err := name.ParseReference(ref, name.Insecure)
	if err != nil {
		t.Fatalf("parsing %q: %v", ref, err)
	}
	// The default keychain reads the local docker config, so the push works
	// against an authenticated registry (a GKE dev cluster's gcr.io) as well
	// as CI's anonymous kind registry.
	if err := remote.Write(tag, img, remote.WithAuthFromKeychain(authn.DefaultKeychain)); err != nil {
		t.Fatalf("pushing %q: %v", ref, err)
	}

	digest, err := img.Digest()
	if err != nil {
		t.Fatalf("computing digest: %v", err)
	}
	return fmt.Sprintf("%s@%s", tag.Context().Name(), digest)
}

// storageClassOrEmpty returns the configured StorageClass if the cluster has
// one, and "" if it does not. The CSI driver is optional, so a missing class
// drops the external volume from the template instead of failing every test
// in the suite.
func storageClassOrEmpty(ctx context.Context, t *testing.T, clients *e2e.Clients) string {
	t.Helper()

	if _, err := clients.K8s.StorageV1().StorageClasses().Get(ctx, e2e.StorageClass, metav1.GetOptions{}); err != nil {
		t.Logf("StorageClass %q not found (%v); the external-volume case will be skipped", e2e.StorageClass, err)
		return ""
	}
	return e2e.StorageClass
}

// createTemplate builds a probe ActorTemplate with the fixture attached as an
// image volume, copying the resolved runtime from the shared probe template.
// The template's name is suffixed per test run: it lives in the suite's
// shared atespace, which outlives the per-test k8s namespace. A non-empty
// storageClass adds the external volume.
func createTemplate(ctx context.Context, t *testing.T, clients *e2e.Clients, ns *e2e.Namespace, fixtureImage, storageClass string) *ateapipb.ActorTemplate {
	t.Helper()

	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatalf("CheckEnv: %v", err)
	}

	// The probe supplies this suite's container image and resolved runtime.
	probeAtespace, _ := e2e.DeployProbe(t, env["BUCKET_NAME"], "combinedvolumes")
	src := e2e.SubstrateFixture{
		Atespace:      probeAtespace,
		Name:          probeName,
		PoolNamespace: probeAtespace,
		PoolName:      probeName,
		DeployWith:    "the combinedvolumes suite's own DeployProbe",
	}

	return e2e.CreateSubstrateTemplateFrom(ctx, t, clients, ns.Name, src, e2e.SubstrateTemplateOptions{
		Atespace:     atespace,
		Name:         "probe-" + ns.Name,
		PoolName:     probeName,
		PoolReplicas: 2,
		// The pool is labeled uniquely to this namespace so the cluster-wide
		// scheduler cannot hand its workers to another suite's actors.
		Labels: map[string]string{"combinedvolumes": ns.Name},
		SnapshotsConfig: &ateapipb.SnapshotsConfig{
			StorageLocation: fmt.Sprintf("gs://%s/%s/", env["BUCKET_NAME"], ns.Name),
		},
		Modify: func(tmpl *ateapipb.ActorTemplate) {
			// Every volume is mounted at two paths, covering the read-only
			// (image), writable (durable-dir) and per-actor provisioned
			// (external) multi-path cases.
			tmpl.Containers[0].VolumeMounts = append(tmpl.Containers[0].VolumeMounts,
				&ateapipb.VolumeMount{Name: "fixture", MountPath: mountPath},
				&ateapipb.VolumeMount{Name: "fixture", MountPath: mountPathAlias},
				&ateapipb.VolumeMount{Name: "scratch", MountPath: scratchPathA},
				&ateapipb.VolumeMount{Name: "scratch", MountPath: scratchPathB})
			tmpl.Volumes = append(tmpl.Volumes,
				&ateapipb.Volume{
					Name:  "fixture",
					Image: &ateapipb.ImageVolumeSource{Reference: fixtureImage},
				},
				&ateapipb.Volume{
					Name:       "scratch",
					DurableDir: &ateapipb.DurableDirVolumeSource{},
				})
			if storageClass == "" {
				return
			}
			tmpl.Containers[0].VolumeMounts = append(tmpl.Containers[0].VolumeMounts,
				&ateapipb.VolumeMount{Name: extVolume, MountPath: extPathA},
				&ateapipb.VolumeMount{Name: extVolume, MountPath: extPathB})
			tmpl.Volumes = append(tmpl.Volumes, &ateapipb.Volume{
				Name: extVolume,
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					Capacity:         extCapacity,
					StorageClassName: storageClass,
				},
			})
		},
	})
}

// probeJSON calls a probe endpoint through the router and decodes its reply.
func probeJSON(ctx context.Context, t *testing.T, router *e2e.RouterClient, actorRef resources.ActorRef, path string) map[string]string {
	t.Helper()

	resp, err := router.Get(ctx, actorRef, path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s: status %d: %s", path, resp.StatusCode, body)
	}

	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding %s: %v", path, err)
	}
	return out
}

// requireContent fails unless path holds want. Used to verify a mount
// delivers what the volume behind it should.
func requireContent(ctx context.Context, t *testing.T, router *e2e.RouterClient, actorRef resources.ActorRef, path, want string) {
	t.Helper()

	got := probeJSON(ctx, t, router, actorRef, "/readfile?path="+path)
	if got["error"] != "" {
		t.Fatalf("reading %s: %s", path, got["error"])
	}
	if got["content"] != want {
		t.Errorf("content at %s = %q, want %q", path, got["content"], want)
	}
}

// requireContentAtBoth fails unless both paths hold want, so both mounts must
// be live and backed by the same volume.
func requireContentAtBoth(ctx context.Context, t *testing.T, router *e2e.RouterClient, actorRef resources.ActorRef, pathA, pathB, want string) {
	t.Helper()

	requireContent(ctx, t, router, actorRef, pathA, want)
	requireContent(ctx, t, router, actorRef, pathB, want)
}

// requireUnreadable fails if path can be read.
func requireUnreadable(ctx context.Context, t *testing.T, router *e2e.RouterClient, actorRef resources.ActorRef, path string) {
	t.Helper()

	if got := probeJSON(ctx, t, router, actorRef, "/readfile?path="+path); got["error"] == "" {
		t.Errorf("%s is readable (%q), want it hidden", path, got["content"])
	}
}

// requireWriteRejected fails if path can be written.
func requireWriteRejected(ctx context.Context, t *testing.T, router *e2e.RouterClient, actorRef resources.ActorRef, path string) {
	t.Helper()

	if got := probeJSON(ctx, t, router, actorRef, "/writefile?path="+path); got["error"] == "" {
		t.Errorf("write to %s succeeded, want it rejected as read-only", path)
	}
}

// requireSharedWrite writes through writePath and requires the content at both
// paths: writePath confirms the write persisted, aliasPath that the two mounts
// reach the same volume.
func requireSharedWrite(ctx context.Context, t *testing.T, router *e2e.RouterClient, actorRef resources.ActorRef, writePath, aliasPath string) {
	t.Helper()

	if got := probeJSON(ctx, t, router, actorRef, "/writefile?path="+writePath); got["error"] != "" {
		t.Fatalf("writing %s: %s", writePath, got["error"])
	}
	requireContentAtBoth(ctx, t, router, actorRef, writePath, aliasPath, probeWrittenContent)
}

func TestCombinedVolumes(t *testing.T) {
	repo := os.Getenv("KO_DOCKER_REPO")
	if repo == "" {
		t.Skip("KO_DOCKER_REPO is unset; it names the registry both this host and the cluster can reach")
	}

	ctx := context.Background()
	clients := e2e.GetClients()
	ns := e2e.CreateNamespace(t)

	fixtureImage := buildFixtureImage(t, repo)
	t.Logf("fixture image: %s", fixtureImage)
	storageClass := storageClassOrEmpty(ctx, t, clients)
	tmpl := createTemplate(ctx, t, clients, ns, fixtureImage, storageClass)

	actorRef := resources.ActorRef{Atespace: atespace, Name: "cv-" + ns.Name}
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
			ActorTemplate: e2e.TemplateRef(tmpl),
		},
	}); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapipb.SuspendActorRequest{Actor: actorRef.ToObjectRef()})
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: actorRef.ToObjectRef()})
	})

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: actorRef.ToObjectRef()}); err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}

	router, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer router.Close()

	payloadPath := mountPath + "/" + payloadName

	t.Run("DeliversImageContents", func(t *testing.T) {
		requireContent(ctx, t, router, actorRef, payloadPath, payloadContent)
	})

	t.Run("UpperLayerWins", func(t *testing.T) {
		requireContent(ctx, t, router, actorRef, mountPath+"/"+shadowedName, shadowedContent)
	})

	t.Run("WhiteoutHidesLowerLayerFile", func(t *testing.T) {
		requireUnreadable(ctx, t, router, actorRef, mountPath+"/"+deletedName)
	})

	t.Run("MountIsReadOnly", func(t *testing.T) {
		requireWriteRejected(ctx, t, router, actorRef, mountPath+"/should-not-exist")
	})

	t.Run("SameImageVolumeAtTwoPaths", func(t *testing.T) {
		requireContentAtBoth(ctx, t, router, actorRef, payloadPath, mountPathAlias+"/"+payloadName, payloadContent)
	})

	t.Run("SameDurableVolumeAtTwoPathsSharesWrites", func(t *testing.T) {
		requireSharedWrite(ctx, t, router, actorRef, scratchPathA+"/multi.txt", scratchPathB+"/multi.txt")
	})

	t.Run("SameExternalVolumeAtTwoPathsSharesWrites", func(t *testing.T) {
		if storageClass == "" {
			t.Skipf("StorageClass %q is not installed", e2e.StorageClass)
		}
		requireSharedWrite(ctx, t, router, actorRef, extPathA+"/multi.txt", extPathB+"/multi.txt")
	})

	t.Run("SurvivesSuspendResume", func(t *testing.T) {
		if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef.ToObjectRef()}); err != nil {
			t.Fatalf("SuspendActor: %v", err)
		}

		// No explicit resume: routing to the actor is what wakes it. Restore
		// must re-establish all mounts and preserve shared writes across them.
		resumeCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		requireContentAtBoth(resumeCtx, t, router, actorRef, payloadPath, mountPathAlias+"/"+payloadName, payloadContent)
		requireContentAtBoth(resumeCtx, t, router, actorRef, scratchPathA+"/multi.txt", scratchPathB+"/multi.txt", probeWrittenContent)

		// Suspend detached the external volume; the resume must reattach it
		// once and restore both of its mounts.
		t.Run("ExternalVolumeReattached", func(t *testing.T) {
			if storageClass == "" {
				t.Skipf("StorageClass %q is not installed", e2e.StorageClass)
			}
			requireContentAtBoth(resumeCtx, t, router, actorRef, extPathA+"/multi.txt", extPathB+"/multi.txt", probeWrittenContent)
		})
	})
}
