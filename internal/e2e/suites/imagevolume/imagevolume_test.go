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

// Package imagevolume exercises image volumes against a live cluster.
package imagevolume

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	atespace = "imagevolume"

	// mountPath must not collide with anything the probe's own image ships.
	mountPath = "/mnt/ate-image-volume"

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
	ref := fmt.Sprintf("%s/e2e-imagevolume-fixture:%d", strings.TrimSuffix(repo, "/"), time.Now().UnixNano())
	tag, err := name.ParseReference(ref, name.Insecure)
	if err != nil {
		t.Fatalf("parsing %q: %v", ref, err)
	}
	if err := remote.Write(tag, img); err != nil {
		t.Fatalf("pushing %q: %v", ref, err)
	}

	digest, err := img.Digest()
	if err != nil {
		t.Fatalf("computing digest: %v", err)
	}
	return fmt.Sprintf("%s@%s", tag.Context().Name(), digest)
}

// createTemplate builds a probe ActorTemplate with the fixture attached as an
// image volume, copying the resolved runtime from the shared probe template.
func createTemplate(ctx context.Context, t *testing.T, clients *e2e.Clients, ns *e2e.Namespace, fixtureImage string) *v1alpha1.ActorTemplate {
	t.Helper()

	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatalf("CheckEnv: %v", err)
	}

	// The probe supplies this suite's container image and resolved runtime.
	probeNamespace := e2e.DeployProbe(t, env["BUCKET_NAME"], "imagevolume")

	srcPool, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(probeNamespace).Get(ctx, probeName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting WorkerPool %s/%s: %v", probeNamespace, probeName, err)
	}
	srcTemplate, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(probeNamespace).Get(ctx, probeName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting ActorTemplate %s/%s: %v", probeNamespace, probeName, err)
	}

	// The pool is labeled uniquely to this namespace so the cluster-wide
	// scheduler cannot hand its workers to another suite's actors.
	poolLabels := map[string]string{"imagevolume": ns.Name}
	pool := &v1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: probeName, Namespace: ns.Name, Labels: poolLabels},
		Spec: v1alpha1.WorkerPoolSpec{
			Replicas:          2,
			AteomImage:        srcPool.Spec.AteomImage,
			SandboxClass:      srcPool.Spec.SandboxClass,
			SandboxConfigName: srcPool.Spec.SandboxConfigName,
		},
	}
	if _, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(ns.Name).Create(ctx, pool, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating WorkerPool: %v", err)
	}

	container := srcTemplate.Spec.Containers[0]
	container.VolumeMounts = append(container.VolumeMounts, v1alpha1.VolumeMount{Name: "fixture", MountPath: mountPath})

	volumes := append(slices.Clone(srcTemplate.Spec.Volumes), v1alpha1.Volume{
		Name:         "fixture",
		VolumeSource: v1alpha1.VolumeSource{Image: &v1alpha1.ImageVolumeSource{Reference: fixtureImage}},
	})

	at := &v1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: probeName, Namespace: ns.Name},
		Spec: v1alpha1.ActorTemplateSpec{
			Containers:     []v1alpha1.Container{container},
			WorkerSelector: &metav1.LabelSelector{MatchLabels: poolLabels},
			SandboxClass:   srcTemplate.Spec.SandboxClass,
			Volumes:        volumes,
			SnapshotsConfig: v1alpha1.SnapshotsConfig{
				Location: fmt.Sprintf("gs://%s/%s/", env["BUCKET_NAME"], ns.Name),
			},
		},
	}
	created, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(ns.Name).Create(ctx, at, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating ActorTemplate: %v", err)
	}

	e2e.WaitForTemplateReady(ctx, t, clients, ns.Name, probeName)
	return created
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

func TestImageVolume(t *testing.T) {
	repo := os.Getenv("KO_DOCKER_REPO")
	if repo == "" {
		t.Skip("KO_DOCKER_REPO is unset; it names the registry both this host and the cluster can reach")
	}

	ctx := context.Background()
	clients := e2e.GetClients()
	ns := e2e.CreateNamespace(t)

	fixtureImage := buildFixtureImage(t, repo)
	t.Logf("fixture image: %s", fixtureImage)
	createTemplate(ctx, t, clients, ns, fixtureImage)

	if _, err := clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: atespace}},
	}); err != nil {
		t.Logf("CreateAtespace (may already exist): %v", err)
	}

	actorRef := resources.ActorRef{Atespace: atespace, Name: "iv-" + ns.Name}
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
			ActorTemplateNamespace: ns.Name,
			ActorTemplateName:      probeName,
		},
	}); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapipb.SuspendActorRequest{Actor: actorRef.ToObjectRef()})
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: actorRef.ToObjectRef()})
	})

	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef.ToObjectRef()}); err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}

	router, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer router.Close()

	payloadPath := mountPath + "/" + payloadName

	t.Run("DeliversImageContents", func(t *testing.T) {
		got := probeJSON(ctx, t, router, actorRef, "/readfile?path="+payloadPath)
		if got["error"] != "" {
			t.Fatalf("reading %s: %s", payloadPath, got["error"])
		}
		if got["content"] != payloadContent {
			t.Errorf("content = %q, want %q", got["content"], payloadContent)
		}
	})

	t.Run("UpperLayerWins", func(t *testing.T) {
		got := probeJSON(ctx, t, router, actorRef, "/readfile?path="+mountPath+"/"+shadowedName)
		if got["error"] != "" {
			t.Fatalf("reading %s: %s", shadowedName, got["error"])
		}
		if got["content"] != shadowedContent {
			t.Errorf("content = %q, want %q from the upper layer", got["content"], shadowedContent)
		}
	})

	t.Run("WhiteoutHidesLowerLayerFile", func(t *testing.T) {
		got := probeJSON(ctx, t, router, actorRef, "/readfile?path="+mountPath+"/"+deletedName)
		if got["error"] == "" {
			t.Errorf("%s is readable (%q), want it hidden by the whiteout", deletedName, got["content"])
		}
	})

	t.Run("MountIsReadOnly", func(t *testing.T) {
		got := probeJSON(ctx, t, router, actorRef, "/writefile?path="+mountPath+"/should-not-exist")
		if got["error"] == "" {
			t.Errorf("write to the image volume succeeded, want it rejected as read-only")
		}
	})

	t.Run("SurvivesSuspendResume", func(t *testing.T) {
		if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef.ToObjectRef()}); err != nil {
			t.Fatalf("SuspendActor: %v", err)
		}

		// No explicit resume: routing to the actor is what wakes it.
		resumeCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		got := probeJSON(resumeCtx, t, router, actorRef, "/readfile?path="+payloadPath)
		if got["error"] != "" {
			t.Fatalf("reading %s after resume: %s", payloadPath, got["error"])
		}
		if got["content"] != payloadContent {
			t.Errorf("content after resume = %q, want %q", got["content"], payloadContent)
		}
	})
}
