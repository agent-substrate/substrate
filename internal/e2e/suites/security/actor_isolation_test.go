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

// This file covers the actor-level isolation boundary within a single
// atespace. Both the victim and attacker actors are created from the same
// ActorTemplate in the same atespace and take turns on a single-worker pool
// (fixtures/security/actor-isolation.yaml.tmpl)
package security

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/objectstore"
	"go.ate.dev/substrate/internal/portforward"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// templateName is the ActorTemplate name deployed by the security fixtures.
	templateName = "probe"

	// Actor names used by TestActorIsolation.
	victim    = "victim"
	attacker  = "attacker"
	cloneName = "cloned-actor"

	// dataMount is where the security template mounts the durable-dir volume,
	// and durableVolume is that volume's name. Keep both in sync with the manifest.
	dataMount     = "/data"
	durableVolume = "data"

	writtenByProbe = "written by probe"
	neverWritten   = dataMount + "/never-written"

	// Durable markers written inside /data by the victim and attacker actors
	// in the same atespace.
	victimDurableMarker   = dataMount + "/victim-marker"
	attackerDurableMarker = dataMount + "/attacker-marker"

	// Ephemeral markers written outside durable volumes by the victim and
	// attacker actors.
	//   - /tmp (ephemeral directory inside the container)
	//   - / (the container's writable rootfs upper layer)
	//   - /dev (the sandbox's tmpfs mount)
	victimTmpMarker      = "/tmp/victim-tmp-marker"
	attackerTmpMarker    = "/tmp/attacker-tmp-marker"
	victimRootfsMarker   = "/victim-rootfs-marker"
	attackerRootfsMarker = "/attacker-rootfs-marker"
	victimTmpfsMarker    = "/dev/victim-tmpfs-marker"
	attackerTmpfsMarker  = "/dev/attacker-tmpfs-marker"
)

// fileResponse mirrors the probe's /readfile and /writefile responses.
type fileResponse struct {
	Path    string `json:"path"`
	Content string `json:"content"`
	OK      string `json:"ok"`
	Error   string `json:"error"`
}

// whoamiResponse mirrors the probe's /whoami response.
type whoamiResponse struct {
	File     string `json:"file"`
	Atespace string `json:"atespace"`
	UID      string `json:"uid"`
	Held     string `json:"held"`
	Error    string `json:"error"`
}

// TestActorIsolation checks that actor-level isolation invariants hold across
// worker reuse and snapshot lifecycle.
func TestActorIsolation(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	atespace, _ := e2e.DeploySubstrateFixture(t, ctx, clients, e2e.SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/security/actor-isolation.yaml.tmpl",
		Template: "internal/e2e/fixtures/security/actor-isolation-template.yaml.tmpl",
	}, env["BUCKET_NAME"], "sec-actor", nil)

	rc, err := e2e.NewRouterClient(t, ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rc.Close()

	// Victim: runs first on the single shared worker. Record its UID and
	// worker.
	createAndResumeActor(t, ctx, clients, atespace, victim)
	victimActor := resources.ActorRef{Atespace: atespace, Name: victim}
	victimUID := actorUID(t, ctx, clients, atespace, victim)
	victimWorker := assignedWorker(t, ctx, clients, atespace, victim)

	// Populate durable and ephemeral state in the victim, and
	// verify each write with a positive read control before suspending.
	for _, path := range []string{
		victimDurableMarker,
		victimTmpMarker,
		victimRootfsMarker,
		victimTmpfsMarker,
	} {
		writeFile(t, ctx, rc, victimActor, path)
		if got := readFile(t, ctx, rc, victimActor, path); got.Content != writtenByProbe {
			t.Fatalf("control: victim %s reading its OWN %s = %q (error %q), want %q — the probe cannot see its own written state, so nothing below is meaningful",
				victimActor, path, got.Content, got.Error, writtenByProbe)
		}
	}
	victimWhoami := whoami(t, ctx, rc, victimActor)
	if victimWhoami.File != victim || victimWhoami.Held != victim || victimWhoami.Atespace != atespace || victimWhoami.UID != victimUID {
		t.Fatalf("control: victim %s /whoami = %+v, want file=%q held=%q atespace=%q uid=%q",
			victimActor, victimWhoami, victim, victim, atespace, victimUID)
	}

	// Suspend the victim to free the worker. The durable marker stays on the
	// worker; the snapshot goes to object storage. Record the snapshot URI for
	// post-delete bucket check.
	victimSnapshotURI := suspendDurably(t, ctx, clients, atespace, victim)

	// Attacker: a second actor in the SAME atespace takes the freed worker.
	createAndResumeActor(t, ctx, clients, atespace, attacker)
	attackerActor := resources.ActorRef{Atespace: atespace, Name: attacker}
	attackerUID := actorUID(t, ctx, clients, atespace, attacker)
	requireReusedWorker(t, ctx, clients, atespace, attacker, victimWorker)

	// Sanity checks before the negative assertions. First confirm that
	// /readfile reports a missing file as an error at all, and that the
	// attacker can write and read its own files.
	if got := readFile(t, ctx, rc, attackerActor, neverWritten); got.Error == "" {
		t.Fatalf("control: attacker %s read %s, which nothing ever created, and reported no error (content %q) — /readfile does not report misses as errors, so every negative assertion here would pass vacuously",
			attackerActor, neverWritten, got.Content)
	}
	for _, path := range []string{
		attackerDurableMarker,
		attackerTmpMarker,
		attackerRootfsMarker,
		attackerTmpfsMarker,
	} {
		writeFile(t, ctx, rc, attackerActor, path)
		if got := readFile(t, ctx, rc, attackerActor, path); got.Content != writtenByProbe {
			t.Fatalf("control: attacker %s reading its OWN %s = %q (error %q), want %q",
				attackerActor, path, got.Content, got.Error, writtenByProbe)
		}
	}

	// Baseline identity read on the attacker while RUNNING. This also indexes
	// the /run/ate inodes and exercises heldIdentity in the guest before
	// snapshotting the attacker.
	attackerWhoami := whoami(t, ctx, rc, attackerActor)
	if attackerWhoami.File != attacker || attackerWhoami.Held != attacker || attackerWhoami.Atespace != atespace || attackerWhoami.UID != attackerUID {
		t.Fatalf("control: attacker %s /whoami = %+v, want file=%q held=%q atespace=%q uid=%q",
			attackerActor, attackerWhoami, attacker, attacker, atespace, attackerUID)
	}

	// ST-1 [time], durable mount: the attacker's /data must be its own.
	// The victim's durable data left the worker in its snapshot, so seeing the marker here would mean durable dirs are
	// keyed by something shared, breaking the security invariant.
	t.Run("VictimDurableMarkerNotVisibleThroughAttackerMount", func(t *testing.T) {
		t.Log("Invariant ST-1 [time]: An actor must not read or write another actor's durable-dir volume through its own mount")
		if got := readFile(t, ctx, rc, attackerActor, victimDurableMarker); got.Error == "" {
			t.Errorf("attacker %s read %s, the victim's durable marker in the same atespace %s, through its own mount: content %q — durable-dir volumes shouldn't have been shared across worker-reuse",
				attackerActor, victimDurableMarker, atespace, got.Content)
		}
	})

	// EX-4 [time], an actor must not read/write the per-actor host state of another actor on the same worker.
	t.Run("VictimHostStateNotReachableFromAttackerSandbox", func(t *testing.T) {
		t.Log("Invariant EX-4 [time]: An actor must not read/write the per-actor host state of another actor on the same worker or worker-pod credentials (T-18/T-19/T-22/T-27)")
		for _, probe := range []struct {
			what string
			path string
		}{
			{"the victim's durable marker on the host", actorHostPath(victimUID, "durable-dir", durableVolume, filepath.Base(victimDurableMarker))},
			{"the victim's projected actor identity", actorHostPath(victimUID, "system-info", "system-info", "actor-id")},
			{"the victim's sandbox asset record", actorHostPath(victimUID, "sandbox-assets.json")},
			{"the worker pod's pod-identity credential bundle (T-19)", "/run/podidentity.podcert.ate.dev/credential-bundle.pem"},
			{"the worker pod's Kubernetes ServiceAccount token (T-22)", "/var/run/secrets/kubernetes.io/serviceaccount/token"},
		} {
			if got := readFile(t, ctx, rc, attackerActor, probe.path); got.Error == "" {
				t.Errorf("attacker %s read %s (%s): content %q — the sandbox exposes the worker's per-actor host state or worker credentials across the reuse boundary",
					attackerActor, probe.path, probe.what, got.Content)
			}
		}

		// A write is the more serious direction of the same boundary: reaching the
		// victim's durable directory means an actor can corrupt another actor's data.
		injected := actorHostPath(victimUID, "durable-dir", durableVolume, "injected")
		if got := writeFileAllowingFailure(t, ctx, rc, attackerActor, injected); got.Error == "" {
			t.Errorf("attacker %s wrote %s, inside the victim's durable directory on the worker — an actor can corrupt another actor's stored data",
				attackerActor, injected)
		}
	})

	// Ephemeral sandbox state: when the worker hosts the attacker
	// after the victim, the attacker must find none of the victim's ephemeral
	// state in /tmp, the writable rootfs layer (/), the sandbox tmpfs (/dev),
	// or open file descriptors.
	t.Run("VictimEphemeralStateNotPresentInAttackerSandbox", func(t *testing.T) {
		t.Log("Invariant EX-5 [time]: When a worker hosts a new actor, the new actor must find no state from the previous actor (/tmp, writable layer, tmpfs, file descriptors) (T-27)")
		for _, probe := range []struct {
			what string
			path string
		}{
			{"the victim's /tmp file", victimTmpMarker},
			{"the victim's writable rootfs upper-layer file", victimRootfsMarker},
			{"the victim's /dev tmpfs file", victimTmpfsMarker},
		} {
			if got := readFile(t, ctx, rc, attackerActor, probe.path); got.Error == "" {
				t.Errorf("attacker %s read %s (%s): content %q — ephemeral state from the previous actor survived worker reuse",
					attackerActor, probe.path, probe.what, got.Content)
			}
		}

		gotWhoami := whoami(t, ctx, rc, attackerActor)
		if gotWhoami.Held == victim || gotWhoami.File == victim || gotWhoami.UID == victimUID {
			t.Errorf("attacker %s /whoami = %+v, carries previous actor %s (uid %s) state across worker reuse",
				attackerActor, gotWhoami, victim, victimUID)
		}
	})

	// ST-3 [time]: projected identity must never be captured in
	// snapshots and leaked to a cloned actor in the same atespace.
	t.Run("ProjectedIdentityNotCapturedInSnapshot", func(t *testing.T) {
		t.Log("Invariant ST-3 [time]: Projected identity (system-info volume) must never be captured in snapshots (T-23)")
		testProjectedIdentityNotInSnapshot(t, ctx, clients, rc, atespace, attacker, attackerUID)
	})

	// ST-5 [time]: deleting the victim actor must remove its external snapshots
	// from object storage and prevent a newly created actor of the same name
	// from recovering the deleted actor's state.
	t.Run("DeletedActorSnapshotsRemoved", func(t *testing.T) {
		t.Log("Invariant ST-5 [time]: Deletion of an actor must remove its snapshots, so a later actor cannot recover them")
		testDeletedActorSnapshotsRemoved(t, ctx, clients, rc, atespace, victim, victimUID, victimSnapshotURI, victimWorker)
	})
}

// actorHostPath is nodepath.ActorsDir/<uid>/<elem...> on the worker host. The
// leaves (durable-dir, system-info, sandbox-assets.json) mirror
// cmd/atelet/internal/ateletpath, which this suite cannot import.
func actorHostPath(actorUID string, elem ...string) string {
	return filepath.Join(append([]string{nodepath.ActorsDir, actorUID}, elem...)...)
}

// testProjectedIdentityNotInSnapshot checks that an actor cloned from a
// snapshot tag in the same atespace observes its own projected system-info
// identity, never the snapshot source actor's identity.
func testProjectedIdentityNotInSnapshot(t *testing.T, ctx context.Context, clients *e2e.Clients, rc *e2e.RouterClient, atespace, sourceName, sourceUID string) {
	t.Helper()

	// The tag needs a durable snapshot to capture.
	suspendDurably(t, ctx, clients, atespace, sourceName)

	tagRef := &ateapipb.ObjectRef{Atespace: atespace, Name: "actor-snapshot-tag"}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.DeleteTag(context.Background(), &ateapipb.DeleteTagRequest{Tag: tagRef})
	})
	_, _ = clients.SubstrateAPI.DeleteTag(ctx, &ateapipb.DeleteTagRequest{Tag: tagRef})
	tag, err := clients.SubstrateAPI.CreateTag(ctx, &ateapipb.CreateTagRequest{
		Tag: &ateapipb.Tag{
			Metadata:    &ateapipb.ResourceMetadata{Atespace: atespace, Name: tagRef.GetName()},
			Scope:       ateapipb.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapipb.ObjectRef{Atespace: atespace, Name: sourceName},
		},
	})
	if err != nil {
		t.Fatalf("CreateTag %s/%s: %v", atespace, tagRef.GetName(), err)
	}
	if snapshotURI(tag.GetStatus().GetSnapshot()) == "" {
		t.Fatalf("tag %s/%s has no snapshot uri", atespace, tagRef.GetName())
	}

	cloneRef := &ateapipb.ObjectRef{Atespace: atespace, Name: cloneName}
	_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: cloneRef})
	_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: cloneRef})
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapipb.SuspendActorRequest{Actor: cloneRef})
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: cloneRef})
	})
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: cloneName},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: atespace, Name: templateName},
		SourceTag:     tagRef,
	}}); err != nil {
		t.Fatalf("CreateActor %s/%s from tag %s: %v", atespace, cloneName, tagRef.GetName(), err)
	}
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: cloneRef}); err != nil {
		t.Fatalf("ResumeActor %s/%s: %v", atespace, cloneName, err)
	}
	waitForActorState(t, ctx, clients, atespace, cloneName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	// Positive control: the clone carries the snapshot source's durable data,
	// proving it actually restored from the source actor's checkpoint.
	cloneActor := resources.ActorRef{Atespace: atespace, Name: cloneName}
	if got := readFile(t, ctx, rc, cloneActor, attackerDurableMarker); got.Content != writtenByProbe {
		t.Fatalf("control: clone %s restored from tag %s reads %s = %q (error %q), want %q — the clone did not restore the source snapshot",
			cloneActor, tagRef.GetName(), attackerDurableMarker, got.Content, got.Error, writtenByProbe)
	}

	cloneUID := actorUID(t, ctx, clients, atespace, cloneName)
	if cloneUID == sourceUID {
		t.Fatalf("clone %s/%s was assigned the same UID %q as source %s/%s", atespace, cloneName, cloneUID, atespace, sourceName)
	}

	got := whoami(t, ctx, rc, cloneActor)
	if got.File == sourceName {
		t.Errorf("clone %s/%s /run/ate/actor-id = %q (the snapshot source's name) — projected identity was captured in the snapshot",
			atespace, cloneName, got.File)
	} else if got.File != cloneName {
		t.Errorf("clone %s/%s /run/ate/actor-id = %q, want %q (probe error: %q)",
			atespace, cloneName, got.File, cloneName, got.Error)
	}

	if got.Held == sourceName {
		t.Errorf("clone %s/%s startup-held fd for /run/ate/actor-id read %q (the snapshot source's name) — open system-info fd did not re-bind on restore",
			atespace, cloneName, got.Held)
	} else if got.Held != cloneName {
		t.Errorf("clone %s/%s startup-held fd for /run/ate/actor-id = %q, want %q (probe error: %q)",
			atespace, cloneName, got.Held, cloneName, got.Error)
	}

	if got.UID == sourceUID {
		t.Errorf("clone %s/%s /run/ate/actor-uid = %q (the snapshot source's UID) — projected UID was captured in the snapshot",
			atespace, cloneName, got.UID)
	} else if got.UID != cloneUID {
		t.Errorf("clone %s/%s /run/ate/actor-uid = %q, want %q (probe error: %q)",
			atespace, cloneName, got.UID, cloneUID, got.Error)
	}

	if got.Atespace != atespace {
		t.Errorf("clone %s/%s /run/ate/atespace = %q, want %q (probe error: %q)",
			atespace, cloneName, got.Atespace, atespace, got.Error)
	}
}

// testDeletedActorSnapshotsRemoved checks that deleting an actor removes all
// objects under its owner prefix in object storage, and a newly created actor
// with the same name starts fresh without recovering the deleted actor's
// snapshot or durable/ephemeral data.
func testDeletedActorSnapshotsRemoved(t *testing.T, ctx context.Context, clients *e2e.Clients, rc *e2e.RouterClient, atespace, victim, victimUID, victimSnapshotURI, victimWorker string) {
	t.Helper()

	uri, err := resources.ParseSnapshotURI(victimSnapshotURI)
	if err != nil {
		t.Fatalf("ParseSnapshotURI(%q): %v", victimSnapshotURI, err)
	}
	bucket, prefix, err := objectstore.BucketPrefix(uri.OwnerPrefix())
	if err != nil {
		t.Fatalf("BucketPrefix(%s): %v", uri.OwnerPrefix(), err)
	}

	store := openSnapshotObjectStore(t, ctx, clients)

	// Positive control: before DeleteActor, the suspended victim's snapshot
	// objects must be visible at bucket/prefix so the post-delete empty check
	// below cannot pass vacuously on a miscalculated prefix.
	before, err := store.List(ctx, bucket, prefix)
	if err != nil {
		t.Fatalf("listing victim snapshot prefix %s/%s before DeleteActor: %v", bucket, prefix, err)
	}
	if len(before) == 0 {
		t.Fatalf("control: victim %s/%s (uid %s) has 0 objects under %s/%s before DeleteActor (snapshotUri %q) — post-delete empty check would be vacuous",
			atespace, victim, victimUID, bucket, prefix, victimSnapshotURI)
	}

	victimRef := &ateapipb.ObjectRef{Atespace: atespace, Name: victim}
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: victimRef}); err != nil {
		t.Fatalf("DeleteActor %s/%s: %v", atespace, victim, err)
	}

	// Negative assertion 1: the deleted actor's object-storage prefix is empty.
	after, err := store.List(ctx, bucket, prefix)
	if err != nil {
		t.Fatalf("listing victim snapshot prefix %s/%s after DeleteActor: %v", bucket, prefix, err)
	}
	if len(after) != 0 {
		t.Errorf("deleted actor %s/%s (uid %s) still has %d snapshot objects under %s/%s: %v",
			atespace, victim, victimUID, len(after), bucket, prefix, after)
	}

	// Negative assertion 2: a new actor created in the same atespace with the
	// exact same name starts with a new UID and cannot recover the deleted
	// actor's durable or ephemeral markers. Free the single shared worker first.
	for _, active := range []string{attacker, cloneName} {
		_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: active},
		})
	}
	createAndResumeActor(t, ctx, clients, atespace, victim)
	requireReusedWorker(t, ctx, clients, atespace, victim, victimWorker)

	reincarnatedUID := actorUID(t, ctx, clients, atespace, victim)
	if reincarnatedUID == victimUID {
		t.Errorf("recreated actor %s/%s reused deleted actor's UID %q", atespace, victim, victimUID)
	}
	victimActor := resources.ActorRef{Atespace: atespace, Name: victim}
	for _, marker := range []string{
		victimDurableMarker,
		victimTmpMarker,
		victimRootfsMarker,
		victimTmpfsMarker,
	} {
		if got := readFile(t, ctx, rc, victimActor, marker); got.Error == "" {
			t.Errorf("recreated actor %s/%s (uid %s) read deleted actor's marker %s = %q — state survived actor deletion",
				atespace, victim, reincarnatedUID, marker, got.Content)
		}
	}
}

// openSnapshotObjectStore connects to the cluster's snapshot object store:
// port-forwarding to the in-cluster rustfs S3 service on kind, or using the
// default GCS client on GKE.
func openSnapshotObjectStore(t *testing.T, ctx context.Context, clients *e2e.Clients) objectstore.Store {
	t.Helper()
	sysNS := e2e.SystemNamespace()
	rustfsSvc := e2e.ResourceName("rustfs")
	if _, err := clients.K8s.CoreV1().Services(sysNS).Get(ctx, rustfsSvc, metav1.GetOptions{}); err == nil {
		config, err := ateclient.LoadKubeConfig(e2e.KubeConfig, e2e.KubeContext)
		if err != nil {
			t.Fatalf("loading kubeconfig for rustfs port-forward: %v", err)
		}
		localPort, stop, err := portforward.ServicePortForward(ctx, config, clients.K8s, sysNS, rustfsSvc, 9000)
		if err != nil {
			t.Fatalf("port-forwarding to %s/%s:9000: %v", sysNS, rustfsSvc, err)
		}
		t.Cleanup(stop)
		return objectstore.NewS3(s3.New(s3.Options{
			Region:       "us-east-1",
			Credentials:  credentials.NewStaticCredentialsProvider("rustfsadmin", "rustfsadmin", ""),
			BaseEndpoint: aws.String(fmt.Sprintf("http://127.0.0.1:%d", localPort)),
			UsePathStyle: true,
		}))
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("checking for %s/%s service: %v", sysNS, rustfsSvc, err)
	}

	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatalf("creating GCS client: %v", err)
	}
	t.Cleanup(func() { _ = gcsClient.Close() })
	return objectstore.NewGCS(gcsClient)
}

// requireReusedWorker fails unless the actor landed on priorWorker. The fixture
// runs one worker, so any other worker means the attacker never touched the
// host the victim left its state on and the test proves nothing.
func requireReusedWorker(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name, priorWorker string) {
	t.Helper()
	got := assignedWorker(t, ctx, clients, atespace, name)
	if got != priorWorker {
		t.Fatalf("actor %s/%s landed on worker %q, not the worker %q the victim vacated; the fixture deploys one worker, so the reuse boundary under test is not being exercised",
			atespace, name, got, priorWorker)
	}
	t.Logf("actor %s/%s reused worker %s", atespace, name, priorWorker)
}

func assignedWorker(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name string) string {
	t.Helper()
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor %s/%s: %v", atespace, name, err)
	}
	worker := actor.GetStatus().GetWorkerAssignment().GetWorker().GetName()
	if worker == "" {
		t.Fatalf("actor %s/%s has no worker assignment while RUNNING", atespace, name)
	}
	return worker
}

func actorUID(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name string) string {
	t.Helper()
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor %s/%s: %v", atespace, name, err)
	}
	uid := actor.GetMetadata().GetUid()
	if uid == "" {
		t.Fatalf("actor %s/%s has no uid", atespace, name)
	}
	return uid
}

// createAndResumeActor creates an actor from its atespace's template and resumes
// it from the golden snapshot, removing it when the test ends.
func createAndResumeActor(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name string) {
	t.Helper()
	ref := &ateapipb.ObjectRef{Atespace: atespace, Name: name}
	// Actor records outlive the fixture namespace, so an interrupted run can
	// leave one behind and wedge reruns on AlreadyExists. DeleteActor needs
	// SUSPENDED or CRASHED, hence the suspend; both are best-effort.
	_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
	_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref})
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: atespace, Name: templateName},
	}}); err != nil {
		t.Fatalf("CreateActor %s/%s: %v", atespace, name, err)
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapipb.SuspendActorRequest{Actor: ref})
		// NotFound is normal: a subtest may already have deleted the actor.
		if _, err := clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: ref}); err != nil && status.Code(err) != codes.NotFound {
			t.Logf("cleanup: DeleteActor %s/%s failed, actor leaked (remove with: kubectl ate delete actor %s -a %s): %v", atespace, name, name, atespace, err)
		}
	})

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor %s/%s: %v", atespace, name, err)
	}
	waitForActorState(t, ctx, clients, atespace, name, ateapipb.ActorState_ACTOR_STATE_RUNNING)
}

func waitForActorState(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name string, want ateapipb.ActorState) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: name},
		})
		if err == nil && resp.GetStatus().GetState() == want {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("timed out waiting for actor %s/%s to reach state %v", atespace, name, want)
}

// writeFile writes the probe's fixed body at path and fails the test if the
// write did not land, so a later negative assertion cannot pass on missing
// state.
func writeFile(t *testing.T, ctx context.Context, rc *e2e.RouterClient, actor resources.ActorRef, path string) {
	t.Helper()
	if got := writeFileAllowingFailure(t, ctx, rc, actor, path); got.OK != "true" {
		t.Fatalf("actor %s writing %s: %q", actor, path, got.Error)
	}
}

// writeFileAllowingFailure is writeFile for assertions that expect the write
// to be refused; it returns the probe's report instead of failing.
func writeFileAllowingFailure(t *testing.T, ctx context.Context, rc *e2e.RouterClient, actor resources.ActorRef, path string) fileResponse {
	t.Helper()
	return probeFileOp(t, ctx, rc, actor, "/writefile", path)
}

func readFile(t *testing.T, ctx context.Context, rc *e2e.RouterClient, actor resources.ActorRef, path string) fileResponse {
	t.Helper()
	return probeFileOp(t, ctx, rc, actor, "/readfile", path)
}

// probeFileOp calls one of the probe's file endpoints. A transport or HTTP
// failure is fatal rather than returned: the probe never ran the operation,
// which is not the same as the actor being denied.
func probeFileOp(t *testing.T, ctx context.Context, rc *e2e.RouterClient, actor resources.ActorRef, endpoint, path string) fileResponse {
	t.Helper()
	resp, err := rc.Get(ctx, actor, endpoint+"?path="+url.QueryEscape(path))
	if err != nil {
		t.Fatalf("GET %s for %s: %v", endpoint, actor, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s for %s: status %d, body %q", endpoint, actor, resp.StatusCode, body)
	}
	var out fileResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding %s for %s: %v", endpoint, actor, err)
	}
	return out
}

// whoami calls the probe's /whoami endpoint, which reads /run/ate/actor-id,
// /run/ate/atespace, /run/ate/actor-uid, and the startup-held open file
// descriptor to /run/ate/actor-id.
func whoami(t *testing.T, ctx context.Context, rc *e2e.RouterClient, actor resources.ActorRef) whoamiResponse {
	t.Helper()
	resp, err := rc.Get(ctx, actor, "/whoami")
	if err != nil {
		t.Fatalf("GET /whoami for %s: %v", actor, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET /whoami for %s: status %d, body %q", actor, resp.StatusCode, body)
	}
	var out whoamiResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding /whoami for %s: %v", actor, err)
	}
	return out
}

// suspendDurably suspends the actor, waits for it to be SUSPENDED, and returns
// the object-storage URI of its own durable snapshot. The default suspend
// mode returns once the snapshot is captured on the node and uploads in the
// background, so this asks for a synchronous suspend and still polls: an
// actor that was already suspended is returned with whatever snapshot it
// holds, which may not have been uploaded yet.
func suspendDurably(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name string) string {
	t.Helper()
	ref := &ateapipb.ObjectRef{Atespace: atespace, Name: name}
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor:   ref,
		Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	}); err != nil {
		t.Fatalf("SuspendActor %s/%s: %v", atespace, name, err)
	}
	waitForActorState(t, ctx, clients, atespace, name, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	deadline := time.Now().Add(2 * time.Minute)
	for {
		actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
		if err != nil {
			t.Fatalf("GetActor %s/%s: %v", atespace, name, err)
		}
		if uri := durableSnapshotURI(actor.GetStatus()); uri != "" {
			return uri
		}
		if time.Now().After(deadline) {
			t.Fatalf("suspended actor %s/%s has no completed durable snapshot of its own after two minutes: %v",
				atespace, name, actor.GetStatus().GetSnapshots())
		}
		time.Sleep(time.Second)
	}
}

// durableSnapshotURI returns the object-storage URI of the newest snapshot
// in status that the actor itself owns and that has finished uploading, or ""
// if none has. A suspended actor always has one. status.snapshots also lists
// tag-owned snapshots (the golden snapshot the actor was restored from), which
// live under the tag's prefix, not the actor's, so they are skipped.
func durableSnapshotURI(status *ateapipb.ActorStatus) string {
	var newest *ateapipb.Snapshot
	for _, snap := range status.GetSnapshots() {
		if snap.GetOwner() != ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR {
			continue
		}
		if uri := snapshotURI(snap); uri != "" {
			if newest == nil || snap.GetGeneration() > newest.GetGeneration() {
				newest = snap
			}
		}
	}
	return snapshotURI(newest)
}

func snapshotURI(snap *ateapipb.Snapshot) string {
	st := snap.GetDurableSnapshot()
	if st.GetStatus() == ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED {
		return st.GetObject().GetSnapshotUri()
	}
	return ""
}
