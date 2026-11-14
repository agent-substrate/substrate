// Copyright 2026 The Agent Substrate Authors
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

package controlapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/scheduling"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/cmd/ateapi/internal/workercache"
	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/tools/cache"
)

type benchAnnotationProfile struct {
	name string
	// size is the combined length of all keys and values.
	size        int
	annotations map[string]string
}

// The profiles pin the actors row to a PostgreSQL storage regime. On top of
// the ~1.4 KiB status createBenchActor adds, a row above the 2 KiB TOAST
// threshold is compressed, and moved to the TOAST table in 1996-byte chunks
// if it is still above the threshold. Random values do not compress, so the
// row goes out of line as is; text values (SPIFFE IDs, URLs, JSON, prose with
// random identifiers) compress about 2:1, so the row goes out of line
// compressed. Repeated characters would compress to almost nothing and keep
// the row inline, which is not what real annotations do.
var benchAnnotationProfiles = []benchAnnotationProfile{
	{name: "0B"},
	randomBenchProfile("4KiB-1entry", 1, 4096),
	randomBenchProfile("4KiB", 16, 4096),
	textDocumentBenchProfile("4KiB-text-1entry", 4096),
	textBenchProfile("4KiB-text", 16, 4096),
}

// benchKeys returns n annotation keys, and the byte budget of each entry so
// that the entries total totalBytes.
func benchKeys(n, totalBytes int) ([]string, []int) {
	keys := make([]string, n)
	budgets := make([]int, n)
	remaining := totalBytes
	for i := range n {
		keys[i] = fmt.Sprintf("example.com/key-%02d", i)
		budgets[i] = remaining / (n - i)
		remaining -= budgets[i]
	}
	return keys, budgets
}

// randomBenchProfile builds n entries of base64-encoded random bytes.
func randomBenchProfile(name string, n, totalBytes int) benchAnnotationProfile {
	rng := newBenchText(name).src
	keys, budgets := benchKeys(n, totalBytes)
	out := make(map[string]string, n)
	for i, k := range keys {
		raw := make([]byte, budgets[i]-len(k))
		_, _ = rng.Read(raw)
		out[k] = base64.RawStdEncoding.EncodeToString(raw)[:budgets[i]-len(k)]
	}
	return benchAnnotationProfile{name: name, size: totalBytes, annotations: out}
}

// textBenchProfile builds n entries of mixed SPIFFE IDs, URLs, small JSON and
// prose.
func textBenchProfile(name string, n, totalBytes int) benchAnnotationProfile {
	text := newBenchText(name)
	keys, budgets := benchKeys(n, totalBytes)
	out := make(map[string]string, n)
	for i, k := range keys {
		var sb strings.Builder
		for j := 0; sb.Len() < budgets[i]-len(k); j++ {
			sb.WriteString(text.fragment(i + j))
			sb.WriteByte(' ')
		}
		out[k] = sb.String()[:budgets[i]-len(k)]
	}
	return benchAnnotationProfile{name: name, size: totalBytes, annotations: out}
}

// textDocumentBenchProfile builds one entry holding a JSON delegation
// document, cut to size.
func textDocumentBenchProfile(name string, totalBytes int) benchAnnotationProfile {
	text := newBenchText(name)
	keys, budgets := benchKeys(1, totalBytes)
	var sb strings.Builder
	fmt.Fprintf(&sb, `{"iss":"%s","sub":"%s","jti":"%s","delegations":[`, text.spiffeID(), text.spiffeID(), text.hex(16))
	for sb.Len() < budgets[0]-len(keys[0]) {
		fmt.Fprintf(&sb, `{"actor":"%s","scope":"%s %s","trace":"%s","nonce":"%s"},`, text.spiffeID(), text.word(), text.word(), text.url(), text.hex(8))
	}
	return benchAnnotationProfile{
		name:        name,
		size:        totalBytes,
		annotations: map[string]string{keys[0]: sb.String()[:budgets[0]-len(keys[0])]},
	}
}

// benchText generates text with random identifiers and word choices from a
// seed, so profiles are stable across runs and compress like real text.
type benchText struct {
	src   *rand.ChaCha8
	rng   *rand.Rand
	words []string
}

func newBenchText(seed string) benchText {
	var key [32]byte
	copy(key[:], seed)
	src := rand.NewChaCha8(key)
	return benchText{
		src:   src,
		rng:   rand.New(src),
		words: strings.Fields("agent release pipeline summary incident status draft nightly review queue ticket owner budget model prompt tool search code memory session token quota region cluster worker node snapshot restore volume policy egress ingress audit trace span retry limit window batch stream schema field value table index cache lease version schedule alert pager handoff rollout canary stable preview flag"),
	}
}

func (t benchText) word() string { return t.words[t.rng.IntN(len(t.words))] }

func (t benchText) hex(n int) string {
	raw := make([]byte, n)
	_, _ = t.src.Read(raw)
	return fmt.Sprintf("%x", raw)
}

func (t benchText) spiffeID() string {
	return fmt.Sprintf("spiffe://example.com/ns/%s-%s/sa/%s-%s", t.word(), t.word(), t.word(), t.hex(4))
}

func (t benchText) url() string {
	return fmt.Sprintf("https://%s.example.com/v1/%s/%s?span=%s", t.word(), t.word(), t.hex(16), t.hex(8))
}

func (t benchText) fragment(i int) string {
	switch i % 4 {
	case 0:
		return t.spiffeID()
	case 1:
		return t.url()
	case 2:
		return fmt.Sprintf(`{"model":"%s-%s","temperature":0.%d,"tools":["%s","%s"],"max_turns":%d}`, t.word(), t.hex(2), t.rng.IntN(10), t.word(), t.word(), t.rng.IntN(100))
	default:
		parts := make([]string, 12)
		for i := range parts {
			parts[i] = t.word()
		}
		return strings.Join(parts, " ") + "."
	}
}

func annotationsSize(annotations map[string]string) int {
	size := 0
	for k, v := range annotations {
		size += len(k) + len(v)
	}
	return size
}

type benchAteletServer struct {
	ateletpb.UnimplementedAteletServer
}

func (benchAteletServer) Run(context.Context, *ateletpb.RunRequest) (*ateletpb.RunResponse, error) {
	return &ateletpb.RunResponse{WorkerEpoch: registeredEpoch}, nil
}

func (benchAteletServer) Restore(context.Context, *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error) {
	return &ateletpb.RestoreResponse{WorkerEpoch: registeredEpoch}, nil
}

func (benchAteletServer) Checkpoint(_ context.Context, req *ateletpb.CheckpointRequest) (*ateletpb.CheckpointResponse, error) {
	return &ateletpb.CheckpointResponse{
		State: &ateletpb.ActorSnapshotState{
			LatestLocal: &ateletpb.SnapshotReport{Snapshot: req.GetSnapshot()},
		},
	}, nil
}

func newBenchAteletDialer(b *testing.B) *AteletDialer {
	b.Helper()
	srv := grpc.NewServer(grpc.UnaryInterceptor(ateinterceptors.InternalServerUnaryInterceptor))
	ateletpb.RegisterAteletServer(srv, benchAteletServer{})
	lis := bufconn.Listen(1 << 20)
	go func() {
		_ = srv.Serve(lis)
	}()
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
	if err != nil {
		b.Fatalf("connecting to bench atelet: %v", err)
	}
	b.Cleanup(func() {
		conn.Close()
		srv.Stop()
	})

	ateletPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: installdefaults.SystemNamespace, Name: "atelet-1", UID: "atelet-uid"},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
		Status:     corev1.PodStatus{PodIPs: []corev1.PodIP{{IP: "10.0.0.1"}}},
	}
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		byNode: func(obj any) ([]string, error) {
			return []string{obj.(*corev1.Pod).Spec.NodeName}, nil
		},
	})
	if err := idx.Add(ateletPod); err != nil {
		b.Fatalf("adding atelet pod to indexer: %v", err)
	}
	dialer := NewAteletDialer(idx, installdefaults.SystemNamespace, "", "")
	dialer.ateletConns.Add("atelet-uid", &ateletConn{ip: "10.0.0.1", conn: conn})
	return dialer
}

// BenchmarkActorAnnotations measures the impact of ResourceMetadata.annotations
// across protobuf serialization, validation, PostgreSQL CRUD, and the cold
// ResumeActor/SuspendActor lifecycle. The PostgreSQL sub-benchmarks report how
// the actors row was stored (pg-column-bytes, pg-octet-bytes, toast-bytes,
// toast-chunks) so each profile's storage regime is visible in the output.
func BenchmarkActorAnnotations(b *testing.B) {
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	b.Cleanup(func() { slog.SetDefault(prevLogger) })
	for _, prof := range benchAnnotationProfiles {
		if got := annotationsSize(prof.annotations); got != prof.size {
			b.Fatalf("profile %s: annotations total %d bytes, want %d", prof.name, got, prof.size)
		}
	}

	b.Run("MarshalUnmarshalValidate", func(b *testing.B) {
		for _, prof := range benchAnnotationProfiles {
			b.Run(prof.name, func(b *testing.B) {
				actor := validActor(withActorStatus(func(s *ateapi.ActorStatus) {
					s.State = ateapi.ActorState_ACTOR_STATE_RUNNING
					s.AssignedNode = "node-1"
					s.LastAssignedGeneration = 2
					s.WorkerAssignment = wireTestAssignment()
					s.Snapshots = []*ateapi.Snapshot{
						newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-uid-1", "snap-1", "gs://bucket/atespaces/team-a/actors/uid-1/snapshots/snap-1", ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED),
						newDurableSnapshot(2, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-uid-1", "snap-2", "gs://bucket/atespaces/team-a/actors/uid-1/snapshots/snap-2", ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING),
					}
				}))
				actor.Metadata.Annotations = prof.annotations
				wire, err := proto.Marshal(actor)
				if err != nil {
					b.Fatalf("proto.Marshal: %v", err)
				}
				ctx := context.Background()
				actorPath := field.NewPath("actor")
				b.ReportAllocs()
				for b.Loop() {
					buf, err := proto.Marshal(actor)
					if err != nil {
						b.Fatalf("proto.Marshal: %v", err)
					}
					var decoded ateapi.Actor
					if err := proto.Unmarshal(buf, &decoded); err != nil {
						b.Fatalf("proto.Unmarshal: %v", err)
					}
					if errs := apivalidation.ValidateActorUpdate(ctx, actorPath, &decoded, actor, false); len(errs) > 0 {
						b.Fatalf("ValidateActorUpdate: %v", errs)
					}
				}
				b.ReportMetric(float64(len(wire)), "proto-bytes")
			})
		}
	})

	b.Run("CreateActor", func(b *testing.B) {
		for _, prof := range benchAnnotationProfiles {
			b.Run(prof.name, func(b *testing.B) {
				ctx := context.Background()
				persistence, svc, tmplRef := setupBenchControlAPI(b, ctx)
				pg := newBenchPG(b, ctx, persistence)
				sample, err := svc.CreateActor(ctx, &ateapi.CreateActorRequest{
					Actor: &ateapi.Actor{
						Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "actor-sample", Annotations: prof.annotations},
						ActorTemplate: tmplRef,
					},
				})
				if err != nil {
					b.Fatalf("CreateActor sample: %v", err)
				}
				storage := pg.rowStorage(resources.ActorRefFromActor(sample))
				b.ReportAllocs()
				var i int
				for b.Loop() {
					i++
					if _, err := svc.CreateActor(ctx, &ateapi.CreateActorRequest{
						Actor: &ateapi.Actor{
							Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: fmt.Sprintf("actor-%d", i), Annotations: prof.annotations},
							ActorTemplate: tmplRef,
						},
					}); err != nil {
						b.Fatalf("CreateActor: %v", err)
					}
				}
				storage.report(b)
				reportProtoBytes(b, sample)
			})
		}
	})

	b.Run("GetActor", func(b *testing.B) {
		for _, prof := range benchAnnotationProfiles {
			b.Run(prof.name, func(b *testing.B) {
				ctx := context.Background()
				persistence, svc, tmplRef := setupBenchControlAPI(b, ctx)
				pg := newBenchPG(b, ctx, persistence)
				created := createBenchActor(b, ctx, persistence, svc, tmplRef, "actor-1", prof)
				ref := resources.ActorRefFromActor(created)
				storage := pg.rowStorage(ref)
				blocks := pg.getActorBlocks(ref)
				req := &ateapi.GetActorRequest{Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "actor-1"}}
				b.ReportAllocs()
				for b.Loop() {
					if _, err := svc.GetActor(ctx, req); err != nil {
						b.Fatalf("GetActor: %v", err)
					}
				}
				storage.report(b)
				b.ReportMetric(blocks, "pg-blocks")
				reportProtoBytes(b, created)
			})
		}
	})

	b.Run("UpdateActor", func(b *testing.B) {
		for _, prof := range benchAnnotationProfiles {
			b.Run(prof.name, func(b *testing.B) {
				ctx := context.Background()
				persistence, svc, tmplRef := setupBenchControlAPI(b, ctx)
				pg := newBenchPG(b, ctx, persistence)
				created := createBenchActor(b, ctx, persistence, svc, tmplRef, "actor-1", prof)
				storage := pg.rowStorage(resources.ActorRefFromActor(created))
				selectors := []*ateapi.Selector{
					{MatchLabels: map[string]string{"tier": "a"}},
					{MatchLabels: map[string]string{"tier": "b"}},
				}
				uid := created.GetMetadata().GetUid()
				version := created.GetMetadata().GetVersion()
				var i int
				b.ReportAllocs()
				reportWrites := pg.measureWrites()
				for b.Loop() {
					i++
					updated, err := svc.UpdateActor(ctx, &ateapi.UpdateActorRequest{
						Actor: &ateapi.Actor{
							Metadata: &ateapi.ResourceMetadata{
								Atespace:    testAtespace,
								Name:        "actor-1",
								Uid:         uid,
								Version:     version,
								Annotations: prof.annotations,
							},
							ActorTemplate:  tmplRef,
							WorkerSelector: selectors[i&1],
						},
					})
					if err != nil {
						b.Fatalf("UpdateActor: %v", err)
					}
					version = updated.GetMetadata().GetVersion()
				}
				reportWrites()
				storage.report(b)
				reportProtoBytes(b, created)
			})
		}
	})

	for _, page := range []struct {
		name     string
		size     int
		profiles []benchAnnotationProfile
	}{
		{name: "ListActors_Page100", size: 100, profiles: benchAnnotationProfiles},
		{name: "ListActors_Page1000", size: maxPageSize, profiles: []benchAnnotationProfile{benchAnnotationProfiles[0], benchAnnotationProfiles[2]}},
	} {
		b.Run(page.name, func(b *testing.B) {
			for _, prof := range page.profiles {
				b.Run(prof.name, func(b *testing.B) {
					ctx := context.Background()
					persistence, svc, tmplRef := setupBenchControlAPI(b, ctx)
					pg := newBenchPG(b, ctx, persistence)
					var first *ateapi.Actor
					for i := range page.size {
						created := createBenchActor(b, ctx, persistence, svc, tmplRef, fmt.Sprintf("actor-%04d", i), prof)
						if i == 0 {
							first = created
						}
					}
					storage := pg.rowStorage(resources.ActorRefFromActor(first))
					req := &ateapi.ListActorsRequest{
						Atespace: testAtespace,
						Options:  &ateapi.ListOptions{PageSize: int32(page.size)},
					}
					var respBytes int
					b.ReportAllocs()
					for b.Loop() {
						resp, err := svc.ListActors(ctx, req)
						if err != nil || len(resp.GetActors()) != page.size {
							b.Fatalf("ListActors: %v (len=%d)", err, len(resp.GetActors()))
						}
						respBytes = proto.Size(resp)
					}
					// gRPC clients reject responses above 4 MiB unless they
					// raise grpc.MaxCallRecvMsgSize.
					b.ReportMetric(float64(respBytes), "resp-bytes")
					storage.report(b)
					reportProtoBytes(b, first)
				})
			}
		})
	}

	b.Run("ColdResumeSuspend", func(b *testing.B) {
		for _, prof := range benchAnnotationProfiles {
			b.Run(prof.name, func(b *testing.B) {
				ctx := context.Background()
				persistence, svc, tmplRef := setupBenchControlAPI(b, ctx)
				pg := newBenchPG(b, ctx, persistence)
				workerName := testWorkerUID("pod-1")
				if _, err := persistence.CreateWorker(ctx, &ateapi.Worker{
					Metadata:        &ateapi.ResourceMetadata{Name: workerName},
					WorkerNamespace: "worker-ns",
					WorkerPool:      "pool",
					WorkerPod:       "pod-1",
					WorkerPodUid:    workerName,
					NodeName:        "node-1",
					Status: &ateapi.WorkerStatus{
						State:           ateapi.WorkerState_WORKER_STATE_ACTIVE,
						RegisteredEpoch: registeredEpoch,
						Capacity: &ateapi.WorkerResources{
							Actors:        500,
							CpuMillicores: 500 * 1000,
							MemoryBytes:   500 * (1 << 30),
						},
						SandboxClasses: workerSandboxClasses(testSandboxRuntime()),
					},
				}); err != nil {
					b.Fatalf("CreateWorker: %v", err)
				}

				cacheCtx, cancel := context.WithCancel(ctx)
				b.Cleanup(cancel)
				wc := workercache.New(persistence, time.Minute)
				if err := wc.Start(cacheCtx); err != nil {
					b.Fatalf("workercache.Start: %v", err)
				}
				w := &ActorWorkflow{
					store:       persistence,
					dialer:      newBenchAteletDialer(b),
					workerCache: wc,
					scheduler:   scheduling.New(wc),
				}

				actorRef := resources.ActorRef{Atespace: testAtespace, Name: "actor-1"}
				if _, err := svc.CreateActor(ctx, &ateapi.CreateActorRequest{
					Actor: &ateapi.Actor{
						Metadata:      &ateapi.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name, Annotations: prof.annotations},
						ActorTemplate: tmplRef,
					},
				}); err != nil {
					b.Fatalf("CreateActor: %v", err)
				}

				// Run one initial cycle so every timed iteration restores from a
				// local checkpoint and suspends with a pruned snapshot history.
				if _, _, err := w.ResumeActor(ctx, actorRef); err != nil {
					b.Fatalf("warmup ResumeActor: %v", err)
				}
				suspended, err := w.SuspendActor(ctx, actorRef, suspendAsync)
				if err != nil {
					b.Fatalf("warmup SuspendActor: %v", err)
				}
				storage := pg.rowStorage(actorRef)

				// One unmeasured cycle attributes WAL to each half.
				lsn := pg.walLSN()
				if _, _, err := w.ResumeActor(ctx, actorRef); err != nil {
					b.Fatalf("sample ResumeActor: %v", err)
				}
				resumeWAL := pg.walBytesSince(lsn)
				lsn = pg.walLSN()
				if _, err := w.SuspendActor(ctx, actorRef, suspendAsync); err != nil {
					b.Fatalf("sample SuspendActor: %v", err)
				}
				suspendWAL := pg.walBytesSince(lsn)

				var resumeTime, suspendTime time.Duration
				b.ReportAllocs()
				reportWrites := pg.measureWrites()
				for b.Loop() {
					start := time.Now()
					if _, _, err := w.ResumeActor(ctx, actorRef); err != nil {
						b.Fatalf("ResumeActor: %v", err)
					}
					resumed := time.Now()
					if _, err := w.SuspendActor(ctx, actorRef, suspendAsync); err != nil {
						b.Fatalf("SuspendActor: %v", err)
					}
					resumeTime += resumed.Sub(start)
					suspendTime += time.Since(resumed)
				}
				reportWrites()
				n := float64(b.N)
				b.ReportMetric(float64(resumeTime)/n, "resume-ns/op")
				b.ReportMetric(float64(suspendTime)/n, "suspend-ns/op")
				b.ReportMetric(resumeWAL, "resume-wal-bytes")
				b.ReportMetric(suspendWAL, "suspend-wal-bytes")
				storage.report(b)
				reportProtoBytes(b, suspended)
			})
		}
	})
}

func setupBenchControlAPI(b *testing.B, ctx context.Context) (store.Interface, *RPCService, *ateapi.ObjectRef) {
	b.Helper()
	persistence, cleanup := storetest.SetupTestStore(b)
	b.Cleanup(cleanup)
	impl := newServiceImpl(persistence, nil)
	impl.clusterSnapshotLocation = testStorageLocation
	svc := &RPCService{impl: impl}
	storetest.MustCreateAtespace(b, ctx, persistence, testAtespace)

	tmpl, err := svc.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{
		ActorTemplate: validActorTemplate(func(t *ateapi.ActorTemplate) {
			t.Metadata = &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "bench-tmpl"}
			t.SnapshotConfig = &ateapi.SnapshotConfig{
				StorageLocation:   testStorageLocation,
				PreferredFidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			}
		}),
	})
	if err != nil {
		b.Fatalf("CreateActorTemplate: %v", err)
	}
	return persistence, svc, resources.ActorTemplateRefFromActorTemplate(tmpl).ToObjectRef()
}

// createBenchActor creates name through the API and then gives it the status
// of an actor that has run for a while: a worker assignment, four durable
// snapshots and three external volumes. That is about 1.5 KiB on the wire
// before annotations, so with annotations the row straddles PostgreSQL's 2 KiB
// TOAST threshold the way production rows do.
func createBenchActor(b *testing.B, ctx context.Context, persistence store.Interface, svc *RPCService, tmplRef *ateapi.ObjectRef, name string, prof benchAnnotationProfile) *ateapi.Actor {
	b.Helper()
	created, err := svc.CreateActor(ctx, &ateapi.CreateActorRequest{
		Actor: &ateapi.Actor{
			Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: name, Annotations: prof.annotations},
			ActorTemplate: tmplRef,
		},
	})
	if err != nil {
		b.Fatalf("CreateActor %s: %v", name, err)
	}
	uid := created.GetMetadata().GetUid()
	status := &ateapi.ActorStatus{
		State:                  ateapi.ActorState_ACTOR_STATE_RUNNING,
		AssignedNode:           "node-1",
		LastAssignedGeneration: 4,
		WorkerAssignment:       wireTestAssignment(),
		ExternalVolumes:        &ateapi.ExternalVolumes{},
	}
	for gen := int32(1); gen <= 4; gen++ {
		snapName := fmt.Sprintf("snap-%d", gen)
		uri, err := resources.NewActorSnapshotURI(testStorageLocation, testAtespace, uid, snapName)
		if err != nil {
			b.Fatalf("NewActorSnapshotURI: %v", err)
		}
		snap := newDurableSnapshot(gen, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-uid-1", snapName, uri.String(), ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED)
		snap.SandboxRuntime = testSandboxRuntime()
		status.Snapshots = append(status.Snapshots, snap)
	}
	for i := range 3 {
		status.ExternalVolumes.Volumes = append(status.ExternalVolumes.Volumes, &ateapi.ExternalVolumeStatus{
			Name:            fmt.Sprintf("data-%d", i),
			StorageVolumeId: fmt.Sprintf("projects/example/zones/us-central1-a/disks/substrate-%s-data-%d", uid, i),
			VolumeType:      "pd.csi.storage.gke.io",
			VolumeContext:   map[string]string{"fsType": "ext4", "zone": "us-central1-a"},
		})
	}
	grown, err := persistence.UpdateActor(ctx, resources.ActorRefFromActor(created), store.PreconditionFrom(created), func(a *ateapi.Actor) error {
		a.Status = status
		return nil
	})
	if err != nil {
		b.Fatalf("UpdateActor %s status: %v", name, err)
	}
	return grown
}

func reportProtoBytes(b *testing.B, m proto.Message) {
	b.Helper()
	b.ReportMetric(float64(proto.Size(m)), "proto-bytes")
}

// benchPG queries PostgreSQL about the actors table behind a benchmark.
type benchPG struct {
	b    *testing.B
	ctx  context.Context
	pool *pgxpool.Pool
}

func newBenchPG(b *testing.B, ctx context.Context, persistence store.Interface) benchPG {
	b.Helper()
	return benchPG{b: b, ctx: ctx, pool: storetest.Pool(b, persistence)}
}

func (p benchPG) queryFloat(query string, args ...any) float64 {
	p.b.Helper()
	var v float64
	if err := p.pool.QueryRow(p.ctx, query, args...).Scan(&v); err != nil {
		p.b.Fatalf("%s: %v", query, err)
	}
	return v
}

// benchRowStorage is how PostgreSQL stored one actors row: columnBytes is
// the on-disk size of proto (compressed if compression succeeded), octetBytes
// its logical size, and toastChunks and toastBytes describe the table's TOAST
// relation, which stays empty while every row fits inline.
type benchRowStorage struct {
	columnBytes, octetBytes, toastChunks, toastBytes float64
}

// rowStorage measures ref's row. Report the result after the loop: b.Loop
// clears metrics reported before its first iteration.
func (p benchPG) rowStorage(ref resources.ActorRef) benchRowStorage {
	p.b.Helper()
	var s benchRowStorage
	if err := p.pool.QueryRow(p.ctx, `SELECT pg_column_size(proto)::float8, octet_length(proto)::float8 FROM actors WHERE atespace = $1 AND name = $2`,
		ref.Atespace, ref.Name).Scan(&s.columnBytes, &s.octetBytes); err != nil {
		p.b.Fatalf("pg_column_size(actors.proto): %v", err)
	}
	var toastRel string
	if err := p.pool.QueryRow(p.ctx, `SELECT reltoastrelid::regclass::text FROM pg_class WHERE oid = 'actors'::regclass`).Scan(&toastRel); err != nil {
		p.b.Fatalf("actors reltoastrelid: %v", err)
	}
	s.toastChunks = p.queryFloat(`SELECT count(*)::float8 FROM ` + toastRel)
	s.toastBytes = p.queryFloat(`SELECT pg_relation_size($1)::float8`, toastRel)
	return s
}

func (s benchRowStorage) report(b *testing.B) {
	b.ReportMetric(s.columnBytes, "pg-column-bytes")
	b.ReportMetric(s.octetBytes, "pg-octet-bytes")
	b.ReportMetric(s.toastChunks, "toast-chunks")
	b.ReportMetric(s.toastBytes, "toast-bytes")
}

func (p benchPG) walLSN() string {
	p.b.Helper()
	var lsn string
	if err := p.pool.QueryRow(p.ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&lsn); err != nil {
		p.b.Fatalf("pg_current_wal_lsn: %v", err)
	}
	return lsn
}

func (p benchPG) walBytesSince(lsn string) float64 {
	p.b.Helper()
	return p.queryFloat(`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)::float8`, lsn)
}

// measureWrites snapshots the WAL position and the actors table size. The
// returned func reports both deltas per iteration; call it after the loop.
func (p benchPG) measureWrites() func() {
	p.b.Helper()
	lsn := p.walLSN()
	const tableSize = `SELECT pg_total_relation_size('actors')::float8`
	before := p.queryFloat(tableSize)
	return func() {
		n := float64(p.b.N)
		p.b.ReportMetric(p.walBytesSince(lsn)/n, "wal-bytes/op")
		p.b.ReportMetric((p.queryFloat(tableSize)-before)/n, "table-growth-bytes/op")
	}
}

type benchExplainBuffers struct {
	Hit  float64 `json:"Shared Hit Blocks"`
	Read float64 `json:"Shared Read Blocks"`
}

// getActorBlocks returns the shared buffers GetActor's SELECT touches.
// SERIALIZE makes EXPLAIN detoast the result, so TOAST reads are counted.
func (p benchPG) getActorBlocks(ref resources.ActorRef) float64 {
	p.b.Helper()
	var plan string
	if err := p.pool.QueryRow(p.ctx, `EXPLAIN (ANALYZE, BUFFERS, SERIALIZE, FORMAT JSON) SELECT proto FROM actors WHERE atespace = $1 AND name = $2`,
		ref.Atespace, ref.Name).Scan(&plan); err != nil {
		p.b.Fatalf("EXPLAIN GetActor: %v", err)
	}
	var explained []struct {
		Plan          benchExplainBuffers `json:"Plan"`
		Serialization benchExplainBuffers `json:"Serialization"`
	}
	if err := json.Unmarshal([]byte(plan), &explained); err != nil || len(explained) != 1 {
		p.b.Fatalf("parsing EXPLAIN output %q: %v", plan, err)
	}
	e := explained[0]
	return e.Plan.Hit + e.Plan.Read + e.Serialization.Hit + e.Serialization.Read
}
