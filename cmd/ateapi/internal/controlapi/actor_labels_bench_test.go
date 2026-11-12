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
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"strings"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/proto"
)

// benchmarkActorLabels uses distinct DNS prefixes and values to avoid repeated padding.
func benchmarkActorLabels(size int) map[string]string {
	if size == 0 {
		return nil
	}
	random := rand.New(rand.NewPCG(1, 2))
	randomString := func(length int) string {
		const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
		value := make([]byte, length)
		for i := range value {
			value[i] = alphabet[random.IntN(len(alphabet))]
		}
		return string(value)
	}
	labels := make(map[string]string, 16)
	for i := range 16 {
		entrySize := size / 16
		if i < size%16 {
			entrySize++
		}
		name := fmt.Sprintf("k%02d", i)
		valueLength := min(63, entrySize-len(name))
		prefixLength := entrySize - len(name) - valueLength - 1
		if prefixLength > 0 {
			var prefix strings.Builder
			for prefixLength > 63 {
				prefix.WriteString(randomString(62))
				prefix.WriteByte('.')
				prefixLength -= 63
			}
			prefix.WriteString(randomString(prefixLength))
			name = prefix.String() + "/" + name
		}
		labels[name] = randomString(valueLength)
	}
	return labels
}

func BenchmarkActorLabels(b *testing.B) {
	logger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))
	b.Cleanup(func() { slog.SetDefault(logger) })
	for _, size := range []int{0, 256, 1024} {
		for _, snapshots := range []int{0, 1} {
			for _, method := range []string{"Create", "Get", "Update", "List100"} {
				if snapshots > 0 && method == "Create" {
					continue
				}
				b.Run(fmt.Sprintf("bytes=%d/snapshots=%d/%s", size, snapshots, method), func(b *testing.B) {
					ctx := b.Context()
					persistence := storetest.SetupPostgresPersistence(b)
					service := &RPCService{impl: newServiceImpl(persistence, nil)}
					if _, err := persistence.CreateAtespace(ctx, &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: "bench"}}); err != nil {
						b.Fatal(err)
					}
					template, err := service.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(template *ateapi.ActorTemplate) {
						template.Metadata = &ateapi.ResourceMetadata{Atespace: "bench", Name: "template"}
						template.SnapshotConfig.StorageLocation = "gs://bench/snapshots"
					})})
					if err != nil {
						b.Fatal(err)
					}
					labels := benchmarkActorLabels(size)
					create := func(name string) *ateapi.Actor {
						actor, err := service.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
							Metadata:      &ateapi.ResourceMetadata{Atespace: "bench", Name: name, Labels: labels},
							ActorTemplate: resources.ActorTemplateRefFromActorTemplate(template).ToObjectRef(),
						}})
						if err != nil {
							b.Fatal(err)
						}
						if snapshots > 0 {
							snapshotName := resources.NewSnapshotName()
							snapshotURI, err := resources.NewActorSnapshotURI(template.GetSnapshotConfig().GetStorageLocation(), "bench", actor.GetMetadata().GetUid(), snapshotName)
							if err != nil {
								b.Fatal(err)
							}
							actor, err = service.impl.UpdateActor(ctx, resources.ActorRefFromActor(actor), store.PreconditionFrom(actor), func(actor *ateapi.Actor) error {
								actor.Status.LastAssignedGeneration = 1
								actor.Status.Snapshots = []*ateapi.Snapshot{newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, template.GetMetadata().GetUid(), snapshotName, snapshotURI.String(), ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED)}
								return nil
							})
							if err != nil {
								b.Fatal(err)
							}
						}
						return actor
					}
					actor := create("actor")
					if method == "List100" {
						for i := range 99 {
							create(fmt.Sprintf("actor-%d", i))
						}
					}
					var wireBytes, storedBytes, toastBytes int
					if err := persistence.Pool().QueryRow(ctx, `SELECT octet_length(proto), pg_column_size(proto),
					(SELECT pg_relation_size(reltoastrelid) FROM pg_class WHERE oid = 'actors'::regclass)
					FROM actors WHERE atespace = 'bench' AND name = 'actor'`).Scan(&wireBytes, &storedBytes, &toastBytes); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					counter := 0
					for b.Loop() {
						switch method {
						case "Create":
							create(fmt.Sprintf("created-%d", counter))
							counter++
						case "Get":
							if _, err := service.GetActor(ctx, &ateapi.GetActorRequest{Actor: resources.ActorRefFromActor(actor).ToObjectRef()}); err != nil {
								b.Fatal(err)
							}
						case "Update":
							actor, err = service.UpdateActor(ctx, &ateapi.UpdateActorRequest{Actor: proto.CloneOf(actor)})
							if err != nil {
								b.Fatal(err)
							}
						case "List100":
							page, err := service.ListActors(ctx, &ateapi.ListActorsRequest{Atespace: "bench", Options: &ateapi.ListOptions{PageSize: 100}})
							if err != nil {
								b.Fatal(err)
							}
							if len(page.GetActors()) != 100 {
								b.Fatalf("listed %d actors, want 100", len(page.GetActors()))
							}
						}
					}
					b.ReportMetric(float64(wireBytes), "proto-B/actor")
					b.ReportMetric(float64(storedBytes), "stored-B/actor")
					b.ReportMetric(float64(toastBytes), "toast-B/table")
				})
			}
		}
	}
}
