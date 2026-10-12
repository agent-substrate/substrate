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

package atepg

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

const (
	legacyTestBundleName = "egress-mitm.ate.dev"
	legacyTestBundlePath = "trust-bundle.pem"
)

// legacyTestTemplate is a template whose system-info volume projects the
// actor's name and the given trust bundle.
func legacyTestTemplate(trustBundle *ateapipb.TrustBundleDataSource) *ateapipb.ActorTemplate {
	return &ateapipb.ActorTemplate{
		Metadata:       &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "template-a"},
		SnapshotConfig: &ateapipb.SnapshotConfig{},
		Volumes:        legacyTestVolumes(trustBundle),
	}
}

func legacyTestVolumes(trustBundle *ateapipb.TrustBundleDataSource) []*ateapipb.Volume {
	return []*ateapipb.Volume{{
		Name: "system-info",
		SystemInfo: &ateapipb.SystemInfoVolumeSource{
			DataSources: []*ateapipb.SystemInfoDataSource{
				{ActorMetadata: &ateapipb.ActorMetadataDataSource{Items: []*ateapipb.ActorMetadataItem{{
					Field: ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME, Path: "actor-name",
				}}}},
				{TrustBundle: trustBundle},
			},
		},
	}}
}

// legacyTestTrustBundleNames returns the names of the trust bundle
// legacyTestVolumes projects, as template holds them.
func legacyTestTrustBundleNames(template *ateapipb.ActorTemplate) []string {
	return template.GetVolumes()[0].GetSystemInfo().GetDataSources()[1].GetTrustBundle().GetNames()
}

func TestUnmarshalStoredMigratesLegacyTrustBundleName(t *testing.T) {
	// What every release since reads: the bundles the projection unifies.
	migrated := &ateapipb.TrustBundleDataSource{Names: []string{legacyTestBundleName}, Path: legacyTestBundlePath}
	legacy := func() *ateapipb.TrustBundleDataSource {
		return withUnknownString(&ateapipb.TrustBundleDataSource{Path: legacyTestBundlePath}, legacyTrustBundleDataSourceName, legacyTestBundleName)
	}

	tests := []struct {
		name    string
		written *ateapipb.ActorTemplate
		want    []*ateapipb.Volume
	}{{
		// Before names: field 1 is the one bundle the projection unified.
		name:    "legacy name",
		written: legacyTestTemplate(legacy()),
		want:    legacyTestVolumes(migrated),
	}, {
		name:    "current names",
		written: legacyTestTemplate(proto.CloneOf(migrated)),
		want:    legacyTestVolumes(migrated),
	}, {
		name: "names win over a legacy name",
		written: legacyTestTemplate(withUnknownString(
			&ateapipb.TrustBundleDataSource{Names: []string{"other.ate.dev"}, Path: legacyTestBundlePath},
			legacyTrustBundleDataSourceName, legacyTestBundleName)),
		want: legacyTestVolumes(&ateapipb.TrustBundleDataSource{Names: []string{"other.ate.dev"}, Path: legacyTestBundlePath}),
	}, {
		name:    "a projection without any name stays without one",
		written: legacyTestTemplate(&ateapipb.TrustBundleDataSource{Path: legacyTestBundlePath}),
		want:    legacyTestVolumes(&ateapipb.TrustBundleDataSource{Path: legacyTestBundlePath}),
	}, {
		name:    "two projections, one legacy",
		written: twoProjections(legacyTestTemplate(legacy()), proto.CloneOf(migrated)),
		want:    twoProjections(legacyTestTemplate(migrated), migrated).GetVolumes(),
	}, {
		name:    "unknown fields of a newer release are dropped",
		written: withUnknownString(legacyTestTemplate(withUnknownString(proto.CloneOf(migrated), 9999, "newer")), 9999, "newer"),
		want:    legacyTestVolumes(migrated),
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, err := proto.Marshal(tc.written)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			got := &ateapipb.ActorTemplate{}
			if err := unmarshalStored(b, got); err != nil {
				t.Fatalf("unmarshalStored: %v", err)
			}
			if diff := cmp.Diff(tc.want, got.GetVolumes(), protocmp.Transform()); diff != "" {
				t.Errorf("volumes (-want +got):\n%s", diff)
			}
			reencoded, err := proto.Marshal(got)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if !unknownFieldsFree(t, reencoded, &ateapipb.ActorTemplate{}) {
				t.Errorf("the decoded template still holds unknown fields")
			}
		})
	}
}

// twoProjections adds a second system-info volume to template, projecting
// trustBundle alone.
func twoProjections(template *ateapipb.ActorTemplate, trustBundle *ateapipb.TrustBundleDataSource) *ateapipb.ActorTemplate {
	template.Volumes = append(template.Volumes, &ateapipb.Volume{
		Name: "system-info-2",
		SystemInfo: &ateapipb.SystemInfoVolumeSource{
			DataSources: []*ateapipb.SystemInfoDataSource{{TrustBundle: trustBundle}},
		},
	})
	return template
}

func TestLegacyTrustBundleNameMigrationIsPersisted(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	createTestAtespace(t, s, "team-a")
	created, err := s.CreateActorTemplate(ctx, legacyTestTemplate(&ateapipb.TrustBundleDataSource{
		Names: []string{legacyTestBundleName}, Path: legacyTestBundlePath,
	}))
	if err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}
	templateRef := resources.ActorTemplateRefFromActorTemplate(created)

	// Overwrite the row with what a release before names wrote for this
	// template.
	legacy := proto.CloneOf(created)
	legacy.Volumes = legacyTestVolumes(withUnknownString(
		&ateapipb.TrustBundleDataSource{Path: legacyTestBundlePath}, legacyTrustBundleDataSourceName, legacyTestBundleName))
	legacyBytes, err := proto.Marshal(legacy)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE actor_templates SET proto = $1 WHERE atespace = $2 AND name = $3`, legacyBytes, templateRef.Atespace, templateRef.Name); err != nil {
		t.Fatalf("writing the legacy row: %v", err)
	}

	wantNames := []string{legacyTestBundleName}
	got, err := s.GetActorTemplate(ctx, templateRef)
	if err != nil {
		t.Fatalf("GetActorTemplate: %v", err)
	}
	if diff := cmp.Diff(wantNames, legacyTestTrustBundleNames(got)); diff != "" {
		t.Fatalf("GetActorTemplate trust bundle names (-want +got):\n%s", diff)
	}

	if _, err := s.UpdateActorTemplate(ctx, templateRef, store.PreconditionFrom(got), func(*ateapipb.ActorTemplate) error { return nil }); err != nil {
		t.Fatalf("UpdateActorTemplate: %v", err)
	}
	var stored []byte
	if err := s.pool.QueryRow(ctx, `SELECT proto FROM actor_templates WHERE atespace = $1 AND name = $2`, templateRef.Atespace, templateRef.Name).Scan(&stored); err != nil {
		t.Fatalf("reading the row: %v", err)
	}
	if !unknownFieldsFree(t, stored, &ateapipb.ActorTemplate{}) {
		t.Errorf("the updated row still holds the legacy name")
	}
	persisted := &ateapipb.ActorTemplate{}
	if err := proto.Unmarshal(stored, persisted); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if diff := cmp.Diff(wantNames, legacyTestTrustBundleNames(persisted)); diff != "" {
		t.Errorf("persisted trust bundle names (-want +got):\n%s", diff)
	}
}

// withUnknownString appends a length-delimited field m's descriptor does not
// declare, the way an older release's encoding carries it.
func withUnknownString[M proto.Message](m M, num protowire.Number, v string) M {
	r := m.ProtoReflect()
	b := protowire.AppendTag(r.GetUnknown(), num, protowire.BytesType)
	r.SetUnknown(protowire.AppendString(b, v))
	return m
}

// unknownFieldsFree reports whether b decodes without a field the current
// descriptors leave unknown, anywhere in a message of kind's type.
func unknownFieldsFree(t *testing.T, b []byte, kind proto.Message) bool {
	t.Helper()
	strict := kind.ProtoReflect().New().Interface()
	if err := proto.Unmarshal(b, strict); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	discarded := kind.ProtoReflect().New().Interface()
	if err := (proto.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(b, discarded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	return proto.Equal(strict, discarded)
}
