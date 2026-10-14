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

package removedfields

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func varint(num protowire.Number) []byte {
	return protowire.AppendVarint(protowire.AppendTag(nil, num, protowire.VarintType), 1)
}

func TestLookup(t *testing.T) {
	if f, ok := Lookup("ateapi.SnapshotConfig", 1, protowire.VarintType); !ok || f.Name != "on_pause" {
		t.Errorf("Lookup(SnapshotConfig, 1, varint) = %v, %v; want on_pause, true", f, ok)
	}
	for _, tc := range []struct {
		msg protoreflect.FullName
		num protowire.Number
		typ protowire.Type
	}{
		{"ateapi.SnapshotConfig", 1, protowire.BytesType},
		{"ateapi.SnapshotConfig", 9999, protowire.VarintType},
		{"ateapi.ActorTemplate", 1, protowire.VarintType},
	} {
		if f, ok := Lookup(tc.msg, tc.num, tc.typ); ok {
			t.Errorf("Lookup(%s, %d, %v) = %v, true; want no removed field", tc.msg, tc.num, tc.typ, f)
		}
	}
}

func TestDiscardUnknown(t *testing.T) {
	cfg := &ateapipb.SnapshotConfig{}
	cfg.ProtoReflect().SetUnknown(append(append(varint(9998), varint(1)...), varint(9999)...))
	template := &ateapipb.ActorTemplate{SnapshotConfig: cfg}
	template.ProtoReflect().SetUnknown(varint(1))

	DiscardUnknown(template.ProtoReflect())

	if got, want := cfg.ProtoReflect().GetUnknown(), varint(1); !bytes.Equal(got, want) {
		t.Errorf("snapshot_config unknown fields = %x, want only on_pause %x", got, want)
	}
	if got := template.ProtoReflect().GetUnknown(); len(got) != 0 {
		t.Errorf("actor_template unknown fields = %x, want none: field 1 is removed only on SnapshotConfig", got)
	}
}
