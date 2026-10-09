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

package ateapipb_test

import (
	"encoding/json"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestTimestampFieldNames(t *testing.T) {
	tests := []struct {
		message  proto.Message
		name     protoreflect.Name
		jsonName string
	}{
		{&ateapipb.MintActorJWTResponse{}, "expire_time", "expireTime"},
		{&ateapipb.GoldenSnapshotStatus{}, "snapshot_time", "snapshotTime"},
	}
	timestamp := &timestamppb.Timestamp{Seconds: 1700000000, Nanos: 123456789}
	for _, tt := range tests {
		t.Run(string(tt.message.ProtoReflect().Descriptor().Name()), func(t *testing.T) {
			message := tt.message.ProtoReflect()
			field := message.Descriptor().Fields().ByName(tt.name)
			if field == nil {
				t.Fatalf("missing Timestamp field %q", tt.name)
			}
			if field.Number() != 2 {
				t.Errorf("field number = %d, want 2", field.Number())
			}
			message.Set(field, protoreflect.ValueOfMessage(timestamp.ProtoReflect()))

			for _, useProtoNames := range []bool{false, true} {
				name := tt.jsonName
				if useProtoNames {
					name = string(tt.name)
				}
				t.Run(name, func(t *testing.T) {
					data, err := (protojson.MarshalOptions{UseProtoNames: useProtoNames}).Marshal(tt.message)
					if err != nil {
						t.Fatalf("Marshal: %v", err)
					}
					var fields map[string]string
					if err := json.Unmarshal(data, &fields); err != nil {
						t.Fatalf("unmarshal JSON fields: %v", err)
					}
					if len(fields) != 1 || fields[name] != "2023-11-14T22:13:20.123456789Z" {
						t.Errorf("JSON fields = %v, want only %q with the original Timestamp", fields, name)
					}
					decoded := message.New().Interface()
					if err := protojson.Unmarshal(data, decoded); err != nil {
						t.Fatalf("Unmarshal: %v", err)
					}
					if !proto.Equal(decoded, tt.message) {
						t.Errorf("round trip = %v, want %v", decoded, tt.message)
					}
				})
			}
		})
	}
}
