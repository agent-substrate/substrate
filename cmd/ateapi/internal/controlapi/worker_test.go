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

package controlapi

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateListWorkersRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.ListWorkersRequest
		want field.ErrorList
	}{{
		"valid, no page_size",
		&ateapipb.ListWorkersRequest{},
		nil,
	}, {
		"valid, positive page_size",
		&ateapipb.ListWorkersRequest{PageSize: 10},
		nil,
	}, {
		"negative page_size",
		&ateapipb.ListWorkersRequest{PageSize: -1},
		field.ErrorList{field.Invalid(field.NewPath("page_size"), int32(-1), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateListWorkersRequest(tt.req), tt.want)
		})
	}
}

// The Worker CRUD methods are declared but not implemented. This pins that they
// report UNIMPLEMENTED rather than panicking on their nil dependencies, and
// will fail loudly as each one is filled in — at which point the corresponding
// case moves to a real test.
func TestWorkerAPIUnimplemented(t *testing.T) {
	s := &RPCService{}
	ctx := context.Background()

	tests := []struct {
		name string
		call func() error
	}{
		{"GetWorker", func() error {
			_, err := s.GetWorker(ctx, &ateapipb.GetWorkerRequest{})
			return err
		}},
		{"CreateWorker", func() error {
			_, err := s.CreateWorker(ctx, &ateapipb.CreateWorkerRequest{})
			return err
		}},
		{"UpdateWorker", func() error {
			_, err := s.UpdateWorker(ctx, &ateapipb.UpdateWorkerRequest{})
			return err
		}},
		{"DeleteWorker", func() error {
			_, err := s.DeleteWorker(ctx, &ateapipb.DeleteWorkerRequest{})
			return err
		}},
		{"DrainWorker", func() error {
			_, err := s.DrainWorker(ctx, &ateapipb.DrainWorkerRequest{})
			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if got := status.Code(err); got != codes.Unimplemented {
				t.Errorf("%s: got code %v (err %v), want %v", tc.name, got, err, codes.Unimplemented)
			}
		})
	}
}
