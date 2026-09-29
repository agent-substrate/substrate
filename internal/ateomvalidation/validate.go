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

package ateomvalidation

import (
	"context"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/api/operation"
)

// Each wrapper validates its request at the RPC edge. A non-nil return is the
// InvalidArgument error the handler responds with, built by resources.ToAPIError. Requests are validated as
// creates: no Ateom RPC updates prior state, so there is nothing to ratchet
// against.

// ValidateRunWorkloadRequest validates req at the RPC edge.
func ValidateRunWorkloadRequest(ctx context.Context, req *ateompb.RunWorkloadRequest) error {
	if errs := Validate_RunWorkloadRequest(ctx, createOp(), nil, req, nil); len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	return nil
}

// ValidateCheckpointWorkloadRequest validates req at the RPC edge.
func ValidateCheckpointWorkloadRequest(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) error {
	if errs := Validate_CheckpointWorkloadRequest(ctx, createOp(), nil, req, nil); len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	return nil
}

// ValidateRestoreWorkloadRequest validates req at the RPC edge.
func ValidateRestoreWorkloadRequest(ctx context.Context, req *ateompb.RestoreWorkloadRequest) error {
	if errs := Validate_RestoreWorkloadRequest(ctx, createOp(), nil, req, nil); len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	return nil
}

// ValidateTerminateWorkloadRequest validates req at the RPC edge.
func ValidateTerminateWorkloadRequest(ctx context.Context, req *ateompb.TerminateWorkloadRequest) error {
	if errs := Validate_TerminateWorkloadRequest(ctx, createOp(), nil, req, nil); len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	return nil
}

// ValidateGetWorkloadStatsRequest validates req at the RPC edge.
func ValidateGetWorkloadStatsRequest(ctx context.Context, req *ateompb.GetWorkloadStatsRequest) error {
	if errs := Validate_GetWorkloadStatsRequest(ctx, createOp(), nil, req, nil); len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	return nil
}

// GetActiveWorkloadStatsRequest has no fields, so validation-gen emits
// nothing for it and there is no wrapper.

func createOp() operation.Operation {
	return operation.Operation{Type: operation.Create}
}

// ateDeepEqual is the deep-equal function declarative validation's generated
// code calls by name; it delegates to resources.DeepEqual.
func ateDeepEqual[T any](a, b T) bool {
	return resources.DeepEqual(a, b)
}
