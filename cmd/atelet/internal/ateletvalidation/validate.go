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

package ateletvalidation

import (
	"context"
	"reflect"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ValidateRequestActorSuspendRequest validates req at the RPC edge. A non-nil
// return is the InvalidArgument error the handler responds with.
func ValidateRequestActorSuspendRequest(ctx context.Context, req *ateletpb.RequestActorSuspendRequest) error {
	return toInvalidArgument(Validate_RequestActorSuspendRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil))
}

// ValidateMintActorCertificateRequest validates req at the RPC edge. A
// non-nil return is the InvalidArgument error the handler responds with.
func ValidateMintActorCertificateRequest(ctx context.Context, req *ateletpb.MintActorCertificateRequest) error {
	return toInvalidArgument(Validate_MintActorCertificateRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil))
}

func toInvalidArgument(errs field.ErrorList) error {
	if len(errs) == 0 {
		return nil
	}
	return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
}

// ateDeepEqual compares two values of any type, using proto.Equal if both are
// proto messages, and reflect.DeepEqual otherwise. This is called by
// declarative validation's generated code. It is a copy of controlapi's
// ateDeepEqual: the generated code calls it as a package-level identifier, so
// each generating package carries its own.
func ateDeepEqual[T any](a, b T) bool {
	asProto := func(x any) proto.Message {
		pm, ok := x.(proto.Message)
		if !ok {
			return nil
		}
		return pm
	}

	if pa, pb := asProto(a), asProto(b); pa != nil && pb != nil {
		return proto.Equal(pa, pb)
	}
	return reflect.DeepEqual(a, b)
}
