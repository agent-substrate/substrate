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
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func actorRuntimeLeaseProto(lease *store.ActorRuntimeLease) *ateapipb.ActorLease {
	if lease == nil {
		return nil
	}
	return &ateapipb.ActorLease{
		Token:      lease.Token,
		Generation: lease.Generation,
		ExpiresAt:  timestamppb.New(lease.ExpiresAt),
	}
}

func runtimeLeaseMatchesProto(lease *store.ActorRuntimeLease, requested *ateapipb.ActorLease) bool {
	return requested != nil && lease != nil &&
		lease.Token == requested.GetToken() &&
		lease.Generation == requested.GetGeneration()
}
