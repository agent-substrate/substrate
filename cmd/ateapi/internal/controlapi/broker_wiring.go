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
	"fmt"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/storagebroker"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// snapshotCapabilityTTL bounds how long a minted snapshot read/write capability
// is valid. It must comfortably exceed a restore or checkpoint transfer, but
// stay short so a leaked URL exposes one snapshot only briefly.
const snapshotCapabilityTTL = 15 * time.Minute

// readAccessFor mints a read capability for each snapshot URI the node will
// read, returning them keyed by URI. A restore that combines the actor's own
// snapshot with a golden snapshot passes both; the caller may pass an unset
// (empty) URI unconditionally and it is skipped. With no broker configured it
// returns nil and the node reads with its built-in storage client.
func (w *ActorWorkflow) readAccessFor(ctx context.Context, snapshotURIs ...string) (map[string]*ateletpb.SignedObjectAccess, error) {
	if w.broker == nil {
		return nil, nil
	}
	return mintAccess(ctx, w.broker.MintRead, snapshotURIs...)
}

// writeAccessFor mints a write capability for each snapshot URI the node will
// write, returning them keyed by URI. With no broker configured it returns nil.
func (w *ActorWorkflow) writeAccessFor(ctx context.Context, snapshotURIs ...string) (map[string]*ateletpb.SignedObjectAccess, error) {
	if w.broker == nil {
		return nil, nil
	}
	return mintAccess(ctx, w.broker.MintWrite, snapshotURIs...)
}

// mintAccess mints one capability per distinct, non-empty URI with mint and maps
// each onto the wire message keyed by URI. It returns nil (not an empty map)
// when nothing was minted, so a caller can assign the result directly.
func mintAccess(ctx context.Context, mint func(context.Context, string, time.Duration) (storagebroker.Capability, error), snapshotURIs ...string) (map[string]*ateletpb.SignedObjectAccess, error) {
	out := make(map[string]*ateletpb.SignedObjectAccess, len(snapshotURIs))
	for _, uri := range snapshotURIs {
		if uri == "" || out[uri] != nil {
			continue
		}
		cap, err := mint(ctx, uri, snapshotCapabilityTTL)
		if err != nil {
			return nil, fmt.Errorf("minting snapshot capability for %q: %w", uri, err)
		}
		out[uri] = signedAccessFromCapability(cap)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// signedAccessFromCapability maps a broker capability onto the wire message
// atelet consumes. A read capability populates the read fields, a write
// capability the write fields; the rest stay empty.
func signedAccessFromCapability(cap storagebroker.Capability) *ateletpb.SignedObjectAccess {
	return &ateletpb.SignedObjectAccess{
		PrefixUrl:      cap.PrefixURL,
		ReadToken:      cap.ReadToken,
		ReadObjectUrls: cap.ReadObjectURLs,
		WriteMethod:    cap.WriteMethod,
		WriteToken:     cap.WriteToken,
		WriteHeaders:   cap.WriteHeaders,
		PostUrl:        cap.PostURL,
		PostFields:     cap.PostFields,
	}
}
