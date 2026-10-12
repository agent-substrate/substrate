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

package objectstorage

import "context"

// A large transfer buffers far more than its object's share of the process: an
// upload holds the parallel compressor's chunks plus up to uploadConcurrency+1 parts
// of uploadPartSize (~200 MiB for a micro-VM memory image), a ranged download
// downloadConcurrency chunks of downloadChunkSize (128 MiB). Each one already spreads
// over every core and several connections, so running more of them at once adds
// memory and no throughput: a node whose actors all suspend together would otherwise
// buffer one image per actor and outgrow its memory limit.
//
// maxLargeTransfers bounds how many run at once in the process; the rest wait for a
// slot. Objects of at most one part (configs, manifests, small tars) never wait.
const maxLargeTransfers = 2

// largeTransfers holds one token per running large transfer.
var largeTransfers = make(chan struct{}, maxLargeTransfers)

// acquireLargeTransfer blocks until a large transfer may start or ctx ends. The
// caller calls release exactly once when its buffers are no longer in use.
func acquireLargeTransfer(ctx context.Context) (release func(), err error) {
	select {
	case largeTransfers <- struct{}{}:
		return func() { <-largeTransfers }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
