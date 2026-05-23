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

package storetest

import (
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/ateredis"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// SetupTestStore starts a miniredis server and returns a real store implementation
// backed by it, along with a cleanup function.
func SetupTestStore(t *testing.T) (store.Interface, func()) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{mr.Addr()},
	})

	persistence := ateredis.NewPersistence(rdb)

	cleanup := func() {
		mr.Close()
	}

	return persistence, cleanup
}
