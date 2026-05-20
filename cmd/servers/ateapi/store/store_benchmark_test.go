//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package store_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"
	"github.com/redis/go-redis/v9"

	"github.com/agent-substrate/substrate/cmd/servers/ateapi/store"
	"github.com/agent-substrate/substrate/cmd/servers/ateapi/store/ateaerospike"
	"github.com/agent-substrate/substrate/cmd/servers/ateapi/store/ateredis"
	"github.com/agent-substrate/substrate/proto/ateapipb"
)

type standaloneRedisWrapper struct {
	*redis.Client
}

func (w standaloneRedisWrapper) ForEachMaster(ctx context.Context, fn func(ctx context.Context, client *redis.Client) error) error {
	return fn(ctx, w.Client)
}

func setupRedis(b *testing.B) (store.Interface, func()) {
	ctx := context.Background()

	// 1. Try Standalone Redis first (common for local dev)
	standaloneClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	if err := standaloneClient.Ping(ctx).Err(); err == nil {
		s := ateredis.NewPersistence(standaloneRedisWrapper{Client: standaloneClient})
		_ = s.DebugClearAll(ctx)
		cleanup := func() {
			standaloneClient.Close()
		}
		return s, cleanup
	}

	// 2. Fallback to Clustered Redis
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{"localhost:6379"},
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		b.Skipf("Skipping Redis benchmark: local Redis server not running at localhost:6379: %v", err)
	}

	s := ateredis.NewPersistence(rdb)
	_ = s.DebugClearAll(ctx)

	cleanup := func() {
		rdb.Close()
	}
	return s, cleanup
}

func setupAerospike(b *testing.B) (store.Interface, func()) {
	// Aerospike connection
	client, err := as.NewClient("localhost", 3000)
	if err != nil {
		b.Skipf("Skipping Aerospike benchmark: local Aerospike server not running at localhost:3000: %v", err)
	}

	s := ateaerospike.NewPersistence(client, "test")
	ctx := context.Background()
	_ = s.DebugClearAll(ctx)

	cleanup := func() {
		client.Close()
	}
	return s, cleanup
}

func runCreateActorBenchmark(b *testing.B, s store.Interface) {
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		actor := &ateapipb.Actor{
			ActorId:                fmt.Sprintf("bench-actor-%d", i),
			ActorTemplateNamespace: "default",
			ActorTemplateName:      "bench-template",
			Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		}
		err := s.CreateActor(ctx, actor)
		if err != nil {
			b.Fatalf("failed to create actor: %v", err)
		}
	}
}

func runGetActorBenchmark(b *testing.B, s store.Interface) {
	ctx := context.Background()
	actor := &ateapipb.Actor{
		ActorId:                "bench-actor-get",
		ActorTemplateNamespace: "default",
		ActorTemplateName:      "bench-template",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
	}
	_ = s.CreateActor(ctx, actor)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.GetActor(ctx, actor.ActorId)
		if err != nil {
			b.Fatalf("failed to get actor: %v", err)
		}
	}
}

func runLockBenchmark(b *testing.B, s store.Interface) {
	ctx := context.Background()
	key := "bench-lock"
	value := "token"
	ttl := 10 * time.Second

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		acquired, err := s.AcquireLock(ctx, key, value, ttl)
		if err != nil {
			b.Fatalf("failed to acquire lock: %v", err)
		}
		if !acquired {
			b.Fatalf("failed to acquire lock (conflict)")
		}
		err = s.ReleaseLock(ctx, key, value)
		if err != nil {
			b.Fatalf("failed to release lock: %v", err)
		}
	}
}

// --- Actor Creation Benchmarks ---

func BenchmarkCreateActor_Redis(b *testing.B) {
	s, cleanup := setupRedis(b)
	defer cleanup()
	runCreateActorBenchmark(b, s)
}

func BenchmarkCreateActor_Aerospike(b *testing.B) {
	s, cleanup := setupAerospike(b)
	defer cleanup()
	runCreateActorBenchmark(b, s)
}

// --- Actor Read Benchmarks ---

func BenchmarkGetActor_Redis(b *testing.B) {
	s, cleanup := setupRedis(b)
	defer cleanup()
	runGetActorBenchmark(b, s)
}

func BenchmarkGetActor_Aerospike(b *testing.B) {
	s, cleanup := setupAerospike(b)
	defer cleanup()
	runGetActorBenchmark(b, s)
}

// --- Lock Acquisition/Release Benchmarks ---

func BenchmarkLock_Redis(b *testing.B) {
	s, cleanup := setupRedis(b)
	defer cleanup()
	runLockBenchmark(b, s)
}

func BenchmarkLock_Aerospike(b *testing.B) {
	s, cleanup := setupAerospike(b)
	defer cleanup()
	runLockBenchmark(b, s)
}
