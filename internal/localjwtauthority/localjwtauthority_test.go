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

package localjwtauthority

import (
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestRefreshingPool(t *testing.T) {
	ca1, err := GenerateECDSAP256Authority("1")
	if err != nil {
		t.Fatalf("Unexpected error generating CA 1: %v", err)
	}
	pool1 := &ConcretePool{
		Authorities:      []*Authority{ca1},
		ActiveForSigning: "1",
	}
	pool1Bytes, err := Marshal(pool1)
	if err != nil {
		t.Fatalf("Unexpected error marshaling pool 1: %v", err)
	}

	ca2, err := GenerateECDSAP256Authority("2")
	if err != nil {
		t.Fatalf("Unexpected error generating CA 2: %v", err)
	}
	pool2 := &ConcretePool{
		Authorities:      []*Authority{ca1, ca2},
		ActiveForSigning: "1",
	}
	pool2Bytes, err := Marshal(pool2)
	if err != nil {
		t.Fatalf("Unexpected error marshaling pool 2: %v", err)
	}

	pool3 := &ConcretePool{
		Authorities:      []*Authority{ca1, ca2},
		ActiveForSigning: "2",
	}
	pool3Bytes, err := Marshal(pool3)
	if err != nil {
		t.Fatalf("Unexpected error marshaling pool 2: %v", err)
	}

	pool4 := &ConcretePool{
		Authorities:      []*Authority{ca2},
		ActiveForSigning: "2",
	}
	pool4Bytes, err := Marshal(pool4)
	if err != nil {
		t.Fatalf("Unexpected error marshaling pool 2: %v", err)
	}

	synctest.Test(t, func(t *testing.T) {
		tempDir := t.TempDir()
		poolFile := filepath.Join(tempDir, "pool.json")

		if err := os.WriteFile(poolFile, pool1Bytes, 0o600); err != nil {
			t.Fatalf("Unexpected error writing pool 1: %v", err)
		}

		refreshingPool, err := NewRefreshingPool(poolFile)
		if err != nil {
			t.Fatalf("Unexpected error creating refreshing pool: %v", err)
		}

		gotVerificationKeys, err := refreshingPool.VerificationKeys()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from refreshing pool: %v", err)
		}
		wantVerificationKeys, err := pool1.VerificationKeys()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from pool 1: %v", err)
		}
		if diff := cmp.Diff(gotVerificationKeys, wantVerificationKeys); diff != "" {
			t.Fatalf("Refreshing pool returned wrong trust anchors; diff (-got +want)\n%s", diff)
		}

		// Write pool2 and advance past the cache threshold.
		if err := os.WriteFile(poolFile, pool2Bytes, 0o600); err != nil {
			t.Fatalf("Unexpected error writing pool 2: %v", err)
		}
		time.Sleep(61 * time.Second)

		gotVerificationKeys, err = refreshingPool.VerificationKeys()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from refreshing pool: %v", err)
		}
		wantVerificationKeys, err = pool2.VerificationKeys()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from pool 2: %v", err)
		}
		if diff := cmp.Diff(gotVerificationKeys, wantVerificationKeys); diff != "" {
			t.Fatalf("Refreshing pool returned wrong trust anchors after file update 2; diff (-got +want)\n%s", diff)
		}

		// Write pool3 and advance past the cache threshold.
		if err := os.WriteFile(poolFile, pool3Bytes, 0o600); err != nil {
			t.Fatalf("Unexpected error writing pool 3: %v", err)
		}
		time.Sleep(61 * time.Second)
		gotVerificationKeys, err = refreshingPool.VerificationKeys()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from refreshing pool: %v", err)
		}
		wantVerificationKeys, err = pool3.VerificationKeys()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from pool 3: %v", err)
		}
		if diff := cmp.Diff(gotVerificationKeys, wantVerificationKeys); diff != "" {
			t.Fatalf("Refreshing pool returned wrong trust anchors after file update 3; diff (-got +want)\n%s", diff)
		}

		// Write pool4 and advance past the cache threshold.
		if err := os.WriteFile(poolFile, pool4Bytes, 0o600); err != nil {
			t.Fatalf("Unexpected error writing pool 4: %v", err)
		}
		time.Sleep(61 * time.Second)
		gotVerificationKeys, err = refreshingPool.VerificationKeys()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from refreshing pool: %v", err)
		}
		wantVerificationKeys, err = pool4.VerificationKeys()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from pool 4: %v", err)
		}
		if diff := cmp.Diff(gotVerificationKeys, wantVerificationKeys); diff != "" {
			t.Fatalf("Refreshing pool returned wrong trust anchors after file update; diff (-got +want)\n%s", diff)
		}
	})
}
