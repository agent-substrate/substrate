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

package authz

import (
	"context"
	"os"
	"slices"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"sigs.k8s.io/yaml"
)

// modelTestFile is in the format `fga model test` reads, so the same file
// also runs with the OpenFGA CLI.
const modelTestFile = "model_test.fga.yaml"

// modelTests is the subset of the `fga model test` format the file uses.
// Parsing is strict, so a test using any other part of the format (contextual
// tuples, list_objects, conditions, ...) fails here rather than being
// silently skipped.
type modelTests struct {
	Name      string       `json:"name"`
	ModelFile string       `json:"model_file"`
	Tuples    []modelTuple `json:"tuples"`
	Tests     []struct {
		Name  string `json:"name"`
		Check []struct {
			User       string          `json:"user"`
			Object     string          `json:"object"`
			Assertions map[string]bool `json:"assertions"`
		} `json:"check"`
	} `json:"tests"`
}

type modelTuple struct {
	User     string `json:"user"`
	Relation string `json:"relation"`
	Object   string `json:"object"`
}

// TestModelFGATests runs the checks in model_test.fga.yaml against model.fga
// as ate-api-server loads it.
func TestModelFGATests(t *testing.T) {
	data, err := os.ReadFile(modelTestFile)
	if err != nil {
		t.Fatalf("reading %s: %v", modelTestFile, err)
	}
	var mt modelTests
	if err := yaml.UnmarshalStrict(data, &mt); err != nil {
		t.Fatalf("parsing %s: %v", modelTestFile, err)
	}
	// The checks must run against the model ate-api-server embeds.
	if mt.ModelFile != "./model.fga" {
		t.Fatalf("%s: model_file = %q, want %q", modelTestFile, mt.ModelFile, "./model.fga")
	}
	if len(mt.Tests) == 0 {
		t.Fatalf("%s has no tests", modelTestFile)
	}

	ctx := context.Background()
	pool := startPostgres(t)
	fgaSrv, storeID, modelID := newTestAuthz(t, ctx, pool)

	// OpenFGA caps the tuples one Write may carry.
	const maxTuplesPerWrite = 100
	for chunk := range slices.Chunk(mt.Tuples, maxTuplesPerWrite) {
		keys := make([]*openfgav1.TupleKey, 0, len(chunk))
		for _, tk := range chunk {
			keys = append(keys, &openfgav1.TupleKey{User: tk.User, Relation: tk.Relation, Object: tk.Object})
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("pool.Begin failed: %v", err)
		}
		if _, err := fgaSrv.Write(ContextWithTx(ctx, tx), &openfgav1.WriteRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			Writes:               &openfgav1.WriteRequestWrites{TupleKeys: keys},
		}); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("writing tuples: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("tx.Commit failed: %v", err)
		}
	}

	for _, tc := range mt.Tests {
		t.Run(tc.Name, func(t *testing.T) {
			if len(tc.Check) == 0 {
				t.Fatal("test has no checks")
			}
			for _, c := range tc.Check {
				relations := make([]string, 0, len(c.Assertions))
				for r := range c.Assertions {
					relations = append(relations, r)
				}
				slices.Sort(relations)
				for _, relation := range relations {
					resp, err := fgaSrv.Check(ctx, &openfgav1.CheckRequest{
						StoreId:              storeID,
						AuthorizationModelId: modelID,
						TupleKey: &openfgav1.CheckRequestTupleKey{
							User:     c.User,
							Relation: relation,
							Object:   c.Object,
						},
					})
					if err != nil {
						t.Errorf("Check(user=%s, relation=%s, object=%s): %v", c.User, relation, c.Object, err)
						continue
					}
					if want := c.Assertions[relation]; resp.GetAllowed() != want {
						t.Errorf("Check(user=%s, relation=%s, object=%s) = %v, want %v", c.User, relation, c.Object, resp.GetAllowed(), want)
					}
				}
			}
		})
	}
}
