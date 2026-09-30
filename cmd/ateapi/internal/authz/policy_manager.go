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
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/server"
)

// PolicyManager manages authorization tuple writes and lifecycle cleanup in OpenFGA.
type PolicyManager struct {
	fgaServer *server.Server
	storeID   string
	modelID   string
}

// DeleteAtespacePolicies removes all tuples associated with an atespace in OpenFGA
// in batches of maxTuplesPerWrite.
//
// Callers (such as atepg.DeleteAtespace) must pass a ctx carrying an active pgx.Tx
// via ContextWithTx(ctx, tx). Both m.fgaServer.Read (which dispatches to
// transactionalDatastore.ReadPage) and m.fgaServer.Write (which dispatches to
// transactionalDatastore.Write) extract and execute on that pgx.Tx, failing with
// ErrNoTransactionInContext if no transaction is present in ctx.
func (m *PolicyManager) DeleteAtespacePolicies(ctx context.Context, name string) error {
	if m == nil {
		return nil
	}
	obj := AtespaceObject(name)
	var toDelete []*openfgav1.TupleKeyWithoutCondition
	var contToken string
	for {
		readResp, err := m.fgaServer.Read(ctx, &openfgav1.ReadRequest{
			StoreId:           m.storeID,
			TupleKey:          &openfgav1.ReadRequestTupleKey{Object: obj},
			ContinuationToken: contToken,
		})
		if err != nil {
			return fmt.Errorf("reading tuples for deleted atespace %q: %w", name, err)
		}
		for _, t := range readResp.GetTuples() {
			if tk := t.GetKey(); tk != nil {
				toDelete = append(toDelete, &openfgav1.TupleKeyWithoutCondition{
					User:     tk.GetUser(),
					Relation: tk.GetRelation(),
					Object:   tk.GetObject(),
				})
			}
		}
		if readResp.GetContinuationToken() == "" {
			break
		}
		contToken = readResp.GetContinuationToken()
	}
	for i := 0; i < len(toDelete); i += maxTuplesPerWrite {
		end := i + maxTuplesPerWrite
		if end > len(toDelete) {
			end = len(toDelete)
		}
		_, err := m.fgaServer.Write(ctx, &openfgav1.WriteRequest{
			StoreId:              m.storeID,
			AuthorizationModelId: m.modelID,
			Deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: toDelete[i:end],
				OnMissing: "ignore",
			},
		})
		if err != nil {
			return fmt.Errorf("deleting tuples for atespace %q: %w", name, err)
		}
	}
	return nil
}
