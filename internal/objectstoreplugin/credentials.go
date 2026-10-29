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

package objectstoreplugin

import (
	"context"
	"net/http"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/pkg/objectstorage"
)

// Credentials supplies the storage client a snapshot transfer runs with. The
// node plugin asks once per transfer with the actor JWT the request carries,
// and again with force set after storage refused a request, which means the
// credential behind the client no longer covers the actor. The GCS source is
// exchangeclient, which trades the JWT for a token scoped to the actor. The
// S3 source is Ambient, which ignores the JWT.
type Credentials interface {
	// Client returns the client for the actor actorJWT names.
	Client(ctx context.Context, actorJWT string, force bool) (objectstorage.ObjectStorage, error)
}

// Ambient returns Credentials that hand out client for every transfer, the
// pod's own credential, whatever JWT the request carries.
func Ambient(client objectstorage.ObjectStorage) Credentials {
	return ambient{client: client}
}

type ambient struct{ client objectstorage.ObjectStorage }

func (a ambient) Client(context.Context, string, bool) (objectstorage.ObjectStorage, error) {
	return a.client, nil
}

// refused reports whether storage turned a request down for lack of
// authorization. With a per-actor credential that means the credential is
// stale: the actor's prefixes changed, or the token expired mid-transfer.
func refused(err error) bool {
	desc, ok := objectstorage.DescribeServiceError(err)
	return ok && (desc.HTTPStatus == http.StatusUnauthorized || desc.HTTPStatus == http.StatusForbidden)
}

// transfer is one FetchSnapshot or UploadSnapshot: the client its object
// requests run with, obtained on first use, and replaced when a request is
// refused. Object requests run concurrently, so a replacement one of them
// forces serves the others.
type transfer struct {
	creds    Credentials
	actorJWT string
	budget   *objectstorage.Uploader

	mu     sync.Mutex
	client objectstorage.ObjectStorage
	gen    int
}

func (p *NodePlugin) begin(actorJWT string) *transfer {
	return &transfer{creds: p.creds, actorJWT: actorJWT, budget: p.uploader}
}

// obtainLocked replaces the transfer's client with a new one from creds. A
// NotFound or Aborted from the source means the actor is not this node's to
// transfer for, gone, recreated or elsewhere. It is reported as
// FailedPrecondition, since atelet reads NotFound from a transfer as a
// missing snapshot object.
func (t *transfer) obtainLocked(ctx context.Context, force bool) error {
	client, err := t.creds.Client(ctx, t.actorJWT, force)
	switch status.Code(err) {
	case codes.NotFound, codes.Aborted:
		return status.Errorf(codes.FailedPrecondition, "the actor cannot be transferred from this node: %v", err)
	}
	if err != nil {
		return err
	}
	t.client, t.gen = client, t.gen+1
	return nil
}

// current returns the transfer's client, obtaining it on first use, and the
// generation it belongs to.
func (t *transfer) current(ctx context.Context) (objectstorage.ObjectStorage, int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.client == nil {
		if err := t.obtainLocked(ctx, false); err != nil {
			return nil, 0, err
		}
	}
	return t.client, t.gen, nil
}

// replace returns a client newer than generation gen: the current one if
// another request already replaced it, otherwise a fresh one forced from
// creds.
func (t *transfer) replace(ctx context.Context, gen int) (objectstorage.ObjectStorage, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.gen == gen {
		if err := t.obtainLocked(ctx, true); err != nil {
			return nil, err
		}
	}
	return t.client, nil
}

// do runs one object request. A request storage refuses is retried once with
// a replaced client. A second refusal is final and means the actor's
// credential does not cover the request, which the caller sees as
// PermissionDenied.
func (t *transfer) do(ctx context.Context, op func(client objectstorage.ObjectStorage, uploader *objectstorage.Uploader) error) error {
	client, gen, err := t.current(ctx)
	if err != nil {
		return err
	}
	if err := op(client, t.budget.WithClient(client)); !refused(err) {
		return err
	}
	client, err = t.replace(ctx, gen)
	if err != nil {
		return err
	}
	if err := op(client, t.budget.WithClient(client)); refused(err) {
		return status.Errorf(codes.PermissionDenied, "storage refused the request with the actor's credential: %v", err)
	} else {
		return err
	}
}
