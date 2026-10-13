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
	"errors"

	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/pkg/objectstorage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// toStatus maps a plugin error to a gRPC status, keeping NotFound
// distinguishable so callers can probe for a snapshot.
func toStatus(err error) error {
	switch {
	case errors.Is(err, objectstorage.ErrObjectNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, objectstore.ErrEmptySource):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
