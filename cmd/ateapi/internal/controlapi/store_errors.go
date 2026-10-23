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
	"errors"
	"fmt"

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
)

// MapStoreError converts a store sentinel in err to the apierror its caller
// receives. The format and args name the resource the store call addressed,
// such as "Actor %s". It returns err unchanged if err is nil, already an
// apierror or a context error, or not a store sentinel.
//
// Check for a sentinel that means something more specific to the caller
// before calling MapStoreError. For example, store.ErrFailedPrecondition means
// a missing parent on create and a non-empty atespace on delete.
func MapStoreError(err error, format string, args ...any) error {
	if _, ok := apierror.FromError(err); ok {
		return err
	}
	what := fmt.Sprintf(format, args...)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return apierror.NotFound("%s not found", what)
	case errors.Is(err, store.ErrAlreadyExists):
		return apierror.AlreadyExists("%s already exists", what)
	case errors.Is(err, store.ErrVersionConflict):
		return apierror.Aborted("concurrent update conflict, please retry")
	case errors.Is(err, store.ErrFailedPrecondition):
		return apierror.FailedPrecondition("precondition failed for %s", what)
	case errors.Is(err, store.ErrUIDConflict):
		return apierror.Aborted("%s has a different uid", what)
	case errors.Is(err, store.ErrPreconditionRequired):
		return apierror.InvalidArgument("updating %s requires uid and version", what)
	case errors.Is(err, store.ErrImmutableField):
		return apierror.InvalidArgument("while updating %s: %v", what, err)
	case errors.Is(err, store.ErrLeaseConflict):
		return apierror.Aborted("another operation is in progress for %s", what)
	case errors.Is(err, store.ErrInvalidPageToken):
		return apierror.InvalidArgument("invalid page_token")
	case errors.Is(err, store.ErrInvalidPageSize):
		return apierror.InvalidArgument("invalid page_size")
	default:
		return err
	}
}
