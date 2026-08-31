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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxPageSize = 1000

// effectivePageSize applies the server-chosen default for an unset page_size
// and silently coerces oversized values.
func effectivePageSize(requested int32) int32 {
	if requested == 0 || requested > maxPageSize {
		return maxPageSize
	}
	return requested
}

func mapListError(err error) error {
	if errors.Is(err, store.ErrInvalidPageToken) {
		return status.Error(codes.InvalidArgument, "invalid page_token")
	}
	if errors.Is(err, store.ErrInvalidPageSize) {
		return status.Error(codes.InvalidArgument, "invalid page_size")
	}
	return err
}
