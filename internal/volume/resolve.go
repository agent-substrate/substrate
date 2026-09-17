// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package volume

import (
	"context"
	"errors"
	"fmt"
)

// LookupPlugin resolves a volume plugin for the given volume type using the provided resolver function.
// It returns an error wrapping the underlying failure if lookup fails, if resolver is nil, or if volumeType is empty.
// Wrapping with %w preserves underlying gRPC status codes (e.g. codes.NotFound).
func LookupPlugin[T any](ctx context.Context, resolver func(context.Context, string) (T, error), volumeType string) (T, error) {
	var zero T
	if resolver == nil {
		return zero, errors.New("plugin resolver is required")
	}
	if volumeType == "" {
		return zero, errors.New("volume type is required")
	}
	plugin, err := resolver(ctx, volumeType)
	if err != nil {
		return zero, fmt.Errorf("failed to get volume plugin for %q: %w", volumeType, err)
	}
	return plugin, nil
}
