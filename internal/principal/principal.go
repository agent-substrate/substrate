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

package principal

import "context"

// PrincipalInfo contains information about an authenticated principal.
type PrincipalInfo struct {
	ID string
}

type contextKey struct{}

var principalKey = contextKey{}

// FromContext returns the PrincipalInfo from the context, if any.
func FromContext(ctx context.Context) (PrincipalInfo, bool) {
	p, ok := ctx.Value(principalKey).(PrincipalInfo)
	return p, ok
}

// InjectContext returns a new context with the given PrincipalInfo.
func InjectContext(ctx context.Context, p PrincipalInfo) context.Context {
	return context.WithValue(ctx, principalKey, p)
}
