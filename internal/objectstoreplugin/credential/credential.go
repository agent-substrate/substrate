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

// Package credential holds the credential type the exchange hands out and
// its issuers produce. It is a leaf so an issuer does not depend on the
// handler that serves it.
package credential

import "time"

// Token is a storage credential and when it expires.
type Token struct {
	AccessToken string
	Expiry      time.Time
}

// MinServeLifetime is the least remaining lifetime a cached Token is served
// with, on both sides of the exchange, so a transfer never starts on a token
// about to expire. There is no mid-transfer refresh. A token with less left is
// still returned to the caller that asked for it, but not cached.
const MinServeLifetime = 20 * time.Minute
