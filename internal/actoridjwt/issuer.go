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

package actoridjwt

import (
	"fmt"
	"net/url"
)

// ValidateIssuer returns an error unless issuer is https:// followed by a host
// and optional port, with nothing after it. ate-idp-server serves the
// discovery document and key set at fixed paths under such an issuer.
func ValidateIssuer(issuer string) error {
	u, err := url.Parse(issuer)
	if err != nil {
		return fmt.Errorf("invalid issuer: %w", err)
	}
	if u.Hostname() == "" || issuer != "https://"+u.Host {
		return fmt.Errorf("issuer %q must be https://<host>, with no path, query, or fragment", issuer)
	}
	return nil
}
