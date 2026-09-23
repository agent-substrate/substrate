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

// Package oidcdiscovery implements the issuer side of OpenID Connect Discovery
// for actor JWTs.
package oidcdiscovery

import (
	"fmt"
	"net/url"
	"strings"
)

// ParseIssuer validates an issuer identifier and returns it in the form that
// goes in the iss claim and the discovery document. Relying parties compare
// issuers byte for byte, so the issuer must be a canonical https URL with a
// host and no user info, query, or fragment; a path is allowed. Trailing
// slashes are removed.
func ParseIssuer(raw string) (string, error) {
	issuer := strings.TrimRight(raw, "/")
	u, err := url.Parse(issuer)
	if err != nil {
		return "", fmt.Errorf("invalid issuer: %w", err)
	}
	switch {
	case u.Scheme != "https":
		return "", fmt.Errorf("issuer %q must use the https scheme", raw)
	case u.Hostname() == "":
		return "", fmt.Errorf("issuer %q has no host", raw)
	case u.User != nil:
		return "", fmt.Errorf("issuer %q must not contain user info", raw)
	case strings.Contains(issuer, "?"):
		return "", fmt.Errorf("issuer %q must not contain a query", raw)
	case strings.Contains(issuer, "#"):
		return "", fmt.Errorf("issuer %q must not contain a fragment", raw)
	case u.String() != issuer:
		return "", fmt.Errorf("issuer %q is not a canonical URL; did you mean %q?", raw, u.String())
	}
	return issuer, nil
}
