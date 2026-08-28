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

package kube

import (
	"bytes"
	"context"
	"encoding/pem"
	"fmt"

	"github.com/agent-substrate/substrate/internal/localca"
)

// CAPoolRootPEM extracts a CA pool Secret's root certificates and returns them
// PEM-encoded.
//
// This replaces ca_pool_root_pem in the shell installer, which piped the
// Secret through jsonpath, base64, grep, sed, and openssl. Decoding the pool
// with the same library that wrote it removes the whole toolchain dependency
// and, more importantly, turns a malformed or empty pool into an error instead
// of an empty string that silently became a trust bundle with no roots.
func (c *Client) CAPoolRootPEM(ctx context.Context, namespace, name string) ([]byte, error) {
	secret, err := c.GetSecret(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, fmt.Errorf("CA pool secret %s/%s not found", namespace, name)
	}

	poolBytes, ok := secret.Data["pool"]
	if !ok {
		return nil, fmt.Errorf("CA pool secret %s/%s has no \"pool\" key", namespace, name)
	}

	pool, err := localca.Unmarshal(poolBytes)
	if err != nil {
		return nil, fmt.Errorf("while decoding CA pool %s/%s: %w", namespace, name, err)
	}
	if len(pool.CAs) == 0 {
		return nil, fmt.Errorf("CA pool %s/%s contains no CAs", namespace, name)
	}

	var buf bytes.Buffer
	for _, ca := range pool.CAs {
		if ca.RootCertificate == nil {
			return nil, fmt.Errorf("CA %q in pool %s/%s has no root certificate", ca.ID, namespace, name)
		}
		if err := pem.Encode(&buf, &pem.Block{
			Type:  "CERTIFICATE",
			Bytes: ca.RootCertificate.Raw,
		}); err != nil {
			return nil, fmt.Errorf("while PEM-encoding the root of %s/%s: %w", namespace, name, err)
		}
	}
	return buf.Bytes(), nil
}
