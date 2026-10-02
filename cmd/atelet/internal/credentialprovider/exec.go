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

package credentialprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	credentialproviderv1 "k8s.io/kubelet/pkg/apis/credentialprovider/v1"
)

// execTimeout bounds one plugin invocation, generously: it exists so a wedged
// plugin surfaces as a failed pull rather than a stuck actor activation.
const execTimeout = 1 * time.Minute

// maxResponseBytes caps the plugin's stdout. Credentials are a few kilobytes;
// the limit keeps a misbehaving binary from ballooning atelet's heap.
const maxResponseBytes = 1 << 20

// exec runs the plugin once for image and returns its parsed response. The
// protocol is the kubelet's: a CredentialProviderRequest as JSON on stdin, a
// CredentialProviderResponse as JSON on stdout.
func (p *plugin) exec(ctx context.Context, image string) (*credentialproviderv1.CredentialProviderResponse, error) {
	req := &credentialproviderv1.CredentialProviderRequest{
		TypeMeta: metav1.TypeMeta{
			Kind:       "CredentialProviderRequest",
			APIVersion: supportedAPIVersion,
		},
		Image: image,
	}
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("while encoding credential provider request: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()

	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, p.path, p.args...)
	cmd.Stdin = bytes.NewReader(reqJSON)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	// Union with the process env, as the kubelet does: providers need inherited
	// settings (proxy vars, cloud SDK config) to reach their metadata service.
	cmd.Env = append(os.Environ(), p.env...)

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("while running credential provider %q: %w (stderr: %s)", p.name, err, truncate(stderr.String()))
	}
	if stdout.Len() > maxResponseBytes {
		return nil, fmt.Errorf("credential provider %q returned %d bytes, over the %d byte limit", p.name, stdout.Len(), maxResponseBytes)
	}

	var resp credentialproviderv1.CredentialProviderResponse
	if err := json.Unmarshal(stdout.Bytes(), &resp); err != nil {
		return nil, fmt.Errorf("while decoding response from credential provider %q: %w (stderr: %s)", p.name, err, truncate(stderr.String()))
	}
	// A response encoded at a version we did not ask for may have different
	// field semantics, so refuse it rather than misread the credentials.
	if resp.APIVersion != supportedAPIVersion {
		return nil, fmt.Errorf("credential provider %q responded with apiVersion %q, want %q", p.name, resp.APIVersion, supportedAPIVersion)
	}
	return &resp, nil
}

// truncate bounds plugin stderr so a chatty provider cannot flood atelet's
// logs through an error message.
func truncate(s string) string {
	const limit = 2048
	s = strings.TrimSpace(s)
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "... (truncated)"
}
