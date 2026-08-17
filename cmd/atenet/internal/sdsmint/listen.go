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

// This file holds the only socket the process opens: the SDS one, which is
// always a unix domain socket because leaf private keys transit it.

package sdsmint

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
)

// listen binds the SDS socket. There is no TCP alternative on purpose: leaf
// private keys transit this channel, and a unix socket restricted to the
// proxy's UID is the only reach that is ever wanted.
func listen(uds string) (net.Listener, error) {
	if err := os.Remove(uds); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("removing stale socket %s: %w", uds, err)
	}
	if err := os.MkdirAll(filepath.Dir(uds), 0o755); err != nil {
		return nil, fmt.Errorf("creating socket directory: %w", err)
	}
	lis, err := net.Listen("unix", uds)
	if err != nil {
		return nil, fmt.Errorf("listening on %s: %w", uds, err)
	}
	// Only the proxy should be able to ask for certificates.
	if err := os.Chmod(uds, 0o600); err != nil {
		lis.Close()
		return nil, fmt.Errorf("restricting socket permissions: %w", err)
	}
	return lis, nil
}
