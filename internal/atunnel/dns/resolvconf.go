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

// Package dns answers an actor's DNS from its sandbox's gateway namespace.
package dns

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

// resolvConfNameservers reads nameservers from resolv.conf as "host:53".
func resolvConfNameservers(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("dns: reading resolv.conf: %w", err)
	}
	defer f.Close()

	var out []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if i := strings.IndexAny(line, "#;"); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		rest, ok := strings.CutPrefix(line, "nameserver")
		if !ok {
			continue
		}
		address := strings.TrimSpace(rest)
		if address == "" || net.ParseIP(address) == nil {
			continue
		}
		out = append(out, net.JoinHostPort(address, "53"))
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("dns: reading resolv.conf: %w", err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("dns: %s names no usable nameserver", path)
	}
	return out, nil
}
