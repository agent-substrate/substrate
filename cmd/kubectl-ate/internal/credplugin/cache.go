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

package credplugin

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Cache keeps tokens on disk so that consecutive CLI invocations, each a new
// plugin process, reuse one token instead of minting one per command.
type Cache struct {
	Dir string
	Now func() time.Time
}

// DefaultCache is the cache under ~/.ate/cache.
func DefaultCache() (*Cache, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	return &Cache{Dir: filepath.Join(home, ".ate", "cache", "credentials"), Now: time.Now}, nil
}

type cacheEntry struct {
	Token  string    `json:"token"`
	Expiry time.Time `json:"expiry"`
}

// path names the file for the token identified by keyParts.
func (c *Cache) path(keyParts []string) string {
	sum := sha256.Sum256([]byte(strings.Join(keyParts, "\x00")))
	return filepath.Join(c.Dir, hex.EncodeToString(sum[:])+".json")
}

// Get returns the cached token for keyParts if it is valid for at least
// minRemaining.
func (c *Cache) Get(keyParts []string, minRemaining time.Duration) (string, time.Time, bool) {
	b, err := os.ReadFile(c.path(keyParts))
	if err != nil {
		return "", time.Time{}, false
	}
	var e cacheEntry
	if err := json.Unmarshal(b, &e); err != nil || e.Token == "" || c.Now().Add(minRemaining).After(e.Expiry) {
		return "", time.Time{}, false
	}
	return e.Token, e.Expiry, true
}

// Put caches token for keyParts. The file is mode 0600 in a 0700 directory.
func (c *Cache) Put(keyParts []string, token string, expiry time.Time) error {
	b, err := json.Marshal(cacheEntry{Token: token, Expiry: expiry})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(c.Dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(c.Dir, ".token-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), c.path(keyParts))
}
