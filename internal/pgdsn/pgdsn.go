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

// Package pgdsn checks PostgreSQL connection strings for inline secrets.
//
// ate-api-server takes its connection strings as flags, which show up in the
// pod spec, the process table, and the startup log. A password therefore has
// to come from a file named by the passfile parameter instead, and a client key
// has to be unencrypted or decrypted by other means than sslpassword.
package pgdsn

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// secretKeys are the libpq connection parameters whose value is itself a
// secret. See
// https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS.
var secretKeys = []string{"password", "sslpassword"}

// RejectInlineSecrets returns an error if dsn, in URI or keyword/value form,
// carries a password or a client key passphrase. The error never includes
// the value. A dsn that does not parse is also an error, so a caller cannot
// mistake a string it failed to inspect for a clean one.
func RejectInlineSecrets(dsn string) error {
	keys, err := connStringKeys(dsn)
	if err != nil {
		return err
	}
	for _, k := range secretKeys {
		if keys[k] {
			return fmt.Errorf("PostgreSQL connection string sets %q inline; supply the password through a file named by the passfile parameter instead", k)
		}
	}
	return nil
}

// connStringKeys returns the parameter keywords dsn sets, following pgx's
// parsing so that the check sees the same keys the driver does.
func connStringKeys(dsn string) (map[string]bool, error) {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		return urlKeys(dsn)
	}
	return keywordValueKeys(dsn)
}

func urlKeys(dsn string) (map[string]bool, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		// url.Error quotes the input, which may hold a password.
		return nil, errors.New("PostgreSQL connection string does not parse as a URI")
	}
	keys := map[string]bool{}
	if u.User != nil {
		if _, ok := u.User.Password(); ok {
			keys["password"] = true
		}
	}
	for k := range u.Query() {
		keys[k] = true
	}
	return keys, nil
}

const space = " \t\n\r\v\f"

func keywordValueKeys(s string) (map[string]bool, error) {
	errInvalid := errors.New("PostgreSQL connection string does not parse as keyword/value pairs")
	keys := map[string]bool{}
	s = strings.TrimLeft(s, space)
	for len(s) > 0 {
		eq := strings.IndexByte(s, '=')
		if eq < 0 {
			return nil, errInvalid
		}
		key := strings.Trim(s[:eq], space)
		if key == "" {
			return nil, errInvalid
		}
		keys[key] = true
		s = strings.TrimLeft(s[eq+1:], space)
		switch {
		case len(s) == 0:
		case s[0] != '\'':
			end := 0
			for ; end < len(s) && !strings.ContainsRune(space, rune(s[end])); end++ {
				if s[end] == '\\' {
					end++
					if end == len(s) {
						return nil, errInvalid
					}
				}
			}
			s = strings.TrimLeft(s[end:], space)
		default:
			s = s[1:]
			end := 0
			for ; end < len(s) && s[end] != '\''; end++ {
				if s[end] == '\\' {
					end++
				}
			}
			if end >= len(s) {
				return nil, errInvalid
			}
			s = strings.TrimLeft(s[end+1:], space)
		}
	}
	return keys, nil
}
