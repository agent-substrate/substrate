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

// Package volumepath holds the rule for the relative paths at which substrate
// projects files into an actor's volumes. ateapi applies it when an
// ActorTemplate is created and atelet applies it again before writing to the
// host filesystem; both call this one implementation so the two cannot drift.
package volumepath

import (
	"errors"
	"fmt"
	"strings"
)

// MaxSegments bounds the depth of a projected path.
const MaxSegments = 16

// ValidateProjected reports why p is not acceptable as the path of a file
// projected into a volume, relative to the volume root. p must be non-empty,
// contain no NUL byte, and split on '/' into at most MaxSegments segments,
// none of which is "", "." or "..". Any other byte is allowed: Linux paths
// are bytes, not text, so Unicode in any encoding passes.
func ValidateProjected(p string) error {
	if p == "" {
		return errors.New("must not be empty")
	}
	if strings.IndexByte(p, 0) >= 0 {
		return errors.New("must not contain a NUL byte")
	}
	segments := strings.Split(p, "/")
	if len(segments) > MaxSegments {
		return fmt.Errorf("must have at most %d path segments", MaxSegments)
	}
	for _, segment := range segments {
		switch segment {
		case "":
			return errors.New("must be a relative path with no empty segments: no leading, trailing, or doubled '/'")
		case ".", "..":
			return fmt.Errorf("must not contain a %q path segment", segment)
		}
	}
	return nil
}
