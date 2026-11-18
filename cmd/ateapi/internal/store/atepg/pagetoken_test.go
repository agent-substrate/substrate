// Copyright 2026 The Agent Substrate Authors
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

package atepg

import (
	"strings"
	"testing"
)

func TestDecodePageTokenRejectsWrongKeyShape(t *testing.T) {
	token := encodePageToken(kindActor, "", []string{"only-an-atespace"})
	if _, err := decodePageToken(token, kindActor, "", 2); err == nil {
		t.Fatal("decodePageToken() accepted a global actor token with only one key part")
	}
}

// TestEncodePageTokenFitsListOptions checks that a page token built from the
// longest names still fits ListOptions.page_token, whose maxLength is 256.
func TestEncodePageTokenFitsListOptions(t *testing.T) {
	const maxPageTokenLen = 256
	longName := strings.Repeat("a", 63)
	for _, kind := range []resourceKind{kindActor, kindActorTemplate, kindTag, kindExternalVolumeSnapshot} {
		for _, tc := range []struct {
			name  string
			scope string
			last  []string
		}{
			{name: "global", scope: "", last: []string{longName, longName}},
			{name: "atespace", scope: longName, last: []string{longName}},
		} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				if got := len(encodePageToken(kind, tc.scope, tc.last)); got > maxPageTokenLen {
					t.Errorf("page token is %d characters, want at most %d", got, maxPageTokenLen)
				}
			})
		}
	}
}
