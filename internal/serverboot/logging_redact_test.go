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

package serverboot

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestInitLoggerRedactsSensitiveProtoFields pins the logger every component
// gets, not just the redaction package: a credential logged as a protobuf
// attribute must not reach the writer.
func TestInitLoggerRedactsSensitiveProtoFields(t *testing.T) {
	orig := slog.Default()
	t.Cleanup(func() {
		slog.SetDefault(orig)
	})

	var log bytes.Buffer
	InitLoggerWithWriter(&log)

	// Error rather than Info so the assertion holds whatever level another test
	// left behind.
	slog.Error("Handle RPC", slog.Any("resp", &ateapipb.MintActorJWTResponse{ActorJwt: "jwt-secret"}))

	if got := log.String(); strings.Contains(got, "jwt-secret") {
		t.Errorf("the serverboot logger leaked a credential: %s", got)
	}
}
