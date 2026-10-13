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

package cmd

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// errPortForwardReset reproduces the client-go report from
// https://github.com/agent-substrate/substrate/issues/26, which surfaced after a
// successful command.
var errPortForwardReset = errors.New("error copying from local connection to remote stream: " +
	"read tcp4 127.0.0.1:42521->127.0.0.1:42648: read: connection reset by peer")

func restoreClientLogging(t *testing.T) {
	t.Helper()
	handlers := utilruntime.ErrorHandlers
	t.Cleanup(func() { utilruntime.ErrorHandlers = handlers })
}

func TestConfigureClientLogging(t *testing.T) {
	tests := []struct {
		name    string
		verbose bool
		want    string
	}{
		{name: "quiet by default"},
		{name: "verbose", verbose: true, want: "connection reset by peer"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			restoreClientLogging(t)

			var errOut bytes.Buffer
			configureClientLogging(&errOut, test.verbose)
			utilruntime.HandleError(errPortForwardReset)

			got := errOut.String()
			if test.want == "" {
				if got != "" {
					t.Errorf("stderr = %q, want no output", got)
				}
				return
			}
			if !strings.Contains(got, test.want) {
				t.Errorf("stderr = %q, want it to contain %q", got, test.want)
			}
		})
	}
}

func TestRootCommandConfiguresClientLogging(t *testing.T) {
	restoreClientLogging(t)

	verbose = false
	t.Cleanup(func() { verbose = false })

	var errOut bytes.Buffer
	rootCmd.SetErr(&errOut)
	t.Cleanup(func() { rootCmd.SetErr(nil) })

	if err := rootCmd.PersistentPreRunE(rootCmd, nil); err != nil {
		t.Fatalf("PersistentPreRunE: %v", err)
	}
	utilruntime.HandleError(errPortForwardReset)

	if errOut.Len() != 0 {
		t.Errorf("stderr = %q, want no output", errOut.String())
	}
}
