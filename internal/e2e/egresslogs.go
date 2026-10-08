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

package e2e

import (
	"os/exec"
	"strconv"
	"testing"

	"github.com/agent-substrate/substrate/internal/installdefaults"
)

// LogEgressGatewayOnFailure writes the egress gateway's recent logs to the
// test log once the test has failed. A test that asserts on what the gateway
// answered has nothing else to go on: the gateway's access log and its
// ext_proc decisions are the only record of why a request got the status it
// did, and CI keeps no pod logs. Register it after the fixtures so it runs
// before their cleanup.
func LogEgressGatewayOnFailure(t *testing.T, tailLines int) {
	t.Helper()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		args := []string{"-n", installdefaults.SystemNamespace, "logs", "deployment/atenet-egress",
			"--all-containers", "--prefix", "--tail=" + strconv.Itoa(tailLines)}
		if KubeContext != "" {
			args = append([]string{"--context=" + KubeContext}, args...)
		}
		out, err := exec.Command("kubectl", args...).CombinedOutput()
		if err != nil {
			t.Logf("egress gateway logs unavailable: kubectl %v: %v\n%s", args, err, out)
			return
		}
		t.Logf("egress gateway logs, last %d lines per container:\n%s", tailLines, out)
	})
}
