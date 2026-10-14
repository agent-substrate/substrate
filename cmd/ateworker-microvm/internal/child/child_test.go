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

package child

import (
	"os/exec"
	"testing"
	"time"
)

func TestReapedWhenItDiesOnItsOwn(t *testing.T) {
	p, err := Start(exec.Command("sh", "-c", "kill -KILL $$"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("process was not reaped")
	}
	if p.Stopped() {
		t.Error("Stopped() = true for a process that died on its own")
	}
	if got, want := p.Exit(), "signal: killed"; got != want {
		t.Errorf("Exit() = %q, want %q", got, want)
	}
}

func TestStop(t *testing.T) {
	p, err := Start(exec.Command("sleep", "30"))
	if err != nil {
		t.Fatal(err)
	}
	p.Stop()
	select {
	case <-p.Done():
	default:
		t.Fatal("Stop returned before the process was reaped")
	}
	if !p.Stopped() {
		t.Error("Stopped() = false after Stop")
	}
	// a second Stop on a reaped process must not block or signal anything
	p.Stop()
}
