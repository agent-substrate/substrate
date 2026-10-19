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

package main

import (
	"testing"

	"github.com/spf13/pflag"
)

// ate-setup passes --preview to every substrate binary it installs; an
// undefined flag makes the binary exit at start.
func TestDefinesPreviewFlag(t *testing.T) {
	f := pflag.CommandLine.Lookup("preview")
	if f == nil {
		t.Fatal("no --preview flag")
	}
	if err := f.Value.Set(""); err != nil {
		t.Errorf("--preview= as ate-setup passes it: %v", err)
	}
}
