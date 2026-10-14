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

// Package runtimegoldenenv defines when runtime golden regression tests are required.
package runtimegoldenenv

import (
	"fmt"
	"os"
)

func Required() bool { return os.Getenv("CI") == "true" && os.Getenv("E2E_SANDBOX_CLASS") == "microvm" }
func Enabled() bool  { return Required() || os.Getenv("E2E_RUNTIME_GOLDEN") == "true" }
func Check() error {
	if !Enabled() {
		return nil
	}
	if os.Getenv("E2E_SANDBOX_CLASS") != "microvm" {
		return fmt.Errorf("runtime golden tests require microvm")
	}
	if os.Getenv("E2E_RUNTIME_LOG_DIR") == "" {
		return fmt.Errorf("runtime golden tests require E2E_RUNTIME_LOG_DIR")
	}
	return nil
}
