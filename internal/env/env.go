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

// Package env declares environment settings beside their consumers. The envdoc
// tool collects these declarations without executing code or reading values.
package env

import (
	"os"
	"strconv"
)

// Var describes a setting. Default is used when the variable is unset;
// an explicitly empty value is passed through the same parser as any other value.
// String values are returned unchanged. Booleans use strconv.ParseBool and fall
// back to Default on invalid input, unless Parse supplies the owner's semantics.
// Get and Lookup read the environment on every call, not at declaration time.
type Var[T string | bool] struct {
	Name           string
	Default        T
	Description    string
	Component      string
	AcceptedValues string
	Precedence     string
	// DefaultDescription explains an effective default resolved by the consumer.
	DefaultDescription string
	// SystemProvided distinguishes injected runtime identity from operator configuration.
	SystemProvided bool
	// Parse overrides the built-in conversion, including for empty values.
	Parse func(string) T
}

// Get returns the current value, or Default when unset.
func (v Var[T]) Get() T {
	value, _ := v.Lookup()
	return value
}

// Lookup reports whether the variable is present, preserving the distinction
// between unset and explicitly empty values.
func (v Var[T]) Lookup() (T, bool) {
	raw, present := os.LookupEnv(v.Name)
	if !present {
		return v.Default, false
	}
	if v.Parse != nil {
		return v.Parse(raw), true
	}
	switch any(v.Default).(type) {
	case string:
		return any(raw).(T), true
	case bool:
		value, err := strconv.ParseBool(raw)
		if err == nil {
			return any(value).(T), true
		}
	}
	return v.Default, true
}
