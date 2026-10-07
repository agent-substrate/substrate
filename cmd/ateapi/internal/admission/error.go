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

package admission

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalid indicates that a caller-supplied spec mutation failed
	// declarative validation (for example, mutating an immutable field).
	ErrInvalid = errors.New("admission: invalid resource")

	// ErrFailedPrecondition indicates that an admission-level precondition
	// failed (for example, referencing a missing template or storage class, or
	// mutating a resource in a state that forbids the change).
	ErrFailedPrecondition = errors.New("admission: failed precondition")
)

type admissionError struct {
	sentinel error
	err      error
}

func (e *admissionError) Error() string { return e.err.Error() }

func (e *admissionError) Unwrap() error { return e.err }

func (e *admissionError) Is(target error) bool {
	return target == e.sentinel
}

func invalidf(format string, args ...any) error {
	return &admissionError{sentinel: ErrInvalid, err: fmt.Errorf(format, args...)}
}

func failedPreconditionf(format string, args ...any) error {
	return &admissionError{sentinel: ErrFailedPrecondition, err: fmt.Errorf(format, args...)}
}
