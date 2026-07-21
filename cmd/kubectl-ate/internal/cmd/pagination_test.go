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
	"context"
	"errors"
	"slices"
	"testing"
)

func TestFetchAllPages(t *testing.T) {
	var tokens []string
	got, err := fetchAllPages(context.Background(), func(_ context.Context, token string) ([]string, string, error) {
		tokens = append(tokens, token)
		if token == "" {
			return []string{"one", "two"}, "next", nil
		}
		return []string{"three"}, "", nil
	})
	if err != nil {
		t.Fatalf("fetchAllPages() error = %v", err)
	}
	if want := []string{"one", "two", "three"}; !slices.Equal(got, want) {
		t.Errorf("fetchAllPages() = %v, want %v", got, want)
	}
	if want := []string{"", "next"}; !slices.Equal(tokens, want) {
		t.Errorf("page tokens = %v, want %v", tokens, want)
	}
}

func TestFetchAllPagesReturnsError(t *testing.T) {
	wantErr := errors.New("list failed")
	got, err := fetchAllPages(context.Background(), func(context.Context, string) ([]string, string, error) {
		return nil, "", wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("fetchAllPages() error = %v, want %v", err, wantErr)
	}
	if got != nil {
		t.Errorf("fetchAllPages() = %v, want nil", got)
	}
}
