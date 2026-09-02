// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package versionlabel derives the ate.dev/substrate-version label that keys
// versioned dataplane objects to a substrate build version. The install script
// stamps it on nodes and on the versioned atelet DaemonSet; upgrade tooling
// pins WorkerPool pod templates to it. Every writer must derive names and
// values through this package so they agree.
package versionlabel

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// Key is the label key used consistently on nodes, Deployments, pod templates,
// and pod nodeSelectors.
const Key = "ate.dev/substrate-version"

// unknown stands in for a version that sanitizes to nothing.
const unknown = "unknown"

const nameSuffixMaxLength = 30

// Value normalizes a build version into a valid label value. The version is
// free-form via -ldflags, so invalid bytes map to '-' deterministically
// instead of being rejected.
func Value(version string) string {
	if version != "" && len(validation.IsValidLabelValue(version)) == 0 {
		return version
	}
	b := []byte(version)
	for i := range b {
		if c := b[i]; !isAlphanumerics(c) && c != '-' && c != '_' && c != '.' {
			b[i] = '-'
		}
	}
	if len(b) > validation.LabelValueMaxLength {
		b = b[:validation.LabelValueMaxLength]
	}
	// Label value boundaries must be alphanumeric.
	start, end := 0, len(b)
	for start < end && !isAlphanumerics(b[start]) {
		start++
	}
	for end > start && !isAlphanumerics(b[end-1]) {
		end--
	}
	if start == end {
		return unknown
	}
	return string(b[start:end])
}

// NameSuffix normalizes a build version into a DNS-1123-label-safe object
// name suffix that can be appended to an object name. Currently it's only
// used by the atelet DaemonSet during upgrade.
func NameSuffix(version string) string {
	if version == "" {
		return unknown
	}
	b := []byte(strings.ToLower(version))
	for i := range b {
		if c := b[i]; !('a' <= c && c <= 'z' || '0' <= c && c <= '9') {
			b[i] = '-'
		}
	}
	start, end := 0, len(b)
	for start < end && b[start] == '-' {
		start++
	}
	for end > start && b[end-1] == '-' {
		end--
	}
	s := string(b[start:end])
	if s == "" || len(s) > nameSuffixMaxLength {
		sum := sha256.Sum256([]byte(version))
		return "v" + hex.EncodeToString(sum[:])[:10]
	}
	return s
}

func isAlphanumerics(c byte) bool {
	return '0' <= c && c <= '9' || 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z'
}
