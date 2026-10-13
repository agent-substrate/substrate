//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

// Package trustbundle resolves the trust bundle names that actors may
// project into system-info volumes to PEM certificate bundles.
package trustbundle

import (
	"bytes"
	"fmt"
	"math/rand/v2"

	"github.com/agent-substrate/substrate/internal/pemutil"
	certsv1 "k8s.io/api/certificates/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// EgressName is the well-known name of the egress gateway CA bundle (#823):
// the trust anchors for the per-SNI leaves the egress gateway mints,
// maintained by atecontroller from the egress-mitm-ca-pool.
const EgressName = "egress-mitm.ate.dev"

// EgressCTB is the ClusterTrustBundle read for EgressName.
const EgressCTB = "egress-mitm.ate.dev:mitm:primary-bundle"

// SystemRootsName is the well-known name for a selection of WebPKI root certificates.
const SystemRootsName = "system-roots.ate.dev"

// Source resolves allowlisted trust bundle names to their contents.
type Source struct {
	getCTB      func(name string) (*certsv1.ClusterTrustBundle, error)
	systemRoots []byte
	shuffleSeed int64
}

// NewSource creates a Source
func NewSource(getCTB func(name string) (*certsv1.ClusterTrustBundle, error), systemRootsPEM []byte) *Source {
	return &Source{
		getCTB:      getCTB,
		systemRoots: systemRootsPEM,
		shuffleSeed: rand.Int64(),
	}
}

// raw returns the unsanitized contents of the allowlisted bundle name.
func (s *Source) raw(name string) ([]byte, error) {
	switch name {
	case SystemRootsName:
		return s.systemRoots, nil
	case EgressName:
		objectName := EgressCTB
		bundle, err := s.getCTB(objectName)
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("trust bundle %q: ClusterTrustBundle %q not found", name, objectName)
		} else if err != nil {
			return nil, fmt.Errorf("trust bundle %q: while reading ClusterTrustBundle %q: %w", name, objectName, err)
		}
		return []byte(bundle.Spec.TrustBundle), nil
	default:
		return nil, fmt.Errorf("unknown trust bundle %q", name)
	}
}

// Combined returns the sanitized union of the named bundles' anchors,
// deduplicated across bundles.
func (s *Source) Combined(names []string) ([]byte, error) {
	if len(names) == 0 {
		return nil, fmt.Errorf("no trust bundle names given")
	}
	all := &bytes.Buffer{}
	for _, name := range names {
		raw, err := s.raw(name)
		if err != nil {
			return nil, err
		}

		all.Write(raw)
		all.WriteString("\n")
	}
	return pemutil.SanitizeCertificateBundle(all.Bytes(), s.shuffleSeed)
}
