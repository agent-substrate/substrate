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

// Package pemutil sanitizes PEM certificate bundles for projection into
// actors, the way kubelet does for clusterTrustBundle projected volumes.
package pemutil

import (
	"encoding/pem"
	"fmt"
	"math/rand/v2"
	"sort"
)

// SanitizeCertificateBundle re-encodes a PEM bundle keeping only CERTIFICATE
// blocks, with block headers stripped and exact duplicates (by DER bytes)
// removed. It returns an error if the input contains no CERTIFICATE blocks
// at all — an empty trust bundle is never what a workload should silently
// receive.
//
// The anchors are deliberately shuffled, as in kubelet's clustertrustbundle
// manager: order carries no meaning, and a scrambled order keeps consumers
// from growing a dependence on it.
func SanitizeCertificateBundle(in []byte) ([]byte, error) {
	seen := map[string]bool{}
	var ders []string
	rest := in
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		der := string(block.Bytes)
		if seen[der] {
			continue
		}
		seen[der] = true
		ders = append(ders, der)
	}
	if len(ders) == 0 {
		return nil, fmt.Errorf("bundle contains no CERTIFICATE PEM blocks")
	}

	// Sort first so the shuffle's input is independent of source order.
	sort.Strings(ders)
	rand.Shuffle(len(ders), func(i, j int) {
		ders[i], ders[j] = ders[j], ders[i]
	})

	var out []byte
	for _, der := range ders {
		out = append(out, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte(der)})...)
	}
	return out, nil
}
