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

package imagecache

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// errDiffIDMismatch marks layer content that does not hash to its claimed
// diffID. It is a property of the image, so retrying cannot fix it.
var errDiffIDMismatch = errors.New("layer content does not match its claimed diffID")

// readVerifiedLayer passes layer's uncompressed stream to consume, which may
// stop reading early (untar stops at the tar end marker), then drains the
// rest and requires the whole stream to hash to diffID. It returns the
// length consume read: drained bytes never land on disk, and counting them
// would let an image inflate the size GC accounts for.
//
// The pool is keyed by diffID and shared by every image on the node, and a
// diffID comes from the image config, which the image's author controls.
// Hashing the content is what makes reusing a layer by diffID safe.
// Draining to EOF matters too: go-containerregistry checks the compressed
// blob against the manifest digest only when the blob stream reaches EOF.
func readVerifiedLayer(layer v1.Layer, diffID v1.Hash, consume func(io.Reader) error) (int64, error) {
	rc, err := layer.Uncompressed()
	if err != nil {
		return 0, fmt.Errorf("while opening layer stream: %w", err)
	}
	defer rc.Close()

	h := sha256.New()
	tee := io.TeeReader(rc, h)
	cr := &countingReader{r: tee}
	if err := consume(cr); err != nil {
		return 0, err
	}
	if _, err := io.Copy(io.Discard, tee); err != nil {
		return 0, fmt.Errorf("while draining layer stream: %w", err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != diffID.Hex {
		return 0, fmt.Errorf("%w: content hashes to sha256:%s, config claims %s", errDiffIDMismatch, got, diffID)
	}
	return cr.n, nil
}
