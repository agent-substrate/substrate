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
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// errDiffIDMismatch marks layer content that does not hash to its claimed
// diffID. It is a property of the image, so retrying cannot fix it.
var errDiffIDMismatch = errors.New("layer content does not match its claimed diffID")

// readVerifiedLayer passes the layer's uncompressed stream to consume, then
// reads whatever consume left unread and checks that the whole stream
// hashes to diffID. It returns only the number of bytes consume read. The
// rest never lands on disk, so counting it would let an image inflate the
// size GC accounts for.
//
// The pool shares layers across images by diffID, but each image's author
// chooses the diffIDs in its config. Checking the hash stops one image from
// planting content under another image's layer. Reading to EOF also lets
// go-containerregistry check the compressed blob against the manifest
// digest, which it does only at EOF.
func readVerifiedLayer(layer v1.Layer, diffID v1.Hash, consume func(io.Reader) error) (int64, error) {
	rc, err := layer.Uncompressed()
	if err != nil {
		return 0, fmt.Errorf("while opening layer stream: %w", err)
	}
	defer rc.Close()

	hasher, err := v1.Hasher(diffID.Algorithm)
	if err != nil {
		return 0, fmt.Errorf("while creating hasher for %s: %w", diffID, err)
	}
	tee := io.TeeReader(rc, hasher)
	counter := &countingReader{r: tee}
	if err := consume(counter); err != nil {
		return 0, err
	}
	if _, err := io.Copy(io.Discard, tee); err != nil {
		return 0, fmt.Errorf("while draining layer stream: %w", err)
	}
	if got := (v1.Hash{Algorithm: diffID.Algorithm, Hex: hex.EncodeToString(hasher.Sum(nil))}); got != diffID {
		return 0, fmt.Errorf("%w: content hashes to %s, config claims %s", errDiffIDMismatch, got, diffID)
	}
	return counter.n, nil
}
