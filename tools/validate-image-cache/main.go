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

// validate-image-cache batch-validates that OCI images can be pulled,
// parsed, and unpacked by internal/imagecache (the atelet-side half of the
// node-local image cache). It exercises Store.EnsureImage — registry pull,
// parallel per-layer streaming unpack, whiteout capture, record write — for
// every ref in a file, and reports per-image results as CSV.
//
// It does NOT mount overlays or run workloads (that half is Linux-only and
// privileged); it answers "can this image be loaded into the cache".
//
// Refs file: one image ref per line (digest refs recommended, e.g.
// us-docker.pkg.dev/proj/repo/img@sha256:...). Generate with:
//
//	gcloud artifacts docker images list REPO --format="value[separator='@'](package,version)"
//
// Disk is bounded by the cache's own eviction engine: when the cache
// volume's free space drops below --min-free-gb, the tool asks
// Store.EvictUnused to reclaim the difference — LRU by image last-use,
// with in-flight pulls protected by record refcounts and layers younger
// than --evict-idle never touched. --evict-all instead empties everything
// evictable and exits (operator use: flush a cache without deleting the
// directory).
package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/imagecache"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	googlecontainerauth "github.com/google/go-containerregistry/pkg/v1/google"
	"golang.org/x/sys/unix"
)

var (
	refsFile  = flag.String("refs-file", "", "File with one image ref per line (required)")
	sample    = flag.Int("sample", 0, "Validate a random sample of N refs (0 = all)")
	seed      = flag.Int64("seed", 1, "Seed for reproducible sampling")
	cacheDir  = flag.String("cache-dir", "", "Cache root (required); reused across runs")
	outCSV    = flag.String("out", "validate-results.csv", "Results CSV path")
	parallel  = flag.Int("parallel", 3, "Images validated concurrently (each pulls up to 4 layers in parallel)")
	timeout   = flag.Duration("timeout", 20*time.Minute, "Per-image timeout")
	minFreeGB = flag.Uint64("min-free-gb", 150, "Ask the eviction engine to reclaim disk when the cache volume has less free space than this")
	evictIdle = flag.Duration("evict-idle", 10*time.Minute, "Eviction min-age: layers and records younger than this are never evicted. NOTE: if the corpus unpacks faster than this window elapses on a small disk, nothing is evictable while the disk fills — size it well below disk-fill time")
	evictAll  = flag.Bool("evict-all", false, "Evict everything evictable from the cache and exit (no refs file needed)")
	platform  = flag.String("platform", "linux/amd64", "Image platform to pull")
)

type result struct {
	ref     string
	digest  string
	layers  int
	took    time.Duration
	errText string
}

func main() {
	flag.Parse()
	if *cacheDir == "" || (*refsFile == "" && !*evictAll) {
		flag.Usage()
		os.Exit(2)
	}
	ctx := context.Background()

	if *evictAll {
		// Flush mode: no refs, no registry auth. New also reclaims any
		// crash-debris orphans before the pass.
		store, err := imagecache.New(*cacheDir, imagecache.WithMinAge(*evictIdle))
		if err != nil {
			log.Fatalf("opening cache: %v", err)
		}
		stats, err := store.EvictUnused(ctx, math.MaxInt64, false)
		if err != nil {
			log.Fatalf("evict-all: %v", err)
		}
		log.Printf("evict-all: %d images / %d layers evicted, %.1f GB credited (free now %.0f GB)",
			stats.EvictedImages, stats.EvictedLayers, float64(stats.FreedBytes)/1e9, float64(freeBytes(*cacheDir))/1e9)
		return
	}

	refs, err := loadRefs(*refsFile)
	if err != nil {
		log.Fatalf("loading refs: %v", err)
	}
	if *sample > 0 && *sample < len(refs) {
		rand.New(rand.NewSource(*seed)).Shuffle(len(refs), func(i, j int) { refs[i], refs[j] = refs[j], refs[i] })
		refs = refs[:*sample]
	}
	log.Printf("validating %d images (parallel=%d, cache=%s)", len(refs), *parallel, *cacheDir)

	osName, arch, ok := strings.Cut(*platform, "/")
	if !ok {
		log.Fatalf("invalid --platform %q", *platform)
	}

	auth, err := googlecontainerauth.NewEnvAuthenticator(ctx)
	if err != nil {
		log.Fatalf("creating GCP authenticator (need application-default credentials): %v", err)
	}

	store, err := imagecache.New(*cacheDir,
		imagecache.WithAuthenticator(auth),
		imagecache.WithPlatform(v1.Platform{OS: osName, Architecture: arch}),
		imagecache.WithMinAge(*evictIdle),
	)
	if err != nil {
		log.Fatalf("opening cache: %v", err)
	}

	outF, err := os.Create(*outCSV)
	if err != nil {
		log.Fatalf("creating %s: %v", *outCSV, err)
	}
	defer outF.Close()
	csvW := csv.NewWriter(outF)
	_ = csvW.Write([]string{"ref", "digest", "layers", "seconds", "error"})
	var csvMu sync.Mutex

	var (
		wg          sync.WaitGroup
		sem         = make(chan struct{}, *parallel)
		done, fails int64
		countMu     sync.Mutex
	)
	tStart := time.Now()

	for _, ref := range refs {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()

			evictIfLow(ctx, store, *cacheDir, *minFreeGB*1e9)

			r := validateOne(ctx, store, ref, *timeout)

			csvMu.Lock()
			_ = csvW.Write([]string{r.ref, r.digest, strconv.Itoa(r.layers), fmt.Sprintf("%.1f", r.took.Seconds()), r.errText})
			csvW.Flush()
			csvMu.Unlock()

			countMu.Lock()
			done++
			if r.errText != "" {
				fails++
				log.Printf("FAIL [%d/%d] %s: %s", done, len(refs), r.ref, r.errText)
			} else if done%10 == 0 || done == int64(len(refs)) {
				elapsed := time.Since(tStart)
				eta := time.Duration(float64(elapsed) / float64(done) * float64(int64(len(refs))-done)).Round(time.Minute)
				log.Printf("ok [%d/%d] fails=%d elapsed=%s eta=%s (last: %s in %.0fs, %d layers)",
					done, len(refs), fails, elapsed.Round(time.Second), eta, shortRef(r.ref), r.took.Seconds(), r.layers)
			}
			countMu.Unlock()
		})
	}
	wg.Wait()

	log.Printf("done: %d images, %d failures, %s total; results in %s", done, fails, time.Since(tStart).Round(time.Second), *outCSV)
	if fails > 0 {
		os.Exit(1)
	}
}

func validateOne(ctx context.Context, store *imagecache.Store, ref string, timeout time.Duration) result {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	t := time.Now()
	img, err := store.EnsureImage(ctx, ref)
	r := result{ref: ref, took: time.Since(t)}
	if err != nil {
		r.errText = err.Error()
		return r
	}
	r.digest = img.Digest.String()
	r.layers = len(img.LayerDirs)
	return r
}

func loadRefs(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var refs []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line != "" {
			refs = append(refs, line)
		}
	}
	return refs, sc.Err()
}

func shortRef(ref string) string {
	if i := strings.LastIndex(ref, "/"); i >= 0 {
		return ref[i+1:]
	}
	return ref
}

// evictIfLow asks the cache's eviction engine to reclaim the free-space
// shortfall when the cache volume drops below minFree. The engine's
// protections all apply — record refcounts keep in-flight images' layers
// alive, --evict-idle is its min-age, deletion is two-phase — so nothing
// here can race a running validation. With no actors dir configured the
// root set is empty by design: nothing in a validation cache is mounted.
var evictMu sync.Mutex // one attempt per low-water episode; queued workers re-check and return

func evictIfLow(ctx context.Context, store *imagecache.Store, cacheRoot string, minFree uint64) {
	evictMu.Lock()
	defer evictMu.Unlock()

	free := freeBytes(cacheRoot)
	if free >= minFree {
		return
	}
	stats, err := store.EvictUnused(ctx, int64(minFree-free), false)
	if err != nil {
		// Per-item failures or a gated pass: either way validation goes on;
		// the next low-water check retries.
		log.Printf("eviction pass reported errors (continuing): %v", err)
	}
	if stats.EvictedImages > 0 || stats.EvictedLayers > 0 {
		log.Printf("evicted %d images / %d layers, %.1f GB credited (free now %.0f GB)",
			stats.EvictedImages, stats.EvictedLayers, float64(stats.FreedBytes)/1e9, float64(freeBytes(cacheRoot))/1e9)
	}
}

func freeBytes(path string) uint64 {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return ^uint64(0) // unknown: don't evict
	}
	return st.Bavail * uint64(st.Bsize)
}
