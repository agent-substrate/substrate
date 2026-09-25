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
// Disk is bounded by the cache's own eviction engine: below
// --min-free-gb free space, the tool asks Store.EvictUnused to reclaim
// the shortfall. --evict-all instead empties everything evictable and
// exits. Bundle-spec rooting (scanned from ateompath.ActorsDir) protects
// placed actors' mounted images. The engine's locks are per-process, so a
// run beside a live atelet is unsynchronized with its GC and pulls — the
// pool cannot be corrupted (record-first pulls, two-phase retirement),
// but a layer idle past --evict-idle can be retired just as atelet
// reuses it: a not-yet-mounted actor start fails once and heals on
// re-pull; an already-mounted actor can take EIO from a removed
// lowerdir. min-age is the only cross-process guard, hence the floor on
// --evict-idle when an actors dir exists. (The end state is an
// atelet-owned flush — RPC or trigger file — not a second process in
// the pool.) Run as the user that owns the cache and actors dirs — an
// unreadable actors dir gates every pass.
package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
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

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/imagecache"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	googlecontainerauth "github.com/google/go-containerregistry/pkg/v1/google"
	"golang.org/x/sys/unix"
)

var (
	refsFile  = flag.String("refs-file", "", "File with one image ref per line (required unless --evict-all)")
	sample    = flag.Int("sample", 0, "Validate a random sample of N refs (0 = all)")
	seed      = flag.Int64("seed", 1, "Seed for reproducible sampling")
	cacheDir  = flag.String("cache-dir", "", "Cache root (required); reused across runs")
	outCSV    = flag.String("out", "validate-results.csv", "Results CSV path")
	parallel  = flag.Int("parallel", 3, "Images validated concurrently (each pulls up to 4 layers in parallel)")
	timeout   = flag.Duration("timeout", 20*time.Minute, "Per-image timeout")
	minFreeGB = flag.Uint64("min-free-gb", 150, "Ask the eviction engine to reclaim disk when the cache volume has less free space than this")
	evictIdle = flag.Duration("evict-idle", 10*time.Minute, "Eviction min-age: layers and records younger than this are never evicted. Minimum 1m on a node with an actors dir. NOTE: if the corpus unpacks faster than this window elapses on a small disk, nothing is evictable while the disk fills — size it well below disk-fill time")
	evictAll  = flag.Bool("evict-all", false, "Evict everything evictable from the cache and exit (no refs file needed). Rooted images and anything younger than --evict-idle survive. Requires --force on a node with an actors dir")
	force     = flag.Bool("force", false, "Allow eviction on a node with an actors dir")
	platform  = flag.String("platform", "linux/amd64", "Image platform to pull")
)

// looksLikeLiveNode reports whether dir — the node's actors dir, the
// same authority the eviction root set is scanned from — exists.
// Anything but ENOENT counts: an unreadable actors dir is still a node.
func looksLikeLiveNode(dir string) bool {
	_, err := os.Stat(dir)
	return err == nil || !errors.Is(err, os.ErrNotExist)
}

// liveNodeIdleFloor: min-age is the only protection that works across
// processes (the engine's locks are per-process), so on a live node it
// must not be tuned away. Validation hosts keep full freedom.
const liveNodeIdleFloor = time.Minute

// errUsage marks validation failures that should print flag usage.
var errUsage = errors.New("usage")

// runConfig is the flag state validate checks — a plain struct so the
// combination matrix is table-testable.
type runConfig struct {
	cacheDir, refsFile string
	evictAll, force    bool
	evictIdle          time.Duration
	minFreeGB          uint64
	live               bool
	setFlags           []string // flag names given explicitly
}

func (c runConfig) validate() error {
	if c.cacheDir == "" || (c.refsFile == "" && !c.evictAll) || (c.refsFile != "" && c.evictAll) {
		return fmt.Errorf("%w: need --cache-dir plus exactly one of --refs-file or --evict-all", errUsage)
	}
	if c.evictIdle < 0 {
		// A negative min-age inverts the veto (the cutoff lands in the
		// future), making even in-flight pulls' layers evictable.
		return fmt.Errorf("--evict-idle %v must be >= 0", c.evictIdle)
	}
	if c.minFreeGB > math.MaxUint64/uint64(1e9) {
		return fmt.Errorf("--min-free-gb %d overflows a byte count", c.minFreeGB)
	}
	if c.evictAll {
		var stray []string
		for _, name := range c.setFlags {
			switch name {
			case "cache-dir", "evict-idle", "evict-all", "force":
				// The whole flush-mode surface; new flags fail closed.
			default:
				stray = append(stray, "--"+name)
			}
		}
		if len(stray) > 0 {
			return fmt.Errorf("%w: %s: only valid with --refs-file", errUsage, strings.Join(stray, ", "))
		}
	}
	if c.live {
		if c.evictIdle < liveNodeIdleFloor {
			return fmt.Errorf("--evict-idle=%v is below %v with %s present: min-age is the only protection that applies across processes using the cache",
				c.evictIdle, liveNodeIdleFloor, ateompath.ActorsDir)
		}
		// Both modes evict here (evictIfLow is a low-water flush on a
		// loop), unsynchronized with every other user of the pool.
		if !c.force {
			return fmt.Errorf("%s exists — this looks like a live node, and evictions are not synchronized with other processes using the cache; re-run with --force to proceed", ateompath.ActorsDir)
		}
	}
	return nil
}

// newStore opens the cache with the options both modes share: min-age
// from --evict-idle, and bundle-spec rooting from the node's actors dir
// (absent on a validation host, which InUse treats as an empty root set).
func newStore(extra ...imagecache.Option) (*imagecache.Store, error) {
	return imagecache.New(*cacheDir, append([]imagecache.Option{
		imagecache.WithMinAge(*evictIdle),
		imagecache.WithActorsDir(ateompath.ActorsDir),
	}, extra...)...)
}

type result struct {
	ref     string
	digest  string
	layers  int
	took    time.Duration
	errText string
}

func main() {
	flag.Parse()
	cfg := runConfig{
		cacheDir: *cacheDir, refsFile: *refsFile,
		evictAll: *evictAll, force: *force,
		evictIdle: *evictIdle, minFreeGB: *minFreeGB,
		live: looksLikeLiveNode(ateompath.ActorsDir),
	}
	flag.Visit(func(f *flag.Flag) { cfg.setFlags = append(cfg.setFlags, f.Name) })
	if err := cfg.validate(); err != nil {
		if errors.Is(err, errUsage) {
			fmt.Fprintln(os.Stderr, err)
			flag.Usage()
			os.Exit(2)
		}
		log.Fatal(err)
	}
	ctx := context.Background()

	if *evictAll {
		// Flush mode: no refs, no registry auth. New also reclaims any
		// crash-debris orphans before the pass.
		store, err := newStore()
		if err != nil {
			log.Fatalf("opening cache: %v", err)
		}
		stats, err := store.EvictUnused(ctx, math.MaxInt64, false)
		if errors.Is(err, imagecache.ErrIncompleteEnumeration) {
			// Gated: nothing was attempted; the error names the unreadable
			// or corrupt path.
			log.Fatalf("evict-all did nothing: %v", err)
		}
		if err != nil {
			log.Printf("evict-all finished with errors: %v", err)
		}
		// Print the summary even on partial failure; err decides the exit code.
		log.Printf("evict-all: %d images / %d layers evicted, %.1f GB credited (free now %s)",
			stats.EvictedImages, stats.EvictedLayers, float64(stats.FreedBytes)/1e9, freeGB(*cacheDir))
		if err != nil {
			os.Exit(1)
		}
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

	// This tool runs on a developer's machine, not a node, so it has no kubelet
	// credential provider to borrow. googlecontainerauth.Keychain authenticates
	// GCR and Artifact Registry from application-default credentials and
	// declines everything else.
	store, err := newStore(
		imagecache.WithKeychain(googlecontainerauth.Keychain),
		imagecache.WithPlatform(v1.Platform{OS: osName, Architecture: arch}),
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

// evictIfLow asks the eviction engine to reclaim the free-space
// shortfall below minFree. All engine protections apply, so in-flight
// validations are never raced. Unlike atelet's loop, the target is not
// capped at the pool size: on a dedicated validation disk an oversized
// target just runs out of candidates. The statfs shortfall and the
// engine's FreedBytes are different estimates (disk blocks vs recorded
// sizes), so a "met" target can leave free space still short; the next
// worker's re-check converges.
var (
	evictMu sync.Mutex // one attempt per low-water episode; queued workers re-check and return
	// lastFruitless backs off when a pass freed nothing — everything
	// younger than --evict-idle, or a gated pass. Without it every queued
	// worker would run a full engine pass ahead of its validation until
	// free space moved.
	lastFruitless time.Time
)

const fruitlessCooldown = 30 * time.Second

func evictIfLow(ctx context.Context, store *imagecache.Store, cacheRoot string, minFree uint64) {
	evictMu.Lock()
	defer evictMu.Unlock()

	free := freeBytes(cacheRoot)
	if free >= minFree {
		return
	}
	if time.Since(lastFruitless) < fruitlessCooldown {
		return
	}
	stats, err := store.EvictUnused(ctx, int64(minFree-free), false)
	switch {
	case errors.Is(err, imagecache.ErrIncompleteEnumeration):
		// Gated: nothing was attempted; the error names the path to repair.
		// Validation itself goes on — it only needed the disk space.
		log.Printf("eviction pass skipped, nothing attempted: %v", err)
	case err != nil:
		// Per-item failures on a pass that ran; each retries next pass.
		log.Printf("eviction pass finished with errors: %v", err)
	}
	if stats.EvictedImages > 0 || stats.EvictedLayers > 0 {
		log.Printf("evicted %d images / %d layers, %.1f GB credited (free now %s)",
			stats.EvictedImages, stats.EvictedLayers, float64(stats.FreedBytes)/1e9, freeGB(cacheRoot))
	}
	// The same counters the log line above gates on — not bytes, which
	// credit zero for a layer retired with an unreadable size file.
	if stats.EvictedImages == 0 && stats.EvictedLayers == 0 {
		lastFruitless = time.Now()
	}
}

func freeBytes(path string) uint64 {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return ^uint64(0) // unknown: don't evict
	}
	return st.Bavail * uint64(st.Bsize)
}

// freeGB renders free space for logs, naming the statfs-failure sentinel
// instead of printing it as ~18 billion GB.
func freeGB(path string) string {
	free := freeBytes(path)
	if free == ^uint64(0) {
		return "unknown"
	}
	return fmt.Sprintf("%.0f GB", float64(free)/1e9)
}
