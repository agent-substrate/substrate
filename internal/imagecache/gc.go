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

// Garbage collection for the layer pool.
//
// The pool is a two-level DAG — image records reference layers by diffID —
// so liveness is plain refcounting, recomputed from disk on every pass;
// there is nothing to maintain between passes and nothing to rebuild after
// a restart. An image (and then any layer its removal leaves unreferenced)
// is evictable unless vetoed by, in order:
//
//  1. the root set — bundle overlay specs under the actors dir, the same
//     authority that hands out mounts (atelet cannot see the ateoms' mount
//     namespaces);
//  2. min-age — records and layers younger than minAge are never touched,
//     covering the pull → spec-write → mount window;
//  3. per-victim re-checks against current disk state immediately before
//     acting.
//
// The periodic pass reaches layers ONLY through records: pull writes the
// image record before unpacking (see Store.pull), so every layer is
// referenced — and thereby protected — before it can exist on disk.
// Unexplained layers can therefore only be crash debris, reclaimed once at
// startup by RecoverOrphans; there is no online whole-pool scan.
//
// Deletion is two-phase: the only steps that contend with the pull path
// are one os.Remove of a record and one rename of a layer dir to a ".rm-*"
// name inside the layer's singleflight (see retireLayer); the slow
// RemoveAll of multi-GB trees happens afterwards, on dirs nothing can
// reach by diffID. A crash in between leaves a ".rm-*" dir for the
// startup sweep.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"golang.org/x/sync/errgroup"
)

// ErrIncompleteEnumeration marks a pass that did nothing because the
// image records or bundle specs could not be fully enumerated. Callers
// use errors.Is to tell "nothing was attempted" (repair the named file;
// not a shortfall) from per-item failures on a pass that ran.
var ErrIncompleteEnumeration = errors.New("image cache enumeration incomplete")

// --- root set ---

// RootSet is the set of images and layers that eviction must not touch,
// recomputed from disk at the start of every pass.
type RootSet struct {
	// ImageDigests are rooted image digest strings ("sha256:<hex>").
	ImageDigests map[string]bool
	// LayerHexes are rooted layer diffID hexes (the layer dir base names),
	// covering bundle specs written before ImageDigest existed and
	// belt-and-suspenders for those written after.
	LayerHexes map[string]bool
	// LayerSets holds a signature per rooted bundle's *exact* layer set.
	// A record whose layer set matches one is rooted too: the multi-arch
	// twin and the digestless pre-ImageDigest spec, whose record would
	// otherwise be evicted while its layers survive — manufacturing an
	// orphan when the bundle goes. Exact match on purpose: rooting every
	// subset image (e.g. a running actor's base image) would quietly
	// weaken --image-cache-max-bytes under heavy layer sharing.
	LayerSets map[string]bool
}

// layerSetSignature canonicalizes a set of layer hexes for comparison.
func layerSetSignature(hexes []string) string {
	uniq := make([]string, 0, len(hexes))
	seen := make(map[string]bool, len(hexes))
	for _, h := range hexes {
		if !seen[h] {
			seen[h] = true
			uniq = append(uniq, h)
		}
	}
	sort.Strings(uniq)
	return strings.Join(uniq, ",")
}

// InUse scans the actors directory for bundle overlay specs and returns
// the images and layers referenced by actors placed on this node. Bundles
// exist exactly while an actor runs or transitions here (spec written
// before any mount, deleted after unmount), so the scan protects actively
// mounted images via the same authority that hands out mounts. Leftover
// bundles from crashed actors over-pin until wiped.
//
// A non-nil error means the root set may be incomplete: an unreadable
// actors dir, bundles dir, or spec. Deleting callers must then do nothing —
// a missing root fails toward retiring a running actor's layers, and for a
// long-running actor the spec is the only protection (its record mtime can
// be arbitrarily old, so min-age would not save it). A missing dir or spec
// file is not an error: no actor was ever placed, the actor is torn down,
// or the bundle predates its spec write.
func (s *Store) InUse() (RootSet, error) {
	// Per-item lines are Debug and gated: on a full node this loop emits
	// hundreds of them, and ungated slog calls build their attr args even
	// when suppressed.
	dbg := slog.Default().Enabled(context.Background(), slog.LevelDebug)
	rs := RootSet{ImageDigests: map[string]bool{}, LayerHexes: map[string]bool{}, LayerSets: map[string]bool{}}
	if s.actorsDir == "" {
		return rs, nil
	}
	actorEntries, err := os.ReadDir(s.actorsDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return rs, nil
		}
		return rs, fmt.Errorf("while listing actors dir %q for root set: %w", s.actorsDir, err)
	}
	var errs []error
	for _, actor := range actorEntries {
		if !actor.IsDir() {
			continue
		}
		bundlesDir := filepath.Join(s.actorsDir, actor.Name(), "bundles")
		bundles, err := os.ReadDir(bundlesDir)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue // no bundles: actor not placed / already torn down
			}
			errs = append(errs, fmt.Errorf("while listing bundles of actor %q: %w", actor.Name(), err))
			continue
		}
		for _, bundle := range bundles {
			if !bundle.IsDir() {
				continue
			}
			// WriteSpec renames its temp file into place, so a spec read here
			// is always whole; a partial read would under-report an actor's
			// layers and fail toward deleting them.
			spec, err := ReadSpec(filepath.Join(bundlesDir, bundle.Name()))
			if err != nil {
				errs = append(errs, fmt.Errorf("while reading bundle overlay spec %q: %w", filepath.Join(actor.Name(), "bundles", bundle.Name()), err))
				continue
			}
			if spec == nil {
				continue
			}
			addSpecRoots(&rs, spec, filepath.Join(actor.Name(), "bundles", bundle.Name()), dbg)
		}
	}
	return rs, errors.Join(errs...)
}

// addSpecRoots roots one bundle spec's image digest, layers, and exact
// layer-set signature.
func addSpecRoots(rs *RootSet, spec *OverlaySpec, bundle string, dbg bool) {
	if spec.ImageDigest != "" {
		rs.ImageDigests[spec.ImageDigest] = true
		if dbg {
			slog.Debug("Image cache root-set: bundle roots image",
				slog.String("bundle", bundle),
				slog.String("digest", spec.ImageDigest),
				slog.Int("layers", len(spec.Layers)))
		}
	} else if len(spec.Layers) > 0 && dbg {
		slog.Debug("Image cache root-set: digestless bundle roots layers only",
			slog.String("bundle", bundle),
			slog.Int("layers", len(spec.Layers)))
	}
	hexes := make([]string, 0, len(spec.Layers))
	for _, layerDir := range spec.Layers {
		hex := filepath.Base(layerDir)
		rs.LayerHexes[hex] = true
		hexes = append(hexes, hex)
	}
	if len(hexes) > 0 {
		rs.LayerSets[layerSetSignature(hexes)] = true
	}
}

// --- eviction ---

// EvictStats reports what an eviction pass did (or, dry-run, would do).
type EvictStats struct {
	// FreedBytes sums retired layers' recorded sizes, read from the size
	// files that rode along with the rename (walked read-only only when
	// absent) — consistent with CacheSize's accounting. An estimate; the
	// caller's next statfs self-corrects.
	FreedBytes int64
	// EvictedImages / EvictedLayers count deleted records and retired layer
	// dirs.
	EvictedImages, EvictedLayers int
	// Candidates is the number of LRU-ordered eviction candidates after all
	// listing-stage vetoes.
	Candidates int
	// RootedImages counts image records excluded because a bundle overlay
	// spec roots them (the "actively placed" protection).
	RootedImages int
	// SkippedRooted counts layers kept during the pass because a bundle
	// spec roots them (rooted images at listing time count into
	// RootedImages instead). SkippedFresh counts min-age vetoes, fired at
	// listing time or by the per-victim re-check.
	SkippedRooted, SkippedFresh int
	// OrphanLayers counts layers reclaimed by the startup scan
	// (RecoverOrphans) — always zero for periodic passes, which reach
	// layers only through records. Bytes are included in FreedBytes.
	OrphanLayers int
}

type evictionCandidate struct {
	digest  v1.Hash
	modTime time.Time
	diffIDs []string // unique, in record order
	raw     []byte   // record file bytes, for restoration if a layer must be kept
}

// EvictUnused evicts least-recently-used unprotected images — and the
// layers their removal leaves unreferenced — until ~targetBytes is freed
// or candidates run out. math.MaxInt64 means "free everything eligible";
// targetBytes <= 0 evicts nothing. With dryRun nothing is deleted or
// renamed.
//
// Per-item failures are aggregated into the error, not fatal: the pass
// continues and each item retries next pass. Passes are serialized. An
// incomplete record or bundle-spec enumeration skips the pass entirely —
// refcounts and roots from partial data would retire layers that unread
// records still reference or running actors still mount.
func (s *Store) EvictUnused(ctx context.Context, targetBytes int64, dryRun bool) (EvictStats, error) {
	var stats EvictStats
	s.evictMu.Lock()
	defer s.evictMu.Unlock()

	roots, rootsErr := s.InUse()
	if rootsErr != nil {
		// Same shape as the record gate below: refcounts and roots from a
		// partial scan would retire layers a running actor still mounts.
		// Not logged here — the caller logs the gated pass once, with its
		// own context (see ErrIncompleteEnumeration).
		return stats, errors.Join(ErrIncompleteEnumeration, rootsErr)
	}
	cutoff := time.Now().Add(-s.minAge)

	candidates, refcount, complete, listErr := s.listEviction(roots, cutoff, &stats)
	if !complete {
		// A layer shared with an unread record would hit refcount zero and
		// be retired while that record still names it. Fail the whole pass
		// toward retention; the error names the records to repair or
		// delete, and every later pass retries.
		return stats, errors.Join(ErrIncompleteEnumeration, listErr)
	}
	stats.Candidates = len(candidates)

	slog.InfoContext(ctx, "Image cache eviction pass",
		slog.Int64("target_bytes", targetBytes),
		slog.Bool("dry_run", dryRun),
		slog.Int("rooted_images", stats.RootedImages),
		slog.Int("rooted_layers", len(roots.LayerHexes)),
		slog.Int("candidates", len(candidates)),
		slog.Duration("min_age", s.minAge))

	var errs []error
	dbg := slog.Default().Enabled(ctx, slog.LevelDebug) // see InUse
	var retired []string                                // renamed-aside dirs awaiting RemoveAll

	for _, cand := range candidates {
		if targetBytes <= 0 || stats.FreedBytes >= targetBytes {
			break
		}
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}

		skip, err := s.removeStaleRecord(cand, cutoff, dryRun)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		switch skip {
		case "fresh":
			if dbg {
				slog.DebugContext(ctx, "Image cache eviction skipped image",
					slog.String("digest", cand.digest.String()),
					slog.String("reason", skip),
					slog.Time("last_used", cand.modTime))
			}
			stats.SkippedFresh++
			continue
		case "gone":
			continue
		}
		slog.InfoContext(ctx, "Image cache evicting image record",
			slog.String("digest", cand.digest.String()),
			slog.Time("last_used", cand.modTime),
			slog.Int("layers", len(cand.diffIDs)),
			slog.Bool("dry_run", dryRun))

		kept, candRetired, retireErrs := s.retireCandidateLayers(ctx, cand, refcount, roots, cutoff, dryRun, dbg, &stats)
		errs = append(errs, retireErrs...)
		retired = append(retired, candRetired...)
		if kept {
			// Restore the record so kept layers stay reachable, and put the
			// refcounts back so later victims don't under-count shared
			// layers. Already-retired layers stay retired — the stale record
			// re-pulls only the gaps.
			if !dryRun {
				if err := s.restoreRecord(cand.digest, cand.raw); err != nil {
					errs = append(errs, fmt.Errorf("while restoring record %s after kept layer: %w", cand.digest, err))
				} else {
					slog.InfoContext(ctx, "Image cache restored image record: some of its layers must be kept",
						slog.String("digest", cand.digest.String()))
				}
			}
			for _, hex := range cand.diffIDs {
				refcount[hex]++
			}
			continue
		}
		stats.EvictedImages++
	}

	if len(retired) > 0 {
		tRemove := time.Now()
		errs = append(errs, removeRetiredDirs(retired)...)
		slog.InfoContext(ctx, "Image cache removed retired layer dirs",
			slog.Int("count", len(retired)),
			slog.Duration("took", time.Since(tRemove)))
	}

	return stats, errors.Join(errs...)
}

// removeStaleRecord deletes the candidate's record unless a re-check
// against current disk state vetoes it — a pull or cache hit may have
// landed since listing. Runs under hitMu held exclusive so a hit's
// last-use touch cannot interleave with the re-check. Returns "gone" or
// "fresh" when the candidate must be skipped, "" to proceed.
func (s *Store) removeStaleRecord(cand evictionCandidate, cutoff time.Time, dryRun bool) (skip string, err error) {
	s.hitMu.Lock()
	defer s.hitMu.Unlock()
	fi, err := os.Stat(s.recordPath(cand.digest))
	if errors.Is(err, os.ErrNotExist) {
		return "gone", nil // e.g. removed by its multi-arch twin's pass
	} else if err != nil {
		return "", err
	}
	if fi.ModTime().After(cutoff) {
		return "fresh", nil // touched since listing: in use moments ago
	}
	if !dryRun {
		if err := os.Remove(s.recordPath(cand.digest)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("while deleting image record %s: %w", cand.digest, err)
		}
	}
	return "", nil
}

// retireCandidateLayers retires the layers un-referenced by the
// candidate's removal, crediting stats, and reports kept=true if any
// layer must stay (still referenced, rooted, fresh, or failed to retire).
// The caller must then restore the record: a kept layer with no record is
// unreachable until the next restart.
func (s *Store) retireCandidateLayers(ctx context.Context, cand evictionCandidate, refcount map[string]int, roots RootSet, cutoff time.Time, dryRun, dbg bool, stats *EvictStats) (kept bool, retired []string, errs []error) {
	for _, hex := range cand.diffIDs {
		refcount[hex]--
		if refcount[hex] > 0 {
			if dbg {
				slog.DebugContext(ctx, "Image cache keeping layer: still referenced",
					slog.String("diffid", hex), slog.Int("refcount", refcount[hex]))
			}
			continue
		}
		if roots.LayerHexes[hex] {
			// Rooted by a bundle spec (the digestless-spec case): without
			// its record this layer would strand when that bundle goes.
			if dbg {
				slog.DebugContext(ctx, "Image cache keeping layer: rooted by a bundle spec",
					slog.String("diffid", hex))
			}
			stats.SkippedRooted++
			kept = true
			continue
		}
		var size int64
		var retiredPath string
		var st retireStatus
		if dryRun {
			size, st = s.dryRunRetire(hex, cutoff)
		} else {
			var rerr error
			if retiredPath, st, rerr = s.retireLayer(hex, cutoff); rerr != nil {
				errs = append(errs, rerr)
				kept = true
				continue
			}
			if st == retireRetired {
				// Credit from the retired dir, whose size file rode along
				// with the rename (O(1); walked only if absent, read-only
				// either way). Sizing after the retire means the eviction
				// path never writes a backfill into the live pool.
				if size, rerr = s.layerSizeReadOnly(retiredPath); rerr != nil {
					size = 0 // unknown size: still evicted, credit nothing
				}
			}
		}
		switch st {
		case retireGone:
			continue
		case retireVetoed:
			kept = true
			continue
		}
		if dbg {
			slog.DebugContext(ctx, "Image cache retiring layer",
				slog.String("diffid", hex),
				slog.Int64("size_bytes", size),
				slog.Bool("dry_run", dryRun))
		}
		if retiredPath != "" {
			retired = append(retired, retiredPath)
		}
		stats.FreedBytes += size
		stats.EvictedLayers++
	}
	return kept, retired, errs
}

// removeRetiredDirs RemoveAlls renamed-aside layer dirs — the slow half
// of two-phase deletion, run outside every lock: the dirs are unreachable
// by diffID, so this contends with nothing. A crash mid-removal leaves
// ".rm-*" dirs for the startup sweep.
//
// Errors are collected per dir rather than through errgroup.Wait, which
// keeps only the first: every failed dir sits invisible as ".rm-*" until
// the next startup, so each deserves surfacing.
func removeRetiredDirs(dirs []string) []error {
	var (
		mu   sync.Mutex
		errs []error
	)
	g := new(errgroup.Group)
	g.SetLimit(4)
	for _, dir := range dirs {
		g.Go(func() error {
			if err := RemoveAllWritable(dir); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("while removing retired layer %q: %w", dir, err))
				mu.Unlock()
			}
			return nil
		})
	}
	_ = g.Wait() // goroutines only ever return nil
	return errs
}

// RecoverOrphans reclaims layer dirs that no image record references.
// Called once from New, before the store serves — the one moment the
// scan is race-free: no pull is in flight, so a layer without a record
// is definitionally garbage. Orphans cannot arise in normal operation
// (pull writes the record first; eviction retires layers in the pass
// that drops their records): this reaps crash debris and operator
// damage, the accepted alternative to an fsck against a live pool every
// pass — mid-life debris leaks until the next restart, logged.
//
// Skipped entirely (ERROR) when the record or bundle-spec enumeration is
// incomplete: refcounts from partial data make referenced layers look
// like garbage, and a missing spec root would sweep a mounted layer.
// Bundle-spec roots and min-age still veto.
func (s *Store) RecoverOrphans(ctx context.Context) (EvictStats, error) {
	var stats EvictStats
	s.evictMu.Lock()
	defer s.evictMu.Unlock()

	roots, rootsErr := s.InUse()
	if rootsErr != nil {
		return stats, errors.Join(ErrIncompleteEnumeration, rootsErr)
	}
	cutoff := time.Now().Add(-s.minAge)
	_, refcount, complete, listErr := s.listEviction(roots, cutoff, &stats)
	if !complete {
		return stats, errors.Join(ErrIncompleteEnumeration, listErr)
	}

	retired, errs := s.sweepOrphanLayers(ctx, roots, refcount, cutoff, false, &stats)
	errs = append(errs, removeRetiredDirs(retired)...)
	if stats.OrphanLayers > 0 {
		slog.InfoContext(ctx, "Image cache startup scan reclaimed orphan layers",
			slog.Int("count", stats.OrphanLayers),
			slog.Int64("freed_bytes", stats.FreedBytes))
	}
	return stats, errors.Join(errs...)
}

// sweepOrphanLayers retires layer dirs that no surviving image record
// references. Returns the renamed-aside paths for the caller's async
// removal batch, plus any per-layer errors (never fatal).
func (s *Store) sweepOrphanLayers(ctx context.Context, roots RootSet, refcount map[string]int, cutoff time.Time, dryRun bool, stats *EvictStats) ([]string, []error) {
	entries, err := os.ReadDir(s.layersDir())
	if err != nil {
		return nil, []error{fmt.Errorf("while listing layer pool for orphan sweep: %w", err)}
	}

	dbg := slog.Default().Enabled(ctx, slog.LevelDebug) // see InUse
	var retired []string
	var errs []error
	for _, e := range entries {
		hex := e.Name()
		// Dot-prefixed dirs are in-flight unpacks (".tmp-") or already
		// retired (".rm-"); both are handled elsewhere. Anything that is
		// not a well-formed digest was not put there by the store, so the
		// sweep leaves it alone rather than deleting an operator's file (or
		// panicking on a name too short to abbreviate).
		if !e.IsDir() || strings.HasPrefix(hex, ".") || !isLayerDirName(hex) {
			continue
		}
		if refcount[hex] > 0 {
			continue // referenced by a surviving record
		}
		if roots.LayerHexes[hex] {
			// A bundle spec references it directly — the digestless-spec
			// case, where the record is gone but an actor is still using
			// these layers.
			continue
		}
		var size int64
		var retiredPath string
		var st retireStatus
		if dryRun {
			size, st = s.dryRunRetire(hex, cutoff)
		} else {
			var rerr error
			if retiredPath, st, rerr = s.retireLayer(hex, cutoff); rerr != nil {
				errs = append(errs, rerr)
				continue
			}
			if st == retireRetired {
				// See retireCandidateLayers: size file rode along with the
				// rename; read-only, never a write into the live pool.
				if size, rerr = s.layerSizeReadOnly(retiredPath); rerr != nil {
					size = 0
				}
			}
		}
		if st != retireRetired {
			continue // too young, or vanished under us
		}
		if dbg {
			slog.DebugContext(ctx, "Image cache retiring orphan layer",
				slog.String("diffid", hex),
				slog.Int64("size_bytes", size),
				slog.Bool("dry_run", dryRun))
		}
		if retiredPath != "" {
			retired = append(retired, retiredPath)
		}
		stats.FreedBytes += size
		stats.OrphanLayers++
	}
	return retired, errs
}

// dryRunRetire reports what retireLayer would do, without renaming.
// Sizing is read-only: even a size-file backfill would break dry-run's
// mutate-nothing contract.
func (s *Store) dryRunRetire(hex string, cutoff time.Time) (int64, retireStatus) {
	dir := filepath.Join(s.layersDir(), hex)
	fi, err := os.Stat(dir)
	if err != nil {
		return 0, retireGone
	}
	if fi.ModTime().After(cutoff) {
		return 0, retireVetoed
	}
	size, err := s.layerSizeReadOnly(dir)
	if err != nil {
		size = 0
	}
	return size, retireRetired
}

// listEviction builds the LRU-ordered candidate list and the layer
// refcounts over ALL image records — rooted and fresh included, since
// their references are what keep shared layers alive — counting
// listing-stage vetoes into stats.
//
// complete reports whether every record was read and decoded. Refcounts
// from a partial listing understate references — a layer shared with an
// unread record looks unreferenced — so both callers gate on it and do
// nothing with a partial listing.
func (s *Store) listEviction(roots RootSet, cutoff time.Time, stats *EvictStats) (cands []evictionCandidate, refcount map[string]int, complete bool, err error) {
	refcount = map[string]int{}
	entries, err := os.ReadDir(s.manifestsDir())
	if err != nil {
		return nil, refcount, false, fmt.Errorf("while listing manifest records: %w", err)
	}
	complete = true
	var candidates []evictionCandidate
	var errs []error
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".json") {
			continue
		}
		hex := strings.TrimSuffix(name, ".json")
		digest := v1.Hash{Algorithm: "sha256", Hex: hex}

		b, err := os.ReadFile(filepath.Join(s.manifestsDir(), name))
		if err != nil {
			// This record's layers get no refcounts while the record itself
			// survives, so its layers would look like orphans.
			complete = false
			errs = append(errs, fmt.Errorf("while reading image record %s: %w", digest, err))
			continue
		}
		var rec imageRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			// An undecodable record contributes no refcounts, so evicting it
			// would strand its (unknown) layers as orphans until the next
			// restart. Never a candidate: leave it in place and fail the
			// enumeration — the error names the file for the operator.
			complete = false
			errs = append(errs, fmt.Errorf("while decoding image record %s: %w", digest, err))
			continue
		}

		// Dedupe diffIDs per record (images may list a layer twice) so the
		// decrement in EvictUnused stays symmetric with this count.
		seen := map[string]bool{}
		var unique []string
		for _, d := range rec.DiffIDs {
			diffID, err := v1.NewHash(d)
			if err != nil {
				// A garbled diffID contributes no refcount, so a layer
				// referenced only through it would look unreferenced — the
				// same failure direction as an undecodable record. Gate.
				complete = false
				errs = append(errs, fmt.Errorf("invalid diffID %q in image record %s: %w", d, digest, err))
				continue
			}
			if !seen[diffID.Hex] {
				seen[diffID.Hex] = true
				unique = append(unique, diffID.Hex)
				refcount[diffID.Hex]++
			}
		}

		fi, err := e.Info()
		if err != nil {
			continue
		}
		// A record is rooted either by digest (a bundle spec naming it) or
		// because its exact layer set matches a rooted bundle's (see
		// RootSet.LayerSets — deliberately not "every layer rooted
		// somewhere"). The exact-set rule covers the multi-arch twin —
		// pull records an image under both the index and per-platform
		// child digest, but a bundle spec carries only the requested
		// one — and digestless (pre-ImageDigest) specs. Without it the
		// twin is evicted and rewritten on every pull of a rooted image:
		// harmless but pure churn, and it inflates the eviction counters.
		if roots.ImageDigests[digest.String()] || (len(unique) > 0 && roots.LayerSets[layerSetSignature(unique)]) {
			stats.RootedImages++
			continue
		}
		if fi.ModTime().After(cutoff) {
			stats.SkippedFresh++
			continue
		}
		candidates = append(candidates, evictionCandidate{digest: digest, modTime: fi.ModTime(), diffIDs: unique, raw: b})
	}

	// LRU by last use, ties broken by name for determinism.
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].modTime.Equal(candidates[j].modTime) {
			return candidates[i].digest.Hex < candidates[j].digest.Hex
		}
		return candidates[i].modTime.Before(candidates[j].modTime)
	})
	return candidates, refcount, complete, errors.Join(errs...)
}

// restoreRecord atomically rewrites a record from bytes captured at
// listing time, after eviction deleted it but had to keep a layer —
// without the record the kept layer is unreachable until restart. The
// rewrite bumps the mtime, so min-age keeps the image off the next
// candidate list instead of churning delete-and-restore every pass; the
// cost is the true last-use, so a cold restored image sorts as fresh
// until it re-ages.
func (s *Store) restoreRecord(digest v1.Hash, raw []byte) error {
	path := s.recordPath(digest)
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("while creating record temp file: %w", err)
	}
	defer os.Remove(tmp.Name()) // no-op once the rename succeeds
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return fmt.Errorf("while writing record: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("while closing record: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("while moving record into place: %w", err)
	}
	return nil
}
