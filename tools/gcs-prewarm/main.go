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

// gcs-prewarm ramps synthetic write load against a GCS bucket so its key
// ranges are scaled up before a load test starts. A bucket sheds write bursts
// with 429 rateLimitExceeded until its autoscaler splits the loaded key
// ranges, which takes on the order of 20 minutes per doubling; running this
// shortly before a suspend/resume benchmark moves that scaling window out of
// the measured run.
//
// Objects are written as <prefix>/<random-hex>/warmup so the load spreads
// across the key range exactly like the real snapshot traffic
// (<prefix>/<uuid>/pages.img.zstd). The random component must come first:
// a fixed subdirectory like <prefix>/warmup/<uuid> would sort into one narrow
// slice of the range and scale only that slice.
//
// Usage:
//
//	go -C tools/gcs-prewarm run . \
//	  --bucket=substrate-snapshots-... \
//	  --prefix=<storageLocation path>/atespaces/<atespace>/actors \
//	  --start-rate=50 --target-rate=800 --double-every=5m --hold=5m
//
// 429s during the ramp are expected — they are the signal that GCS is still
// scaling — so they are counted and reported, not retried and not fatal.
// Warm-up objects are deleted at the end unless --cleanup=false.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

func main() {
	var (
		bucket      = flag.String("bucket", "", "bucket to warm (required)")
		prefix      = flag.String("prefix", "", "object prefix the real traffic writes under (required)")
		startRate   = flag.Float64("start-rate", 50, "initial writes per second")
		targetRate  = flag.Float64("target-rate", 500, "writes per second to ramp to and hold")
		doubleEvery = flag.Duration("double-every", 5*time.Minute, "how often the rate doubles while ramping")
		hold        = flag.Duration("hold", 5*time.Minute, "how long to hold the target rate after the ramp")
		objectBytes = flag.Int("object-bytes", 4096, "size of each warm-up object; index scaling depends on request rate, not bytes")
		workers     = flag.Int("workers", 128, "max writes in flight")
		cleanup     = flag.Bool("cleanup", true, "delete the warm-up objects at the end")
	)
	flag.Parse()
	if *bucket == "" || *prefix == "" {
		flag.Usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("storage client: %v", err)
	}
	defer client.Close()

	total := rampDuration(*startRate, *targetRate, *doubleEvery) + *hold
	log.Printf("warming gs://%s/%s: %.0f -> %.0f writes/s doubling every %s, then holding %s (total %s)",
		*bucket, *prefix, *startRate, *targetRate, *doubleEvery, *hold, total.Round(time.Second))

	bkt := client.Bucket(*bucket)
	warm(ctx, bkt, *prefix, *startRate, *targetRate, *doubleEvery, total, *objectBytes, *workers)

	if *cleanup {
		// Deletion is interesting even after Ctrl-C, so it gets a fresh context.
		cctx, ccancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Minute)
		defer ccancel()
		if err := deleteWarmupObjects(cctx, bkt, *prefix, *workers); err != nil {
			log.Fatalf("cleanup: %v", err)
		}
	}
}

// rateAt is the write rate after elapsed time: start doubling every
// doubleEvery, capped at target. Exponential rather than linear because GCS
// capacity itself grows by range splits, i.e. by doubling.
func rateAt(elapsed time.Duration, start, target float64, doubleEvery time.Duration) float64 {
	if start >= target {
		return target
	}
	r := start * math.Exp2(elapsed.Seconds()/doubleEvery.Seconds())
	return math.Min(r, target)
}

// rampDuration is how long rateAt takes to reach target.
func rampDuration(start, target float64, doubleEvery time.Duration) time.Duration {
	if start >= target {
		return 0
	}
	return time.Duration(math.Log2(target/start) * float64(doubleEvery))
}

// warm writes objects at the ramping rate until total has elapsed or ctx is
// canceled, reporting progress every 30s.
func warm(ctx context.Context, bkt *storage.BucketHandle, prefix string, start, target float64, doubleEvery, total time.Duration, objectBytes, workers int) {
	payload := make([]byte, objectBytes)
	var wrote, limited, failed atomic.Int64
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup

	report := func(when time.Duration) {
		log.Printf("t=%s rate=%.0f/s written=%d rate-limited=%d failed=%d",
			when.Round(time.Second), rateAt(when, start, target, doubleEvery),
			wrote.Load(), limited.Load(), failed.Load())
	}

	t0 := time.Now()
	nextReport := 30 * time.Second
	for {
		elapsed := time.Since(t0)
		if elapsed >= total || ctx.Err() != nil {
			break
		}
		if elapsed >= nextReport {
			report(elapsed)
			nextReport += 30 * time.Second
		}
		// Pace by sleeping one inter-arrival interval per write.
		interval := time.Duration(float64(time.Second) / rateAt(elapsed, start, target, doubleEvery))
		select {
		case <-time.After(interval):
		case <-ctx.Done():
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			continue
		}
		wg.Go(func() {
			defer func() { <-sem }()
			// One write per unique random key, like a real snapshot upload.
			name := fmt.Sprintf("%s/%s/warmup", prefix, randomHex())
			w := bkt.Object(name).NewWriter(ctx)
			_, werr := w.Write(payload)
			cerr := w.Close()
			switch err := errors.Join(werr, cerr); {
			case err == nil:
				wrote.Add(1)
			case isRateLimited(err):
				limited.Add(1)
			default:
				failed.Add(1)
			}
		})
	}
	wg.Wait()
	report(time.Since(t0))
}

// deleteWarmupObjects removes every object under prefix whose name ends in
// /warmup — only what this tool writes, never real snapshot files.
func deleteWarmupObjects(ctx context.Context, bkt *storage.BucketHandle, prefix string, workers int) error {
	log.Printf("deleting warm-up objects under %s/", prefix)
	var deleted atomic.Int64
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	it := bkt.Objects(ctx, &storage.Query{Prefix: prefix + "/"})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("listing: %w", err)
		}
		if !strings.HasSuffix(attrs.Name, "/warmup") {
			continue
		}
		sem <- struct{}{}
		name := attrs.Name
		wg.Go(func() {
			defer func() { <-sem }()
			if err := bkt.Object(name).Delete(ctx); err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
				log.Printf("delete %s: %v", name, err)
				return
			}
			deleted.Add(1)
		})
	}
	wg.Wait()
	log.Printf("deleted %d warm-up objects", deleted.Load())
	return nil
}

// randomHex returns 32 random hex characters: the same length and character
// class as the UUIDs in real snapshot paths, so warm-up keys spread across
// the key range the same way.
func randomHex() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand does not fail on supported platforms
	}
	return hex.EncodeToString(b[:])
}

func isRateLimited(err error) bool {
	var gerr *googleapi.Error
	return errors.As(err, &gerr) && gerr.Code == 429
}
