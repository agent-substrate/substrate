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

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/cmd/atelet/internal/atomicfile"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// runtimeIndex is this node's view of the sandbox runtimes its workers
// advertise: what the worker sandbox syncer wrote into each worker's
// versions file, kept with the asset records the hashes were computed from.
// Run and Restore resolve the runtime an actor is to use here, by the hash
// the worker registered with, so an actor runs exactly the binaries the
// control plane placed it for.
//
// Two maps answer two questions. runtimes, by hash, serves the hot path and
// never forgets an entry: a request may carry a hash a worker advertised
// earlier, config churn is rare, and an entry is a few URLs. workers, by
// worker pod UID, holds what each worker currently advertises and lives
// with the pod; it is where the default runtime to prewarm and to protect
// from eviction comes from.
//
// runtimes is also kept on disk, as one file that is the map serialized,
// rewritten durably before a new entry is added and loaded again by
// openRuntimeIndex. A worker registers a hash with the control plane only
// after atelet wrote its file, which happens after the index was written,
// so every hash a worker registered before an atelet restart resolves as
// soon as atelet serves again, with no wait on the syncer. The file stays
// small: a node sees a handful of configs, each with a few versions, in
// its lifetime. workers is rebuilt by the syncer.
//
// TODO: release runtimes that no pod has advertised for a long time.
type runtimeIndex struct {
	mu       sync.RWMutex
	runtimes map[string]*indexedRuntime
	workers  map[string]*workerRuntime
	// path persists runtimes; empty keeps the index in memory only.
	path string
}

// runtimeIndexFile is the on-disk form of runtimes.
type runtimeIndexFile struct {
	Runtimes map[string]indexedRuntimeFile `json:"runtimes"`
}

// indexedRuntimeFile is the on-disk form of one entry of runtimes, keyed by
// its hash in runtimeIndexFile.
type indexedRuntimeFile struct {
	Config  string               `json:"config"`
	Version string               `json:"version"`
	Record  *sandboxAssetsRecord `json:"record"`
}

// indexedRuntime is one advertised runtime: the record to stage, and where
// it was last computed from, for logs and errors.
type indexedRuntime struct {
	rec     *sandboxAssetsRecord
	config  string
	version string
}

// workerRuntime is what one worker's versions file advertises, by sandbox
// class: the SandboxConfig the worker's pool runs for the class and that
// config's runtimes. A pool runs one class today, so a worker advertises
// one; the shape admits the several a pool will be allowed to run.
type workerRuntime struct {
	classes map[atev1.SandboxClass]*classRuntime
}

// classRuntime is what a worker advertises for one sandbox class.
type classRuntime struct {
	config      string
	defaultHash string
	// hashes holds every advertised runtime, the default first, each once.
	hashes []string
}

// class returns what the worker advertises for c.
func (w *workerRuntime) class(c atev1.SandboxClass) (*classRuntime, bool) {
	cr, ok := w.classes[c]
	return cr, ok
}

// advertisedClass is one sandbox class a worker advertises: the config its
// pool runs for the class, with the config's versions hashed.
type advertisedClass struct {
	class  atev1.SandboxClass
	config string
	hashed []hashedVersion
}

// newRuntimeIndex returns an index kept in memory only.
func newRuntimeIndex() *runtimeIndex {
	return &runtimeIndex{
		runtimes: map[string]*indexedRuntime{},
		workers:  map[string]*workerRuntime{},
	}
}

// openRuntimeIndex returns the index persisted at path, loaded if the file
// exists. A file that cannot be decoded starts the index empty with a
// warning, and an entry whose record does not hash to its key is dropped
// with one: the syncer writes the current runtimes again either way.
func openRuntimeIndex(ctx context.Context, path string) (*runtimeIndex, error) {
	x := newRuntimeIndex()
	x.path = path
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		slog.InfoContext(ctx, "Sandbox runtime index starts empty", slog.String("path", path))
		return x, nil
	}
	if err != nil {
		return nil, fmt.Errorf("while reading the sandbox runtime index: %w", err)
	}
	var f runtimeIndexFile
	if err := json.Unmarshal(data, &f); err != nil {
		slog.WarnContext(ctx, "Sandbox runtime index is unreadable; starting empty", slog.String("path", path), slog.Any("err", err))
		return x, nil
	}
	for hash, e := range f.Runtimes {
		if err := checkIndexedRuntime(hash, e); err != nil {
			slog.WarnContext(ctx, "Dropping a sandbox runtime index entry", slog.String("runtime_hash", hash), slog.Any("err", err))
			continue
		}
		x.runtimes[hash] = &indexedRuntime{rec: e.Record, config: e.Config, version: e.Version}
	}
	slog.InfoContext(ctx, "Sandbox runtime index loaded", slog.String("path", path), slog.Int("runtimes", len(x.runtimes)))
	return x, nil
}

// checkIndexedRuntime reports whether e belongs under hash: its record is
// present and hashes to it.
func checkIndexedRuntime(hash string, e indexedRuntimeFile) error {
	if e.Record == nil {
		return errors.New("no record")
	}
	if got := bundleHash(e.Record); got != hash {
		return fmt.Errorf("record hashes to %s", got)
	}
	return nil
}

// writeIndex persists runtimes plus extra under extraHash, durably: once it
// returns, a crash cannot lose the file. Called with mu held.
func (x *runtimeIndex) writeIndex(extraHash string, extra *indexedRuntime) error {
	f := runtimeIndexFile{Runtimes: make(map[string]indexedRuntimeFile, len(x.runtimes)+1)}
	for hash, rt := range x.runtimes {
		f.Runtimes[hash] = indexedRuntimeFile{Config: rt.config, Version: rt.version, Record: rt.rec}
	}
	f.Runtimes[extraHash] = indexedRuntimeFile{Config: extra.config, Version: extra.version, Record: extra.rec}
	data, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("while encoding the sandbox runtime index: %w", err)
	}
	if err := atomicfile.WriteSync(x.path, data, 0o600); err != nil {
		return fmt.Errorf("while writing the sandbox runtime index: %w", err)
	}
	return nil
}

// register records what the worker pod advertises, replacing what it
// advertised before, and makes each runtime resolvable by its hash. The
// syncer calls it once the pod's file is written, so a worker's entry says
// what the file the worker registers from says; the hashes themselves are
// registered earlier through registerRuntimes. It fails only if a runtime
// not yet indexed cannot be persisted.
func (x *runtimeIndex) register(podUID string, advertised ...advertisedClass) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	wr := &workerRuntime{classes: make(map[atev1.SandboxClass]*classRuntime, len(advertised))}
	for _, a := range advertised {
		cr := &classRuntime{config: a.config}
		seen := make(map[string]bool, len(a.hashed))
		for _, v := range a.hashed {
			if err := x.addRuntime(a.config, v); err != nil {
				return err
			}
			if v.isDefault {
				cr.defaultHash = v.hash
			}
			if !seen[v.hash] {
				seen[v.hash] = true
				cr.hashes = append(cr.hashes, v.hash)
			}
		}
		wr.classes[a.class] = cr
	}
	x.workers[podUID] = wr
	return nil
}

// registerRuntimes makes each hashed version of config resolvable without
// changing what any worker advertises. The syncer calls it the moment a
// SandboxConfig or WorkerPool changes, ahead of the jittered sync that
// rewrites the files, so the index knows a config's runtimes as soon as the
// listers do, and a request for the new default that arrives within that
// delay resolves (see enqueuePool). It fails only if a runtime not yet
// indexed cannot be persisted; the ones before it are indexed.
func (x *runtimeIndex) registerRuntimes(config string, hashed []hashedVersion) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, v := range hashed {
		if err := x.addRuntime(config, v); err != nil {
			return err
		}
	}
	return nil
}

// addRuntime records v under its hash, unless the hash is already known,
// persisting the index first when it has a path. The hash covers every
// asset's name and digest, so two versions with one hash differ at most in
// where identical bytes are fetched from; keeping the first keeps the version
// the file was built from. Called with mu held.
func (x *runtimeIndex) addRuntime(config string, v hashedVersion) error {
	if _, ok := x.runtimes[v.hash]; ok {
		return nil
	}
	rt := &indexedRuntime{rec: v.rec, config: config, version: v.name}
	if x.path != "" {
		if err := x.writeIndex(v.hash, rt); err != nil {
			return err
		}
	}
	x.runtimes[v.hash] = rt
	return nil
}

// forget drops what the worker pod advertised. The runtimes themselves stay
// resolvable.
func (x *runtimeIndex) forget(podUID string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	delete(x.workers, podUID)
}

// lookup returns the runtime advertised under hash.
func (x *runtimeIndex) lookup(hash string) (*indexedRuntime, bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	rt, ok := x.runtimes[hash]
	return rt, ok
}

// worker returns what the worker pod currently advertises.
func (x *runtimeIndex) worker(podUID string) (*workerRuntime, bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	wr, ok := x.workers[podUID]
	return wr, ok
}

// advertisedRuntime returns the sandbox assets to stage for an actor placed
// on runtime rt: the record of the runtime a worker on this node advertises
// under rt's bundle hash. ate-api-server fills rt from the worker
// assignment, which is a runtime the worker registered from the file this
// node wrote, so the hash resolves as long as the request reached the node
// the actor was placed on.
//
// A miss means the request reached a node whose workers never advertised the
// runtime, which must surface. A runtime registered before an atelet restart
// is no miss: the index is loaded from disk before atelet serves.
func (s *Atelet) advertisedRuntime(ctx context.Context, rt *ateletpb.ActorSandboxRuntime) (*sandboxAssetsRecord, error) {
	hash, err := bundleHashOf(rt)
	if err != nil {
		return nil, apierror.InvalidArgument("sandbox_runtime: %v", err)
	}
	indexed, ok := s.runtimes.lookup(hash)
	if !ok {
		slog.WarnContext(ctx, "Sandbox runtime is not advertised by any worker on this node",
			slog.String("bundle_hash", hash),
			slog.String("sandbox_class", rt.GetSandboxClass()))
		return nil, apierror.FailedPrecondition("sandbox runtime %s (class %s) is not advertised by any worker on this node", hash, rt.GetSandboxClass())
	}
	if class := rt.GetSandboxClass(); class != indexed.rec.SandboxClass {
		return nil, apierror.FailedPrecondition("sandbox runtime %s is advertised for class %s, not %s", hash, indexed.rec.SandboxClass, class)
	}
	return indexed.rec, nil
}

// bundleHashOf returns the sandbox_bundle_hash attribute of rt, which names
// the SandboxConfig version a worker runs an actor under.
func bundleHashOf(rt *ateletpb.ActorSandboxRuntime) (string, error) {
	if rt == nil {
		return "", errors.New("missing")
	}
	for _, a := range rt.GetVersion().GetAttributes() {
		if a.GetKey() != hardware.AttrSandboxBundleHash {
			continue
		}
		if a.GetValue() == "" {
			return "", fmt.Errorf("empty %s attribute", hardware.AttrSandboxBundleHash)
		}
		return a.GetValue(), nil
	}
	return "", fmt.Errorf("no %s attribute", hardware.AttrSandboxBundleHash)
}
