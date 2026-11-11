// Copyright 2026 The Agent Substrate Authors
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
	"fmt"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
)

func (r *runner) create(ctx context.Context) {
	r.runPhase(ctx, r.actors, "create", 0, "CreateActor", *flagCreateConc, 0, func(cctx context.Context, name string) (int, error) {
		err := r.createActor(cctx, name)
		if status.Code(err) == codes.AlreadyExists {
			return 0, nil
		}
		return 0, err
	})
	r.settle(ctx)
}

func (r *runner) createActor(ctx context.Context, name string) error {
	_, err := r.cli.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: *flagAtespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: *flagTemplateSpace, Name: *flagTemplate},
	}})
	return err
}

// firstResume resumes every actor once. Substrate creates each actor's
// external volumes on this call.
func (r *runner) firstResume(ctx context.Context) {
	st := r.runPhase(ctx, r.actors, "first-resume", 0, "ResumeActorFirstResume", *flagCreateConc, *flagCreateRate, r.resumeCall)
	states, err := r.stateCounts(ctx)
	if err != nil {
		fatal(err)
	}
	st.States = states
	r.replaceLastStats(st)
	logf("states after first resume: %v", states)
	r.repair(ctx)
	r.printSpread(ctx)
	r.settle(ctx)
}

// repair retries the first resume slowly for actors that are not RUNNING, so a
// volume claim the pool refused once does not count against the whole batch.
// The retries are recorded as their own phase.
func (r *runner) repair(ctx context.Context) {
	for round := 1; round <= *flagRepairRounds; round++ {
		pending, err := r.notRunning(ctx)
		if err != nil {
			fatal(err)
		}
		if len(pending) == 0 || ctx.Err() != nil {
			return
		}
		logf("repair round %d: %d actors not RUNNING", round, len(pending))
		select {
		case <-time.After(20 * time.Second):
		case <-ctx.Done():
			return
		}
		r.runPhase(ctx, pending, "first-resume-retry", round, "ResumeActorFirstResume", 5, 2, r.resumeCall)
	}
	left, err := r.notRunning(ctx)
	if err != nil {
		fatal(err)
	}
	if len(left) > 0 {
		logf("repair gave up with %d actors not RUNNING: %v", len(left), left)
	}
}

// lifecycle runs -repeat cycles of create, first resume, suspend, resume and
// delete, each on new actors.
func (r *runner) lifecycle(ctx context.Context) {
	base := r.actors
	for rep := 1; rep <= *flagRepeat && ctx.Err() == nil; rep++ {
		names := make([]string, len(base))
		for i, a := range base {
			names[i] = fmt.Sprintf("%s-r%02d", a, rep)
		}
		r.actors = names
		r.runPhase(ctx, names, "create", rep, "CreateActor", *flagCreateConc, 0, func(cctx context.Context, name string) (int, error) {
			return 0, r.createActor(cctx, name)
		})
		r.runPhase(ctx, names, "first-resume", rep, "ResumeActorFirstResume", *flagCreateConc, 0, r.resumeCall)
		r.printSpread(ctx)
		r.settle(ctx)
		r.runPhase(ctx, names, "suspend", rep, "SuspendActor", *flagCreateConc, 0, r.suspendCall)
		r.settle(ctx)
		r.runPhase(ctx, names, "resume", rep, "ResumeActor", *flagCreateConc, 0, r.resumeCall)
		r.settle(ctx)
		r.runPhase(ctx, names, "delete", rep, "DeleteActor", *flagDeleteConc, 0, func(cctx context.Context, name string) (int, error) {
			_, err := r.cli.DeleteActor(cctx, &ateapipb.DeleteActorRequest{Actor: r.ref(name)})
			return 0, err
		})
		r.settle(ctx)
	}
}

// delete deletes every actor in r.actors, then waits until they are gone from
// ListActors.
func (r *runner) delete(ctx context.Context) {
	r.runPhase(ctx, r.actors, "delete", 0, "DeleteActor", *flagDeleteConc, 0, func(cctx context.Context, name string) (int, error) {
		_, err := r.cli.DeleteActor(cctx, &ateapipb.DeleteActorRequest{Actor: r.ref(name)})
		if status.Code(err) == codes.NotFound {
			return 0, nil
		}
		return 0, err
	})
	want := map[string]bool{}
	for _, a := range r.actors {
		want[a] = true
	}
	t0 := time.Now()
	for ctx.Err() == nil {
		actors, err := r.listActors(ctx)
		if err != nil {
			fatal(err)
		}
		left := 0
		for _, a := range actors {
			if want[a.GetMetadata().GetName()] {
				left++
			}
		}
		if left == 0 {
			logf("actors gone %.1fs after the last DeleteActor returned", time.Since(t0).Seconds())
			return
		}
		if time.Since(t0) > 10*time.Minute {
			fatal(fmt.Errorf("%d actors still listed 10 min after DeleteActor", left))
		}
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
		}
	}
}

// loadExisting replaces r.actors with the actors that have -prefix.
func (r *runner) loadExisting(ctx context.Context) {
	actors, err := r.listActors(ctx)
	if err != nil {
		fatal(err)
	}
	var names []string
	for _, a := range actors {
		names = append(names, a.GetMetadata().GetName())
	}
	sort.Strings(names)
	r.actors = names
	logf("found %d actors with prefix %q in atespace %q", len(names), *flagPrefix, *flagAtespace)
}

// listActors lists the actors in -atespace whose names start with -prefix.
func (r *runner) listActors(ctx context.Context) ([]*ateapipb.Actor, error) {
	var out []*ateapipb.Actor
	token := ""
	for {
		cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		resp, err := r.cli.ListActors(cctx, &ateapipb.ListActorsRequest{
			Atespace: *flagAtespace,
			Options:  &ateapipb.ListOptions{PageSize: 500, PageToken: token},
		})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("ListActors: %w", err)
		}
		for _, a := range resp.GetActors() {
			if strings.HasPrefix(a.GetMetadata().GetName(), *flagPrefix+"-") {
				out = append(out, a)
			}
		}
		token = resp.GetNextPageToken()
		if token == "" {
			return out, nil
		}
	}
}

// batchActors lists the actors in r.actors, so actors left from an earlier run
// with the same -prefix are not counted or retried.
func (r *runner) batchActors(ctx context.Context) ([]*ateapipb.Actor, error) {
	actors, err := r.listActors(ctx)
	if err != nil {
		return nil, err
	}
	return inBatch(actors, r.actors), nil
}

// inBatch returns the actors whose names are in names.
func inBatch(actors []*ateapipb.Actor, names []string) []*ateapipb.Actor {
	in := make(map[string]bool, len(names))
	for _, n := range names {
		in[n] = true
	}
	var out []*ateapipb.Actor
	for _, a := range actors {
		if in[a.GetMetadata().GetName()] {
			out = append(out, a)
		}
	}
	return out
}

func (r *runner) stateCounts(ctx context.Context) (map[string]int, error) {
	actors, err := r.batchActors(ctx)
	if err != nil {
		return nil, err
	}
	states := map[string]int{}
	for _, a := range actors {
		states[a.GetStatus().GetState().String()]++
	}
	return states, nil
}

// notRunning returns the sorted names of actors in r.actors that are not
// RUNNING.
func (r *runner) notRunning(ctx context.Context) ([]string, error) {
	actors, err := r.batchActors(ctx)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, a := range actors {
		if a.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
			out = append(out, a.GetMetadata().GetName())
		}
	}
	sort.Strings(out)
	return out, nil
}

// printSpread logs how many actors run on each worker pod and node, and
// remembers each actor's node for calls.csv.
func (r *runner) printSpread(ctx context.Context) {
	actors, err := r.listActors(ctx)
	if err != nil {
		logf("actor spread unavailable: %v", err)
		return
	}
	pods := map[string]int{}
	nodes := map[string]int{}
	r.mu.Lock()
	if r.nodeOf == nil {
		r.nodeOf = map[string]string{}
	}
	for _, a := range actors {
		wa := a.GetStatus().GetWorkerAssignment()
		if wa == nil {
			continue
		}
		pods[wa.GetWorkerPod()]++
		nodes[wa.GetNodeName()]++
		r.nodeOf[a.GetMetadata().GetName()] = wa.GetNodeName()
	}
	r.mu.Unlock()
	logf("actors per worker pod: %v", pods)
	logf("actors per node: %v", nodes)
}

func (r *runner) settle(ctx context.Context) {
	if *flagSettle <= 0 {
		return
	}
	select {
	case <-time.After(*flagSettle):
	case <-ctx.Done():
	}
}
