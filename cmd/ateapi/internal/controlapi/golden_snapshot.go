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

package controlapi

import (
	"strconv"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/snapshotlayout"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// goldenActorRefFor names the template's golden actor: the actor the golden
// flow boots and snapshots for the golden tag. It lives in the reserved golden
// atespace, which the suspend workflow always takes a full snapshot in, and is
// named after the template UID, so templates recreated under one name never
// collide.
func goldenActorRefFor(tmpl *ateapipb.ActorTemplate) *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: tmpl.GetMetadata().GetUid()}
}

// isGoldenActor reports whether actor is the golden actor of tmpl, its
// template. The golden actor boots fresh: its snapshot becomes the golden the
// other actors of the template start from, so the golden the template records,
// or one being taken again, is no concern of its create.
func isGoldenActor(actor *ateapipb.Actor, tmpl *ateapipb.ActorTemplate) bool {
	return resources.ActorRefFromActor(actor) == resources.ActorRefFromObjectRef(goldenActorRefFor(tmpl))
}

// staleGoldenSnapshotLayout reports whether the status holds a golden tag
// whose snapshot was written in a layout older than the one the workers
// write, or records no layout. Such a golden is taken again in the current
// layout; its tag stays the template's golden until the new one is recorded.
func staleGoldenSnapshotLayout(snapshotStatus *ateapipb.GoldenSnapshotStatus) bool {
	return snapshotStatus.GetGoldenTag() != nil && snapshotStatus.GetSnapshotLayout() < snapshotlayout.Current
}

// goldenSnapshotRetakePending reports whether the template's golden is being
// taken again in the current snapshot layout: its tag is in an older one and
// the new take has not failed.
func goldenSnapshotRetakePending(snapshotStatus *ateapipb.GoldenSnapshotStatus) bool {
	return snapshotStatus.GetErrorMessage() == "" && staleGoldenSnapshotLayout(snapshotStatus)
}

// goldenSnapshotAwaitingRetake refuses, for the moment, a create on a
// template whose golden is being taken again in the current snapshot layout:
// the ActorTemplate reconciler records the new golden tag once it is
// complete, so the refusal is one a retry outlives.
func goldenSnapshotAwaitingRetake(actorTemplate *ateapipb.ActorTemplate) error {
	meta := actorTemplate.GetMetadata()
	return apierror.Unavailable("the golden snapshot of ActorTemplate %s/%s is being taken again in the current snapshot layout; retry", meta.GetAtespace(), meta.GetName())
}

// goldenTagRef is the tag the golden flow records for the template: named
// after the template UID, like the golden actor, for a first golden; after
// the UID and the layout, <uid>-<layout>, for a golden taken again in the
// current layout, which is made beside the recorded tag while that one stays
// the template's golden.
func goldenTagRef(tmpl *ateapipb.ActorTemplate, snapshotStatus *ateapipb.GoldenSnapshotStatus) *ateapipb.ObjectRef {
	name := tmpl.GetMetadata().GetUid()
	if snapshotStatus.GetGoldenTag().GetName() != "" {
		name = retakenGoldenTagName(name)
	}
	return &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: name}
}

// retakenGoldenTagName names the tag of a golden taken again in the current
// snapshot layout.
func retakenGoldenTagName(templateUID string) string {
	return templateUID + "-" + strconv.Itoa(int(snapshotlayout.Current))
}

// goldenTagRefs lists every tag the golden flow may hold for the template,
// for its deletion: the first golden's, named after the template UID; the one
// the status records; and, while a golden in an older layout is being taken
// again or that take failed, the new one, which may be unfinished.
func goldenTagRefs(tmpl *ateapipb.ActorTemplate) []resources.TagRef {
	uid := tmpl.GetMetadata().GetUid()
	snapshotStatus := tmpl.GetStatus().GetGoldenSnapshotStatus()
	names := []string{uid}
	if recorded := snapshotStatus.GetGoldenTag().GetName(); recorded != "" && recorded != uid {
		names = append(names, recorded)
	}
	if staleGoldenSnapshotLayout(snapshotStatus) {
		names = append(names, retakenGoldenTagName(uid))
	}
	refs := make([]resources.TagRef, 0, len(names))
	for _, name := range names {
		refs = append(refs, resources.TagRef{Atespace: resources.GoldenActorAtespace, Name: name})
	}
	return refs
}
