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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/defaults"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protorange"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// applyAction describes what apply did to the target resource.
type applyAction string

const (
	applyCreated   applyAction = "created"
	applyUpdated   applyAction = "updated"
	applyUnchanged applyAction = "unchanged"
)

// singletonApplier adapts a singleton policy resource (such as AccessPolicy or
// EgressPolicy) to the shared apply flow.
type singletonApplier[T proto.Message] interface {
	label() string
	subcommand() string
	getPolicy(ctx context.Context) (T, error)
	createPolicy(ctx context.Context, policy T) (T, error)
	updatePolicy(ctx context.Context, policy T) (T, error)
	requireParent(ctx context.Context) error
	printApplied(policy T, action applyAction) error
	metadata(policy T) *ateapi.ResourceMetadata
	setMetadata(policy T, meta *ateapi.ResourceMetadata)
}

// validateApplyPreconditions checks that metadata.uid and metadata.version are
// either both omitted or both valid update preconditions. It reports whether
// the manifest pinned explicit preconditions.
func validateApplyPreconditions(meta *ateapi.ResourceMetadata) (pinned bool, err error) {
	if meta == nil {
		return false, nil
	}
	if meta.Uid == "" && meta.Version == 0 {
		return false, nil
	}
	if meta.Version < 0 {
		return false, fmt.Errorf("manifest metadata.version must be at least 1, got %d", meta.Version)
	}
	if meta.Uid == "" || meta.Version == 0 {
		return false, errors.New("manifest metadata.uid and metadata.version must either both be omitted or both be set")
	}
	return true, nil
}

// requireStoredPreconditions extracts uid and version from a server-returned
// resource's metadata, failing loudly if either is absent or invalid.
func requireStoredPreconditions(meta *ateapi.ResourceMetadata) (uid string, version int64, err error) {
	if meta == nil || meta.Uid == "" || meta.Version < 1 {
		return "", 0, errors.New("server response lacks metadata.uid or metadata.version")
	}
	return meta.Uid, meta.Version, nil
}

// hasUnknownFields reports whether m or any nested message contains protobuf
// fields unknown to this binary, such as fields added by a newer server.
func hasUnknownFields(m protoreflect.Message) bool {
	if !m.IsValid() {
		return false
	}
	found := false
	_ = protorange.Range(m, func(p protopath.Values) error {
		if msg, ok := p.Index(-1).Value.Interface().(protoreflect.Message); ok && len(msg.GetUnknown()) > 0 {
			found = true
			return protorange.Terminate
		}
		return nil
	})
	return found
}

// policiesEqual compares desired and existing after stripping metadata and
// applying resource defaults to both clones. It reports hasUnknown when
// existing carries protobuf fields unknown to this binary.
func policiesEqual[T proto.Message](a singletonApplier[T], desired, existing T) (equal bool, hasUnknown bool) {
	d := proto.CloneOf(desired)
	e := proto.CloneOf(existing)
	a.setMetadata(d, nil)
	a.setMetadata(e, nil)
	if hasUnknownFields(e.ProtoReflect()) {
		return false, true
	}
	defaults.Apply(d)
	defaults.Apply(e)
	return proto.Equal(d, e), false
}

// writeAppliedStatus writes the action notice to stderr in table mode only so
// structured -o yaml and -o json output leaves stderr clean.
func writeAppliedStatus(stderr io.Writer, outputFmt, label string, action applyAction) error {
	if outputFmt != "table" {
		return nil
	}
	_, err := fmt.Fprintf(stderr, "%s %s\n", label, action)
	return err
}

// applySingleton executes declarative apply for a singleton policy resource:
// create when missing, leave unchanged when equal after defaulting, or replace
// via optimistic-concurrency update.
func applySingleton[T proto.Message](ctx context.Context, a singletonApplier[T], desired T) error {
	if any(desired) == nil || !desired.ProtoReflect().IsValid() {
		return fmt.Errorf("cannot apply %s: policy is nil", a.label())
	}
	pinned, err := validateApplyPreconditions(a.metadata(desired))
	if err != nil {
		return err
	}

	existing, err := a.getPolicy(ctx)
	if status.Code(err) == codes.NotFound {
		if err := a.requireParent(ctx); err != nil {
			return err
		}
		if pinned {
			return fmt.Errorf("no %s exists; remove metadata.uid and metadata.version from the manifest to create it", a.label())
		}
		created, err := a.createPolicy(ctx, desired)
		if status.Code(err) == codes.AlreadyExists {
			return fmt.Errorf(`%s was created concurrently; re-run "kubectl ate apply %s": %w`, a.label(), a.subcommand(), err)
		}
		if err != nil {
			return fmt.Errorf("failed to create %s: %w", a.label(), err)
		}
		return a.printApplied(created, applyCreated)
	}
	if err != nil {
		return fmt.Errorf("failed to get %s: %w", a.label(), err)
	}
	if any(existing) == nil || !existing.ProtoReflect().IsValid() {
		return fmt.Errorf("cannot apply %s: server returned empty response", a.label())
	}

	uid, version, err := requireStoredPreconditions(a.metadata(existing))
	if err != nil {
		return fmt.Errorf("cannot apply %s: %w", a.label(), err)
	}
	if pinned {
		meta := a.metadata(desired)
		if meta.Uid != uid || meta.Version != version {
			return fmt.Errorf(`preconditions do not match the stored %s; run "kubectl ate get %s -o yaml" and reapply`, a.label(), a.subcommand())
		}
	}
	equal, hasUnknown := policiesEqual(a, desired, existing)
	if hasUnknown {
		return fmt.Errorf("stored %s contains fields unknown to this kubectl-ate version; upgrade kubectl-ate before applying", a.label())
	}
	if equal {
		return a.printApplied(existing, applyUnchanged)
	}

	toUpdate := proto.CloneOf(desired)
	if !pinned {
		meta := a.metadata(toUpdate)
		if meta == nil {
			meta = &ateapi.ResourceMetadata{}
			a.setMetadata(toUpdate, meta)
		}
		meta.Uid = uid
		meta.Version = version
	}
	updated, err := a.updatePolicy(ctx, toUpdate)
	if status.Code(err) == codes.NotFound {
		if err := a.requireParent(ctx); err != nil {
			return err
		}
		if pinned {
			return fmt.Errorf("%s was deleted concurrently; remove metadata.uid and metadata.version from the manifest before reapplying to create it: %w", a.label(), err)
		}
		return fmt.Errorf(`%s was deleted concurrently; re-run "kubectl ate apply %s": %w`, a.label(), a.subcommand(), err)
	}
	if status.Code(err) == codes.Aborted {
		if pinned {
			return fmt.Errorf(`preconditions do not match the stored %s; run "kubectl ate get %s -o yaml" and reapply: %w`, a.label(), a.subcommand(), err)
		}
		return fmt.Errorf(`%s changed concurrently; re-run "kubectl ate apply %s": %w`, a.label(), a.subcommand(), err)
	}
	if err != nil {
		return fmt.Errorf("failed to update %s: %w", a.label(), err)
	}
	return a.printApplied(updated, applyUpdated)
}
