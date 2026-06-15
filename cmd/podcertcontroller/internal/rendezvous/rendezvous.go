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

// Package rendezvous uses rendezvous hashing to help multiple controller replicas
// agree on which replica should handle an item.  The assignment is not atomic,
// so the controllers' actions need to be safe even if multiple replicas attempt
// to process the same item simultaneously.  However, in the steady state, where
// all replicas agree on the set of all healthy replicas.
package rendezvous

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	coordinationinformersv1 "k8s.io/client-go/informers/coordination/v1"
	"k8s.io/client-go/kubernetes"
	coordinationlistersv1 "k8s.io/client-go/listers/coordination/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

const labelKey = "rendezvous.ate.dev/application"

var (
	leaseDuration      = 15 * time.Second
	leaseRenewalPeriod = 10 * time.Second
)

var ErrNotAssigned = errors.New("item not assigned to this replica")

// Hasher is a rendezvous hashing implementation built on top of Kubernetes
// leases.
//
// Use Hasher when:
//
// * You have multiple replicas of your application.
//
// * You want to partition work items efficiently between them,  minimizing
// redundant work.
//
// * You can tolerate multiple replicas working on the same item at the same
// time.
//
// As opposed to leader election, rendezvous hashing lets us spread the
// controller's work across all live replicas of the controller.  As controller
// replicas spin up and down, work items will be gracefully rebalanced among the
// live replicas.  In general, if there are m items, and n replicas running, m/n
// items will be rebalanced to the remaining live replicas, with most items
// remaining at the same replica.
//
// Internally, Hasher writes a heartbeat to a specific Lease object, and scans
// for other Lease objects created by other replicas of your application.  If
// the lease has been renewed recently, the corresponding replica is considered
// live, otherwise it is considered dead and ignored.  Hasher will attempt to
// clean up owned Lease objects that seem to have been dead for longer than an
// hour.  It is possible for replicas to briefly disagree about the set of live
// replicas during membership changes, which may lead to affected items being
// ignored for up to one Lease timeout period.  This is no worse than the
// timeout behavior of Lease-based leader election.
//
// To use Hasher, you provide information about the Lease objects that will be
// created and queried: namespace and label selector.  You also provide the ID
// of the current replica, which is used as both the name of the replica's Lease
// and the identity of the owner of the lease.  It's fine to use a random UUID
// for your ID, but you can get greater stability across restarts by choosing a
// more persistent identifier.
//
// There's no reason to create more than one Hasher instance per binary.
//
// Hasher is based on the FNV-1a 64-bit variant.
type Hasher struct {
	kc kubernetes.Interface

	leaseNamespace  string
	applicationName string
	replicaName     string
	replicaUID      types.UID

	leaseInformer cache.SharedIndexInformer

	clock clock.Clock
}

// New returns a new Hasher.
func New(kc kubernetes.Interface, namespace, applicationName, replicaName string, replicaUID types.UID, clock clock.Clock) *Hasher {
	leaseLabelSelector := labels.SelectorFromSet(labels.Set{
		labelKey: applicationName,
	})

	leaseInformer := coordinationinformersv1.NewFilteredLeaseInformer(
		kc,
		namespace,
		0,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		func(opts *metav1.ListOptions) {
			opts.LabelSelector = leaseLabelSelector.String()
		},
	)

	return &Hasher{
		kc:              kc,
		leaseNamespace:  namespace,
		replicaName:     replicaName,
		replicaUID:      replicaUID,
		applicationName: applicationName,

		leaseInformer: leaseInformer,

		clock: clock,
	}
}

// Run starts the Hasher, blocking the current goroutine until ctx is done.
func (h *Hasher) Run(ctx context.Context) {
	go h.leaseInformer.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), h.leaseInformer.HasSynced) {
		return
	}
	go wait.UntilWithContext(ctx, h.runOnce, leaseRenewalPeriod)
	<-ctx.Done()
}

func (h *Hasher) runOnce(ctx context.Context) {
	if err := h.ensureLease(ctx); err != nil {
		slog.ErrorContext(ctx, "Error while ensuring lease", slog.String("err", err.Error()))
	}
}

func (h *Hasher) ensureLease(ctx context.Context) error {
	now := h.clock.Now()

	desiredLease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: h.leaseNamespace,
			Name:      h.replicaName,
			Labels: map[string]string{
				labelKey: h.applicationName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       h.replicaName,
					UID:        h.replicaUID,
					Controller: ptr.To(true),
				},
			},
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &h.replicaName,
			LeaseDurationSeconds: ptr.To(int32(int64(leaseDuration) / 1_000_000_000)),
			AcquireTime:          ptr.To(metav1.NewMicroTime(now)),
			RenewTime:            ptr.To(metav1.NewMicroTime(now)),
			LeaseTransitions:     ptr.To[int32](1),
		},
	}

	lease, err := h.kc.CoordinationV1().Leases(h.leaseNamespace).Get(ctx, h.replicaName, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		_, err = h.kc.CoordinationV1().Leases(h.leaseNamespace).Create(ctx, desiredLease, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("while creating lease: %w", err)
		}
		return nil
	} else if err != nil {
		return fmt.Errorf("while getting lease: %w", err)
	}

	lease.ObjectMeta.Labels = desiredLease.Labels
	lease.ObjectMeta.OwnerReferences = desiredLease.OwnerReferences
	lease.Spec.HolderIdentity = desiredLease.Spec.HolderIdentity
	lease.Spec.LeaseDurationSeconds = desiredLease.Spec.LeaseDurationSeconds
	lease.Spec.RenewTime = desiredLease.Spec.RenewTime

	_, err = h.kc.CoordinationV1().Leases(h.leaseNamespace).Update(ctx, lease, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("while updating lease: %w", err)
	}

	return nil
}

// AssignedToThisReplica uses rendezvous hashing to decide if the given item is
// assigned to the current replica.
//
// Item needs to be a stable identifier for the work item, like namespace/name
// or UID.  You don't need to treat the keys to make them uniform; that is
// handled by the rendezvous hashing implementation.
//
// To avoid leaking work items, you should continue to recheck items that are
// currently not assigned to this replica until they are finished, just in case,
// they get reassigned to this replica.  For example, when processing
// PodCertificateRequests, if the PCR is not currently assigned to this replica,
// add it back to the work queue with backoff.  Once the PCR is issued, it can
// be removed from the workqueue.
func (h *Hasher) AssignedToThisReplica(ctx context.Context, item string) bool {
	now := h.clock.Now()

	// List all Leases known to the informer --- we filtered based on our label
	// selector when defining the informer.
	leases, err := coordinationlistersv1.NewLeaseLister(h.leaseInformer.GetIndexer()).Leases(h.leaseNamespace).List(labels.Everything())
	if err != nil {
		slog.ErrorContext(ctx, "Error while listing all leases in informer", slog.String("err", err.Error()))
		return false
	}

	liveReplicas := make([]string, 0, len(leases))
	for _, lease := range leases {
		var leaseDuration time.Duration
		if lease.Spec.LeaseDurationSeconds != nil {
			leaseDuration = time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
		}

		// If the replica hasn't checked in by the deadline, consider it dead.
		deadline := lease.Spec.RenewTime.Time.Add(leaseDuration)
		if now.After(deadline) {
			continue
		}

		if lease.Spec.HolderIdentity == nil {
			continue
		}

		liveReplicas = append(liveReplicas, *lease.Spec.HolderIdentity)
	}

	return Hash(item, liveReplicas) == h.replicaName
}

func Hash(item string, replicas []string) string {
	// Rendezvous hashing: hash the item key with each replica key, and consider
	// the item assigned to the replica with the maximum hash value.
	maxWeight := uint64(0)
	maxReplica := ""
	for _, replica := range replicas {
		weight := fnvhash(item + replica)

		if maxReplica == "" {
			maxWeight = weight
			maxReplica = replica
			continue
		}

		// Tiebreak consistently if two replicas have the same weight
		if weight > maxWeight || (weight == maxWeight && replica < maxReplica) {
			maxWeight = weight
			maxReplica = replica
			continue
		}
	}

	return maxReplica
}

func fnvhash(key string) uint64 {
	hasher := fnv.New64()
	_, _ = hasher.Write([]byte(key))
	return hasher.Sum64()
}
