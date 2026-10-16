// Copyright (c) KAITO authors.
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

package karpenter

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	karpenterv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	"github.com/kaito-project/kaito/pkg/utils/nodeclaim"
)

const (
	// maxPendingEventLookups bounds the deleted NodeClaims whose Events are
	// looked up per NodePool, and so the Event list calls per lookup.
	maxPendingEventLookups = 3
	// pendingEventLookupTTL bounds how long the Events of a deleted NodeClaim
	// are looked for; Events are recorded asynchronously around the deletion.
	pendingEventLookupTTL = 5 * time.Minute
	// eventLookupInterval throttles Event lookups per NodePool to limit the
	// load on the API server: a NodeClaim delete/recreate loop reconciles the
	// workspace every few seconds.
	eventLookupInterval = time.Minute
)

// failureTier ranks the provisioning failures of a NodePool's NodeClaims by
// causality: a failure of a higher tier causes the failures of lower tiers, so
// it is the root cause to report. A missing or not Ready NodeClass, which
// causes all of them, is checked live and not tracked.
type failureTier int

const (
	tierNone failureTier = iota
	// tierDeletedOnly: a NodeClaim was deleted before it launched or before its
	// node registered, and nothing else is known about why.
	tierDeletedOnly
	// tierInsufficientCapacity: an InsufficientCapacityError Event. It may be a
	// root cause (e.g. an exhausted regional quota) or the generic consequence
	// of an offering the provider marked unavailable after a cloud error.
	tierInsufficientCapacity
	// tierCloudError: the original cloud error, recorded on a NodeClaim's
	// lifecycle conditions (e.g. CreateInstanceFailed) or reported by an
	// AsyncProvisioningError Event. The providers mark the offering unavailable
	// on such errors, so later launches fail with insufficient capacity.
	tierCloudError
)

// rootCause is the provisioning failure reported for a NodePool.
type rootCause struct {
	tier    failureTier
	reason  string
	message string
}

// deletedNodeClaim identifies a NodeClaim deleted before its node registered.
type deletedNodeClaim struct {
	name      string
	uid       types.UID
	deletedAt time.Time
}

// nodePoolFailures holds the provisioning failures of a NodePool's NodeClaims
// since one of them last registered.
type nodePoolFailures struct {
	uid               types.UID
	cause             *rootCause
	pendingLookups    []deletedNodeClaim
	lastEventLookupAt time.Time
}

// record keeps the higher-tier cause; among causes of the same tier the first
// one is kept, because later attempts often fail with a consequence of it.
func (f *nodePoolFailures) record(c rootCause) {
	if f.cause == nil || c.tier > f.cause.tier {
		f.cause = &c
	}
}

func (f *nodePoolFailures) needsEventLookup() bool {
	return f.cause == nil || f.cause.tier < tierCloudError
}

// provisioningFailureTracker remembers the root cause of NodeClaim provisioning
// failures per NodePool so it stays visible in the Workspace status after
// Karpenter or the cloud provider has deleted the failed NodeClaims. It is fed
// from NodeClaim watch events, which see every transient state, unlike
// CollectNodeStatusInfo, which only sees the NodeClaims that still exist.
//
// A NodePool's entry lives from ProvisionNodes to DeleteNodes; NodeClaim events
// of a NodePool without an entry, or with another UID, are ignored. So a
// recreated NodePool does not inherit the failures of its predecessor, and the
// NodeClaims garbage-collected after the NodePool's deletion leave nothing
// behind. Entries do not expire: a failure stays relevant until a NodeClaim of
// the pool registers. The state is kept in memory only: after a controller
// restart, failures are reported again once they recur, which Karpenter's
// retries make likely, but a cause from before the restart may be replaced by
// a less specific one (e.g. a cached quota error by InsufficientCapacityError).
type provisioningFailureTracker struct {
	mu    sync.Mutex
	now   func() time.Time
	pools map[string]*nodePoolFailures
}

func newProvisioningFailureTracker() *provisioningFailureTracker {
	return &provisioningFailureTracker{now: time.Now, pools: map[string]*nodePoolFailures{}}
}

// track starts or keeps tracking the NodePool with the given name and UID.
func (t *provisioningFailureTracker) track(nodePoolName string, uid types.UID) {
	if nodePoolName == "" || uid == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if f := t.pools[nodePoolName]; f == nil || f.uid != uid {
		t.pools[nodePoolName] = &nodePoolFailures{uid: uid}
	}
}

// untrack forgets the NodePool with the given name.
func (t *provisioningFailureTracker) untrack(nodePoolName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.pools, nodePoolName)
}

func ownedByNodePool(nc *karpenterv1.NodeClaim, uid types.UID) bool {
	for _, ref := range nc.OwnerReferences {
		if ref.Kind == "NodePool" && ref.UID == uid {
			return true
		}
	}
	return false
}

// observe records the provisioning failure of a NodeClaim change. oldNC is the
// previous state on an update and nil otherwise; deleted is true when newNC is
// the final state of a deleted NodeClaim.
func (t *provisioningFailureTracker) observe(oldNC, newNC *karpenterv1.NodeClaim, deleted bool) {
	if newNC == nil {
		return
	}
	nodePoolName := newNC.Labels[karpenterv1.NodePoolLabelKey]
	registered := nodeclaim.IsConditionTrue(newNC, karpenterv1.ConditionTypeRegistered)
	wasRegistered := oldNC != nil && nodeclaim.IsConditionTrue(oldNC, karpenterv1.ConditionTypeRegistered)
	launched := nodeclaim.IsConditionTrue(newNC, karpenterv1.ConditionTypeLaunched)
	reason, message, failed := nodeclaim.LifecycleFailure(newNC)

	t.mu.Lock()
	defer t.mu.Unlock()
	f := t.pools[nodePoolName]
	if f == nil || !ownedByNodePool(newNC, f.uid) {
		return
	}

	if registered {
		// Registration rather than launch marks recovery, because a launch can
		// still fail asynchronously. Only the transition counts, so updates of
		// an already registered NodeClaim do not clear the failures of its
		// siblings. Deleting a registered NodeClaim is a normal lifecycle event.
		if !deleted && !wasRegistered {
			f.cause = nil
			f.pendingLookups = nil
		}
		return
	}

	if failed {
		f.record(rootCause{tier: tierCloudError, reason: nodeclaim.ValidConditionReason(reason), message: message})
	}
	if !deleted {
		return
	}
	if launched {
		f.record(rootCause{tier: tierDeletedOnly, reason: NodeClaimRegistrationFailedReason,
			message: fmt.Sprintf("NodeClaim %s was deleted after it launched but before its node registered", newNC.Name)})
	} else {
		f.record(rootCause{tier: tierDeletedOnly, reason: NodeClaimLaunchFailedReason,
			message: fmt.Sprintf("NodeClaim %s was deleted before it launched", newNC.Name)})
	}
	if !f.needsEventLookup() {
		return
	}
	for _, d := range f.pendingLookups {
		if d.uid == newNC.UID {
			return
		}
	}
	f.pendingLookups = append(f.pendingLookups, deletedNodeClaim{name: newNC.Name, uid: newNC.UID, deletedAt: t.now()})
	if len(f.pendingLookups) > maxPendingEventLookups {
		f.pendingLookups = f.pendingLookups[1:]
	}
}

// rootCause returns the root cause of the NodePool's provisioning failures since
// one of its NodeClaims last registered.
func (t *provisioningFailureTracker) rootCause(nodePoolName string) (rootCause, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	f := t.pools[nodePoolName]
	if f == nil || f.cause == nil {
		return rootCause{}, false
	}
	return *f.cause, true
}

// eventLookupCandidates returns the recently deleted NodeClaims whose Events
// should be looked up now, at most once per eventLookupInterval per NodePool,
// and only while no cloud error is known.
func (t *provisioningFailureTracker) eventLookupCandidates(nodePoolName string) []deletedNodeClaim {
	t.mu.Lock()
	defer t.mu.Unlock()
	f := t.pools[nodePoolName]
	if f == nil {
		return nil
	}
	now := t.now()
	kept := f.pendingLookups[:0]
	for _, d := range f.pendingLookups {
		if now.Sub(d.deletedAt) < pendingEventLookupTTL {
			kept = append(kept, d)
		}
	}
	f.pendingLookups = kept
	if len(f.pendingLookups) == 0 || !f.needsEventLookup() || now.Sub(f.lastEventLookupAt) < eventLookupInterval {
		return nil
	}
	f.lastEventLookupAt = now
	return append([]deletedNodeClaim(nil), f.pendingLookups...)
}

// recordEventLookup records the result of looking up the Events of a deleted
// NodeClaim. A found failure Event is recorded and ends the lookups of that
// NodeClaim; without one, its Events are looked up again later, since they are
// recorded asynchronously. Results for NodeClaims no longer pending, e.g.
// because a NodeClaim registered meanwhile, are dropped.
func (t *provisioningFailureTracker) recordEventLookup(nodePoolName string, d deletedNodeClaim, reason, message string, found bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	f := t.pools[nodePoolName]
	if f == nil {
		return
	}
	for i := range f.pendingLookups {
		if f.pendingLookups[i].uid != d.uid {
			continue
		}
		if found {
			f.pendingLookups = append(f.pendingLookups[:i], f.pendingLookups[i+1:]...)
			f.record(rootCause{tier: eventTier(reason), reason: nodeclaim.ValidConditionReason(reason), message: message})
		}
		return
	}
}
