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
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kaito-project/kaito/pkg/utils/nodeclaim"
)

const (
	// nodeClaimEventNamespace is where Events about cluster-scoped objects such
	// as NodeClaims are recorded.
	nodeClaimEventNamespace = metav1.NamespaceDefault

	// EventFieldInvolvedObjectUID and EventFieldType are the Event field
	// selectors used to list only the Warning Events of a given NodeClaim.
	EventFieldInvolvedObjectUID = "involvedObject.uid"
	EventFieldType              = "type"

	// asyncProvisioningEventReason is published by the Azure provider when an
	// instance it accepted asynchronously fails to provision.
	asyncProvisioningEventReason = "AsyncProvisioningError"
	// insufficientCapacityEventReason is published by Karpenter when the cloud
	// provider reports insufficient capacity (including most quota errors)
	// right before it deletes the NodeClaim.
	insufficientCapacityEventReason = "InsufficientCapacityError"
)

// eventTier ranks the provisioning failure Events of a NodeClaim, see
// failureTier. An asynchronous provisioning error carries the cloud error that
// made the provider mark the offering unavailable, while an insufficient
// capacity Event may only be the generic consequence of such a cached failure.
// Other Events do not explain a provisioning failure.
func eventTier(reason string) failureTier {
	switch reason {
	case asyncProvisioningEventReason:
		return tierCloudError
	case insufficientCapacityEventReason:
		return tierInsufficientCapacity
	default:
		return tierNone
	}
}

// lookupProvisioningEvents looks up the Warning Events of the pool's recently
// deleted, never-registered NodeClaims and records the most specific one. Only
// the Warning Events of those NodeClaims are listed, using field selectors, at
// most once per eventLookupInterval per pool.
func (p *KarpenterProvisioner) lookupProvisioningEvents(ctx context.Context, nodePoolName string) {
	for _, d := range p.provisioningFailures.eventLookupCandidates(nodePoolName) {
		events := &corev1.EventList{}
		if err := p.client.List(ctx, events,
			client.InNamespace(nodeClaimEventNamespace),
			client.MatchingFieldsSelector{Selector: fields.AndSelectors(
				fields.OneTermEqualSelector(EventFieldInvolvedObjectUID, string(d.uid)),
				fields.OneTermEqualSelector(EventFieldType, corev1.EventTypeWarning),
			)},
		); err != nil {
			klog.V(4).InfoS("failed to list NodeClaim events", "nodeClaim", d.name, "err", err)
			continue
		}
		reason, message, found := provisioningFailureEvent(events.Items)
		p.provisioningFailures.recordEventLookup(nodePoolName, d, reason, message, found)
	}
}

// provisioningFailureEvent picks the highest-tier provisioning failure Event,
// the earliest one among Events of the same tier.
func provisioningFailureEvent(events []corev1.Event) (reason, message string, found bool) {
	var best *corev1.Event
	for i := range events {
		e := &events[i]
		if e.Type != corev1.EventTypeWarning || eventTier(e.Reason) == tierNone {
			continue
		}
		if best == nil || eventTier(e.Reason) > eventTier(best.Reason) ||
			eventTier(e.Reason) == eventTier(best.Reason) && eventTime(e).Before(eventTime(best)) {
			best = e
		}
	}
	if best == nil {
		return "", "", false
	}
	return best.Reason, nodeclaim.FormatProvisioningMessage(best.Message), true
}

func eventTime(e *corev1.Event) time.Time {
	switch {
	case !e.FirstTimestamp.IsZero():
		return e.FirstTimestamp.Time
	case !e.EventTime.IsZero():
		return e.EventTime.Time
	default:
		return e.CreationTimestamp.Time
	}
}
