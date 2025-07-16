package resource_division

import (
	"math"

	commonconstants "github.com/NVIDIA/KAI-scheduler/pkg/common/constants"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/common_info"
	rs "github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/proportion/resource_share"
)

// SetResourcesSharePriority distributes resources between queues according to the
// three-phase algorithm described in docs/developer/designs/priority-based-fair-share.md.
//
//  1. Queues are grouped by priority (higher priority buckets processed first).
//  2. Within a bucket we run:
//     a) Phase-1: allocate Deserved quota (subject to remaining capacity).
//     b) Phase-2: allocate over-quota resources proportionally to OverQuotaWeight.
//     c) Phase-3: allocate any still-remaining capacity equally between the queues
//     that still have unmet demand (treating all weights as 1).
func SetResourcesSharePriority(totalResource rs.ResourceQuantities, queues map[common_info.QueueID]*rs.QueueAttributes) {
	for _, resource := range rs.AllResources {
		remaining := totalResource[resource]
		if remaining <= 0 {
			continue
		}

		// Group queues by priority (higher first).
		queuesByPriority, priorities := getQueuesByPriority(queues)

		for _, priority := range priorities {
			if remaining <= 0 {
				break
			}
			bucket := queuesByPriority[priority]

			// ---------- Phase 1: Deserved quota ----------
			for _, q := range bucket {
				if remaining <= 0 {
					break
				}
				rsShare := q.ResourceShare(resource)
				deserved := rsShare.Deserved
				if deserved == commonconstants.UnlimitedResourceQuantity {
					deserved = totalResource[resource]
				}
				toGive := math.Min(deserved, rsShare.GetRequestableShare())
				toGive = math.Min(toGive, remaining)
				if toGive > 0 {
					q.AddResourceShare(resource, toGive)
					remaining -= toGive
				}
			}
			if remaining <= 0 {
				continue
			}

			// ---------- Phase 2: OverQuotaWeight ----------
			remaining = divideOverQuotaResource(remaining, bucket, resource)
			if remaining <= 0 {
				continue
			}

			// ---------- Phase 3: Weight-0 compensation ----------
			// save original OverQuotaWeight
			originalOverQuotaWeight := make(map[common_info.QueueID]float64)
			for _, q := range bucket {
				originalOverQuotaWeight[q.UID] = q.ResourceShare(resource).OverQuotaWeight
				q.ResourceShare(resource).OverQuotaWeight = 1
			}
			// divide over quota resource
			remaining = divideOverQuotaResource(remaining, bucket, resource)
			// restore original OverQuotaWeight
			for _, q := range bucket {
				q.ResourceShare(resource).OverQuotaWeight = originalOverQuotaWeight[q.UID]
			}

		}
	}

	reportDivisionResult(queues)
}
