// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package resource_division

import (
	"math"
	"slices"

	"golang.org/x/exp/maps"

	commonconstants "github.com/NVIDIA/KAI-scheduler/pkg/common/constants"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/resource_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/log"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/metrics"
	rs "github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/proportion/resource_share"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/scheduler_util"
)

type remainingRequestedResource struct {
	queue           *rs.QueueAttributes
	remainingAmount float64
}

func SetResourcesShare(totalResource rs.ResourceQuantities, kValue float64, queues map[common_info.QueueID]*rs.QueueAttributes) {
	for _, resource := range rs.AllResources {
		setResourceShare(totalResource[resource], kValue, resource, queues)
	}
	reportDivisionResult(queues)
}

func setResourceShare(totalAmount, kValue float64, resourceName rs.ResourceName, queues map[common_info.QueueID]*rs.QueueAttributes) float64 {
	log.InfraLogger.V(6).Infof("About to start calculating %v fairShare, totalAmount: <%v>", resourceName, totalAmount)
	remainingAmount := setDeservedResource(totalAmount, queues, resourceName)
	if remainingAmount > 0 {
		remainingAmount = divideOverQuotaResource(remainingAmount, kValue, queues, resourceName)
	} else {
		remainingAmount = 0
	}
	return remainingAmount
}

func reportDivisionResult(queues map[common_info.QueueID]*rs.QueueAttributes) {
	for _, queue := range queues {
		cpuResourceShare := queue.ResourceShare(rs.CpuResource)
		memoryResourceShare := queue.ResourceShare(rs.MemoryResource)
		gpuResourceShare := queue.ResourceShare(rs.GpuResource)

		metrics.UpdateQueueFairShare(
			queue.Name,
			cpuResourceShare.FairShare/resource_info.MilliCPUToCores,
			memoryResourceShare.FairShare/resource_info.MemoryToGB,
			gpuResourceShare.FairShare,
		)

		metrics.UpdateQueueUsage(
			queue.Name,
			cpuResourceShare.GetUsage(),
			memoryResourceShare.GetUsage(),
			gpuResourceShare.GetUsage(),
		)

		log.InfraLogger.V(3).Infof("Resource division result for queue <%v>: "+
			"Queue Priority: <%d>, "+
			"GPU: deserved: <%v>, requested: <%v>, maxAllowed: <%v>, usage: <%v>, fairShare: <%v> "+
			"CPU (cores): deserved: <%v>, requested: <%v>, maxAllowed: <%v>, usage: <%v>, fairShare: <%v> "+
			"memory (GB): deserved: <%v>, requested: <%v>, maxAllowed: <%v>, usage: <%v>, fairShare: <%v> ",
			queue.Name,
			queue.Priority,
			resource_info.HumanizeResource(gpuResourceShare.Deserved, 1),
			resource_info.HumanizeResource(gpuResourceShare.GetRequestableShare(), 1),
			resource_info.HumanizeResource(gpuResourceShare.MaxAllowed, 1),
			resource_info.HumanizeResource(gpuResourceShare.GetUsage(), 1),
			resource_info.HumanizeResource(gpuResourceShare.FairShare, 1),
			resource_info.HumanizeResource(cpuResourceShare.Deserved, resource_info.MilliCPUToCores),
			resource_info.HumanizeResource(cpuResourceShare.GetRequestableShare(), resource_info.MilliCPUToCores),
			resource_info.HumanizeResource(cpuResourceShare.MaxAllowed, resource_info.MilliCPUToCores),
			resource_info.HumanizeResource(cpuResourceShare.GetUsage(), resource_info.MilliCPUToCores),
			resource_info.HumanizeResource(cpuResourceShare.FairShare, resource_info.MilliCPUToCores),
			resource_info.HumanizeResource(memoryResourceShare.Deserved, resource_info.MemoryToGB),
			resource_info.HumanizeResource(memoryResourceShare.GetRequestableShare(), resource_info.MemoryToGB),
			resource_info.HumanizeResource(memoryResourceShare.MaxAllowed, resource_info.MemoryToGB),
			resource_info.HumanizeResource(memoryResourceShare.GetUsage(), resource_info.MemoryToGB),
			resource_info.HumanizeResource(memoryResourceShare.FairShare, resource_info.MemoryToGB))
	}
}

func setDeservedResource(
	totalResourceAmount float64, queues map[common_info.QueueID]*rs.QueueAttributes,
	resource rs.ResourceName,
) (remainingAmount float64) {
	remainingAmount = totalResourceAmount
	for _, queue := range queues {
		resourceShare := queue.ResourceShare(resource)
		deserved := resourceShare.Deserved
		if deserved == commonconstants.UnlimitedResourceQuantity {
			deserved = totalResourceAmount
		}

		amount := math.Min(deserved, resourceShare.GetRequestableShare())
		queue.AddResourceShare(resource, amount)
		remainingAmount -= amount
	}
	return remainingAmount
}

func divideOverQuotaResource(totalResourceAmount, kValue float64, queues map[common_info.QueueID]*rs.QueueAttributes,
	resourceName rs.ResourceName) (remainingAmount float64) {
	queuesByPriority, priorities := getQueuesByPriority(queues)
	remainingRequested := make(map[int]map[common_info.QueueID]*remainingRequestedResource)
	remainingAmount = totalResourceAmount

	for _, priority := range priorities {
		var newRemainingRequested map[common_info.QueueID]*remainingRequestedResource
		remainingAmount, newRemainingRequested = divideUpToFairShare(remainingAmount, kValue, queuesByPriority[priority], resourceName)
		if remainingRequested[priority] == nil {
			remainingRequested[priority] = make(map[common_info.QueueID]*remainingRequestedResource)
		}
		maps.Copy(remainingRequested[priority], newRemainingRequested)
	}

	for _, priority := range priorities {
		if remainingAmount <= 0 {
			break
		}

		remaining, ok := remainingRequested[priority]
		if !ok {
			continue
		}

		if remaining == nil || len(remaining) == 0 {
			continue
		}

		remainingAmount = divideRemainingResource(remainingAmount, remainingRequested[priority], resourceName)
	}

	return remainingAmount
}

func getQueuesByPriority(queues map[common_info.QueueID]*rs.QueueAttributes) (map[int]map[common_info.QueueID]*rs.QueueAttributes, []int) {
	queuesByPriority := map[int]map[common_info.QueueID]*rs.QueueAttributes{}
	for id, queue := range queues {
		priority := queue.Priority
		if _, ok := queuesByPriority[priority]; !ok {
			queuesByPriority[priority] = map[common_info.QueueID]*rs.QueueAttributes{}
		}
		queuesByPriority[priority][id] = queue
	}

	priorities := maps.Keys(queuesByPriority)
	slices.SortFunc(priorities, func(i, j int) int {
		return j - i
	})

	return queuesByPriority, priorities
}

func divideUpToFairShare(totalResourceAmount, kValue float64, queues map[common_info.QueueID]*rs.QueueAttributes,
	resourceName rs.ResourceName) (remainingAmount float64, remainingRequested map[common_info.QueueID]*remainingRequestedResource) {
	remainingRequested = map[common_info.QueueID]*remainingRequestedResource{}

	for {
		shouldRunAnotherRound := false
		amountToGiveInCurrentRound := totalResourceAmount
		totalWeights, totalUsages := getTotalWeightsForUnsatisfied(queues, resourceName)
		if totalWeights == 0 {
			break
		}

		if totalUsages == 0 {
			totalUsages = 1
		}

		portions := make(map[common_info.QueueID]float64)
		totalPortions := 0.0
		for _, queue := range queues {
			share := queue.ResourceShare(resourceName)
			if share.Request <= share.FairShare {
				// queue is satisfied, no need to give it more resources
				continue
			}

			// Normalize queue over quota weight
			nWeight := share.OverQuotaWeight / totalWeights

			// We assume that usage is normalized to usage/clusterCapacity
			nUsage := share.GetUsage()

			portion := nWeight + kValue*(nWeight-nUsage)
			portions[queue.UID] = portion
			totalPortions += portion
		}

		for _, queue := range queues {
			requested := getRemainingRequested(queue, resourceName)
			if requested == 0 {
				continue
			}

			if totalResourceAmount == 0 {
				log.InfraLogger.V(7).Infof("no more resources, exiting")
				break
			}

			resourceShare := queue.ResourceShare(resourceName)
			overQuotaWeight := resourceShare.OverQuotaWeight
			if overQuotaWeight == 0 {
				continue
			}

			log.InfraLogger.V(6).Infof("calculating %v resource fair share for %v: deserved: %v, "+
				"remaining requested: %v, fairShare: %v",
				resourceName, queue.Name, resourceShare.Deserved, requested, resourceShare.FairShare)

			// normalize portion
			portion := portions[queue.UID]
			nPortion := portion / totalPortions

			fairShare := amountToGiveInCurrentRound * nPortion
			resourceToGive := getResourceToGiveInCurrentRound(fairShare, requested, queue, remainingRequested)
			if resourceToGive == 0 {
				continue
			}

			queue.AddResourceShare(resourceName, resourceToGive)
			totalResourceAmount -= resourceToGive

			// if a project/department requested less resources that their fairShare, some resources remained unassigned,
			// we can run another round and assign them to other projects/departments that still request more resources
			shouldRunAnotherRound = shouldRunAnotherRound || requested < fairShare
		}

		if !shouldRunAnotherRound || totalResourceAmount == 0 {
			break
		}
	}

	return totalResourceAmount, remainingRequested
}

func divideRemainingResource(totalResourceAmount float64, remainingRequested map[common_info.QueueID]*remainingRequestedResource,
	resourceName rs.ResourceName) (remainingAmount float64) {
	// The purpose of this part is to divide the remaining resources that were rounded down to fairShare and were not given, there will be at most len(queues) of these
	sortedQueues := sortByOverQuotaWeight(remainingRequested)
	for {
		if totalResourceAmount == 0 || sortedQueues.Empty() {
			break
		}

		largestRemaining := sortedQueues.Pop().(*remainingRequestedResource)

		// totalResourceAmount can be fraction (less than 1) if some queue requested a fraction of GPU
		resourceToGive := math.Min(1, totalResourceAmount)
		largestRemaining.queue.AddResourceShare(resourceName, resourceToGive)
		totalResourceAmount -= resourceToGive
	}
	return totalResourceAmount
}

func getResourceToGiveInCurrentRound(fairShare float64, requested float64, queue *rs.QueueAttributes,
	remainingRequested map[common_info.QueueID]*remainingRequestedResource) float64 {
	resourcesToGive := float64(0)
	if requested <= fairShare {
		resourcesToGive = requested
		log.InfraLogger.V(7).Infof("%v received %v resources and is satisfied", queue.Name, resourcesToGive)
		delete(remainingRequested, queue.UID)
	} else {
		// for better usability, when we limit the fairShare that a project/departments receives, it is always done in round numbers
		roundFairShare := math.Floor(fairShare)
		if roundFairShare > 0 {
			resourcesToGive = roundFairShare
			log.InfraLogger.V(7).Infof("%v received its' fairShare of %v resources in current round, but still not satisfied", queue.Name, resourcesToGive)
		}
		if fairShare-resourcesToGive > 0 {
			remainingRequested[queue.UID] = &remainingRequestedResource{
				queue:           queue,
				remainingAmount: fairShare - resourcesToGive,
			}
		}
	}
	return resourcesToGive
}

func getTotalWeightsForUnsatisfied(queues map[common_info.QueueID]*rs.QueueAttributes, resourceName rs.ResourceName) (totalOverQuotaWeights, totalUsages float64) {
	for _, queue := range queues {
		remainingRequested := getRemainingRequested(queue, resourceName)
		if remainingRequested <= 0 {
			continue
		}
		totalOverQuotaWeights += queue.ResourceShare(resourceName).OverQuotaWeight

		queueUsage := queue.ResourceShare(resourceName).GetUsage()
		if queueUsage < 0 {
			log.InfraLogger.V(1).Warnf("queue <%v> has negative usage score of <%v> for resource <%v>, expected non-negative",
				queue.Name, queueUsage, resourceName)
			continue
		}

		totalUsages += queueUsage
	}
	return totalOverQuotaWeights, totalUsages
}

func getRemainingRequested(queue *rs.QueueAttributes, resourceName rs.ResourceName) float64 {
	resourceShare := queue.ResourceShare(resourceName)
	requested := resourceShare.GetRequestableShare()
	fairShare := resourceShare.FairShare
	if requested < fairShare {
		return 0
	}
	return requested - fairShare
}

func sortByOverQuotaWeight(remainingRequested map[common_info.QueueID]*remainingRequestedResource) *scheduler_util.PriorityQueue {
	sortedGroupQueues := scheduler_util.NewPriorityQueue(remainingRequestedOrderFn(), scheduler_util.QueueCapacityInfinite)
	for _, remaining := range remainingRequested {
		sortedGroupQueues.Push(remaining)
	}
	return sortedGroupQueues
}

func remainingRequestedOrderFn() func(lQ, rQ interface{}) bool {
	return func(lH, rH interface{}) bool {
		lRemaining := lH.(*remainingRequestedResource)
		rRemaining := rH.(*remainingRequestedResource)
		lRemainingAmount := lRemaining.remainingAmount
		rRemainingAmount := rRemaining.remainingAmount
		if lRemainingAmount > rRemainingAmount {
			return true // means lQueue will be popped before rQueue
		}

		if lRemainingAmount < rRemainingAmount {
			return false
		}

		// Give priority to the project/department that was created first
		if !lRemaining.queue.CreationTimestamp.Equal(&rRemaining.queue.CreationTimestamp) {
			return lRemaining.queue.CreationTimestamp.Before(&rRemaining.queue.CreationTimestamp)
		}

		// Alphabetical order for consistent result
		return string(lRemaining.queue.UID) < string(rRemaining.queue.UID)
	}
}
