// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"fmt"

	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/resource_info"
)

const (
	NodePodNumberExceeded = "node(s) pod number exceeded"
)

func GetBuildOverCapacityMessageForQueue(queueName string, resourceName string, deserved, used float64,
	requiredResources *podgroup_info.JobRequirement) string {
	details := getOverCapacityMessageDetails(queueName, resourceName, deserved, used, requiredResources)
	return fmt.Sprintf("Non-preemptible workload is over quota. "+
		"%v Use a preemptible workload to go over quota.", details)
}

func getOverCapacityMessageDetails(queueName, resourceName string, deserved, used float64,
	requiredResources *podgroup_info.JobRequirement) string {
	switch resourceName {
	case GpuResource:
		return fmt.Sprintf("Workload requested %v GPUs, but %s quota is %v GPUs, "+
			"while %v GPUs are already allocated for non-preemptible pods.",
			requiredResources.GPU,
			queueName,
			resource_info.HumanizeResource(deserved, 1),
			resource_info.HumanizeResource(used, 1),
		)
	case CpuResource:
		return fmt.Sprintf("Workload requested %v CPU cores, but %s quota is %v cores, "+
			"while %v cores are already allocated for non-preemptible pods.",
			resource_info.HumanizeResource(requiredResources.MilliCPU, resource_info.MilliCPUToCores),
			queueName,
			resource_info.HumanizeResource(deserved, resource_info.MilliCPUToCores),
			resource_info.HumanizeResource(used, resource_info.MilliCPUToCores),
		)
	case MemoryResource:
		return fmt.Sprintf("Workload requested %v GB memory, but %s quota is %v GB, "+
			"while %v GB are already allocated for non-preemptible pods.",
			resource_info.HumanizeResource(requiredResources.Memory, resource_info.MemoryToGB),
			queueName,
			resource_info.HumanizeResource(deserved, resource_info.MemoryToGB),
			resource_info.HumanizeResource(used, resource_info.MemoryToGB),
		)
	default:
		return ""
	}
}

func GetJobOverMaxAllowedMessageForQueue(
	queueName string, resourceName string, maxAllowed, used, requested float64,
) string {
	var resourceNameStr, details string
	switch resourceName {
	case GpuResource:
		resourceNameStr = "GPUs"
		details = fmt.Sprintf("Limit is %s GPUs, currently %s GPUs allocated and workload requested %s GPUs",
			resource_info.HumanizeResource(maxAllowed, 1),
			resource_info.HumanizeResource(used, 1),
			resource_info.HumanizeResource(requested, 1))
	case CpuResource:
		resourceNameStr = "CPU cores"
		details = fmt.Sprintf("Limit is %s cores, currently %s cores allocated and workload requested %s cores",
			resource_info.HumanizeResource(maxAllowed, resource_info.MilliCPUToCores),
			resource_info.HumanizeResource(used, resource_info.MilliCPUToCores),
			resource_info.HumanizeResource(requested, resource_info.MilliCPUToCores))
	case MemoryResource:
		resourceNameStr = "memory"
		details = fmt.Sprintf("Limit is %s GB, currently %s GB allocated and workload requested %s GB",
			resource_info.HumanizeResource(maxAllowed, resource_info.MemoryToGB),
			resource_info.HumanizeResource(used, resource_info.MemoryToGB),
			resource_info.HumanizeResource(requested, resource_info.MemoryToGB))
	}
	return fmt.Sprintf("%s quota has reached the allowable limit of %s. %s",
		queueName, resourceNameStr, details)
}

func GetGangEvictionMessage(task *pod_info.PodInfo, job *podgroup_info.PodGroupInfo) string {
	if len(job.GetSubGroups()) == 1 {
		if defaultSubgroup, found := job.GetSubGroups()[podgroup_info.DefaultSubGroup]; found {
			return getGangEvictionForDefaultSubgroupMessage(task.Namespace, task.Name, defaultSubgroup.GetMinAvailable())
		}
	}
	subGroup := job.GetSubGroups()[task.SubGroupName]
	return getGangEvictionWithSubGroupsMessage(task.Namespace, task.Name, subGroup.GetName(), subGroup.GetMinAvailable())
}

func getGangEvictionForDefaultSubgroupMessage(taskNamespace, taskName string, minimum int32) string {
	return fmt.Sprintf(
		"Workload doesn't have the minimum required number of pods (%d), evicting remaining pod: %s/%s",
		minimum, taskNamespace, taskName)
}

func getGangEvictionWithSubGroupsMessage(taskNamespace, taskName, subGroup string, minimum int32) string {
	return fmt.Sprintf(
		"Workload doesn't have the minimum required number of pods (%d) on sub-group %s, evicting remaining pod: %s/%s",
		minimum, subGroup, taskNamespace, taskName)
}

func GetPreemptMessage(preemptorJob *podgroup_info.PodGroupInfo, preempteeTask *pod_info.PodInfo) string {
	return fmt.Sprintf("Pod %s/%s was preempted by higher priority workload %s/%s", preempteeTask.Namespace,
		preempteeTask.Name, preemptorJob.Namespace, preemptorJob.Name)
}

func GetReclaimMessage(reclaimeeTask *pod_info.PodInfo, reclaimerJob *podgroup_info.PodGroupInfo) string {
	return fmt.Sprintf("Pod %s/%s was preempted by workload %s/%s.",
		reclaimeeTask.Namespace, reclaimeeTask.Name, reclaimerJob.Namespace, reclaimerJob.Name)
}

func GetReclaimQueueDetailsMessage(queueName string, queueAllocated *resource_info.ResourceRequirements,
	queueQuota *resource_info.ResourceRequirements, queueFairShare *resource_info.ResourceRequirements, queuePriority int) string {
	return fmt.Sprintf("%s uses <%s> with a quota of <%s>, fair-share of <%s> and queue priority of <%d>.",
		queueName, queueAllocated, queueQuota, queueFairShare, queuePriority)
}

func GetConsolidateMessage(preempteeTask *pod_info.PodInfo) string {
	return fmt.Sprintf(
		"Pod %s/%s was preempted and rescheduled due to bin packing (resource consolidation) procedure",
		preempteeTask.Namespace, preempteeTask.Name)
}
