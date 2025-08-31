// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package cluster_info

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
	v12 "k8s.io/api/scheduling/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"

	kueuev1alpha1 "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	kueueclient "sigs.k8s.io/kueue/client-go/clientset/versioned"
	kueuefake "sigs.k8s.io/kueue/client-go/clientset/versioned/fake"
	kueueinformer "sigs.k8s.io/kueue/client-go/informers/externalversions"

	kubeAiSchedulerClient "github.com/NVIDIA/KAI-scheduler/pkg/apis/client/clientset/versioned"
	kubeAiSchedulerClientFake "github.com/NVIDIA/KAI-scheduler/pkg/apis/client/clientset/versioned/fake"
	kubeAiSchedulerInfo "github.com/NVIDIA/KAI-scheduler/pkg/apis/client/informers/externalversions"
	schedulingv1alpha2 "github.com/NVIDIA/KAI-scheduler/pkg/apis/scheduling/v1alpha2"
	enginev2 "github.com/NVIDIA/KAI-scheduler/pkg/apis/scheduling/v2"
	enginev2alpha2 "github.com/NVIDIA/KAI-scheduler/pkg/apis/scheduling/v2alpha2"
	commonconstants "github.com/NVIDIA/KAI-scheduler/pkg/common/constants"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/pod_affinity"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/queue_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/resource_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/storageclaim_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/storageclass_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/cache/cluster_info/data_lister"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/cache/usagedb"
	fakeusage "github.com/NVIDIA/KAI-scheduler/pkg/scheduler/cache/usagedb/fake"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/conf"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/utils"
)

const (
	successErrorMsg     = "SUCCESS"
	nodePoolNameLabel   = "kai.scheduler/node-pool"
	defaultNodePoolName = "default"
)

func TestSnapshot(t *testing.T) {
	tests := map[string]struct {
		kubeObjects          []runtime.Object
		kaiSchedulerObjects  []runtime.Object
		kueueObjects         []runtime.Object
		expectedNodes        int
		expectedDepartments  int
		expectedQueues       int
		expectedBindRequests int
	}{
		"SingleFromEach": {
			kubeObjects: []runtime.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-1",
					},
				},
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-pod",
						Namespace: "my-ns",
					},
				},
			},
			kaiSchedulerObjects: []runtime.Object{
				&enginev2.Queue{
					ObjectMeta: metav1.ObjectMeta{
						Name: "my-department",
					},
					Spec: enginev2.QueueSpec{
						Resources: &enginev2.QueueResources{},
					},
				},
				&enginev2.Queue{
					ObjectMeta: metav1.ObjectMeta{
						Name: "my-queue",
					},
					Spec: enginev2.QueueSpec{
						ParentQueue: "my-department",
					},
				},
				&schedulingv1alpha2.BindRequest{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-pod-1234",
						Namespace: "my-ns",
					},
					Spec: schedulingv1alpha2.BindRequestSpec{
						PodName:      "my-pod",
						SelectedNode: "node-1",
					},
				},
			},
			expectedNodes:        1,
			expectedQueues:       2,
			expectedBindRequests: 1,
		},
		"SingleFromEach2": {
			kubeObjects: []runtime.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-1",
					},
				},
			},
			kaiSchedulerObjects: []runtime.Object{
				&enginev2.Queue{
					ObjectMeta: metav1.ObjectMeta{
						Name: "my-department",
					},
					Spec: enginev2.QueueSpec{
						Resources: &enginev2.QueueResources{},
					},
				},
				&enginev2.Queue{
					ObjectMeta: metav1.ObjectMeta{
						Name: "my-queue",
					},
					Spec: enginev2.QueueSpec{
						ParentQueue: "my-department",
					},
				},
			},
			expectedNodes:  1,
			expectedQueues: 2,
		},
	}

	for name, test := range tests {
		t.Logf("Running test %s", name)
		clusterInfo := newClusterInfoTests(t, clusterInfoTestParams{
			kubeObjects:         test.kubeObjects,
			kaiSchedulerObjects: test.kaiSchedulerObjects,
			kueueObjects:        test.kueueObjects,
		})
		snapshot, err := clusterInfo.Snapshot()
		assert.Equal(t, nil, err)
		assert.Equal(t, test.expectedNodes, len(snapshot.Nodes))
		assert.Equal(t, test.expectedQueues, len(snapshot.Queues))

		assert.Equal(t, test.expectedBindRequests, len(snapshot.BindRequests))
	}
}

func TestSnapshotUsage(t *testing.T) {
	tests := []struct {
		name  string
		usage *queue_info.ClusterUsage
		err   error

		expectedUsage *queue_info.ClusterUsage
	}{
		{
			name: "BasicUsage",
			usage: &queue_info.ClusterUsage{
				Queues: map[common_info.QueueID]queue_info.QueueUsage{
					"queue-1": {
						corev1.ResourceCPU:          10,
						corev1.ResourceMemory:       10,
						commonconstants.GpuResource: 10,
					},
				},
			},
			err: nil,
			expectedUsage: &queue_info.ClusterUsage{
				Queues: map[common_info.QueueID]queue_info.QueueUsage{
					"queue-1": {
						corev1.ResourceCPU:          10,
						corev1.ResourceMemory:       10,
						commonconstants.GpuResource: 10,
					},
				},
			},
		},
		{
			name:          "Error only",
			usage:         nil,
			err:           fmt.Errorf("error"),
			expectedUsage: &queue_info.ClusterUsage{},
		},
		{
			name: "Error and usage",
			usage: &queue_info.ClusterUsage{
				Queues: map[common_info.QueueID]queue_info.QueueUsage{
					"queue-1": {
						corev1.ResourceCPU:          11,
						corev1.ResourceMemory:       11,
						commonconstants.GpuResource: 11,
					},
				},
			},
			err:           fmt.Errorf("error"),
			expectedUsage: &queue_info.ClusterUsage{},
		},
	}

	compareUsage := func(t *testing.T, expected, actual *queue_info.ClusterUsage) {
		if expected == nil {
			assert.Nil(t, actual)
			return
		}
		assert.NotNil(t, actual)
		assert.Equal(t, len(expected.Queues), len(actual.Queues))
		for queueID, expectedUsage := range expected.Queues {
			actualUsage, ok := actual.Queues[queueID]
			assert.True(t, ok)
			assert.Equal(t, expectedUsage, actualUsage)
		}
	}

	for i, test := range tests {
		t.Logf("Running test %d: %s", i, test.name)
		clusterInfo := newClusterInfoTests(t, clusterInfoTestParams{
			kubeObjects:         []runtime.Object{},
			kaiSchedulerObjects: []runtime.Object{},
			kueueObjects:        []runtime.Object{},
			clusterUsage:        test.usage,
			clusterUsageErr:     test.err,
		})
		snapshot, err := clusterInfo.Snapshot()
		assert.Equal(t, nil, err)
		usage := snapshot.QueueResourceUsage
		compareUsage(t, test.expectedUsage, &usage)
	}
}

func TestSnapshotNodes(t *testing.T) {
	examplePod := &corev1.Pod{
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							"cpu": resource.MustParse("2"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	exampleMIGPod := examplePod.DeepCopy()
	exampleMIGPod.Name = "mig-pod"
	exampleMIGPod.Spec.Containers[0].Resources.Requests["nvidia.com/mig-1g.5gb"] = resource.MustParse("2")
	exampleMIGPodWithPG := examplePod.DeepCopy()
	exampleMIGPodWithPG.Name = "mig-pod-with-pg"
	exampleMIGPodWithPG.Annotations = map[string]string{
		commonconstants.PodGroupAnnotationForPod: "pg-1",
	}
	exampleMIGPodWithPG.Spec.Containers[0].Resources.Requests["nvidia.com/mig-1g.5gb"] = resource.MustParse("2")
	tests := map[string]struct {
		objs          []runtime.Object
		resultNodes   []*node_info.NodeInfo
		resultPodsLen int
		nodePoolName  string
	}{
		"BasicUsage": {
			objs: []runtime.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-1",
					},
					Status: corev1.NodeStatus{
						Allocatable: corev1.ResourceList{
							"cpu": resource.MustParse("10"),
						},
					},
				},
				examplePod,
			},
			resultNodes: []*node_info.NodeInfo{
				{
					Name: "node-1",
					Idle: resource_info.ResourceFromResourceList(
						corev1.ResourceList{
							"cpu": resource.MustParse("8"),
						},
					),
					Used: resource_info.ResourceFromResourceList(
						corev1.ResourceList{
							"cpu":    resource.MustParse("2"),
							"memory": resource.MustParse("0"),
						},
					),
					Releasing: resource_info.ResourceFromResourceList(
						corev1.ResourceList{
							"cpu":    resource.MustParse("0"),
							"memory": resource.MustParse("0"),
						},
					),
				},
			},
			resultPodsLen: 1,
		},
		"Finished job": {
			objs: []runtime.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-1",
					},
					Status: corev1.NodeStatus{
						Allocatable: corev1.ResourceList{
							"cpu": resource.MustParse("10"),
						},
					},
				},
				newCompletedPod(examplePod),
			},
			resultNodes: []*node_info.NodeInfo{
				{
					Name: "node-1",
					Idle: resource_info.ResourceFromResourceList(
						corev1.ResourceList{
							"cpu": resource.MustParse("10"),
						},
					),
					Used: resource_info.ResourceFromResourceList(
						corev1.ResourceList{
							"cpu":    resource.MustParse("0"),
							"memory": resource.MustParse("0"),
						},
					),
					Releasing: resource_info.ResourceFromResourceList(
						corev1.ResourceList{
							"cpu":    resource.MustParse("0"),
							"memory": resource.MustParse("0"),
						},
					),
				},
			},
			resultPodsLen: 1,
		},
		"Filter Pods by nodepool": {
			objs: []runtime.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-1",
						Labels: map[string]string{
							defaultNodePoolName: "pool-a",
						},
					},
					Status: corev1.NodeStatus{
						Allocatable: corev1.ResourceList{
							"cpu": resource.MustParse("10"),
						},
					},
				},
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-2",
						Labels: map[string]string{
							defaultNodePoolName: "pool-b",
						},
					},
					Status: corev1.NodeStatus{
						Allocatable: corev1.ResourceList{
							"cpu": resource.MustParse("10"),
						},
					},
				},
				newPodOnNode(examplePod, "node-1"),
				newPodOnNode(examplePod, "node-2"),
			},
			resultNodes: []*node_info.NodeInfo{
				{
					Name: "node-1",
					Idle: resource_info.ResourceFromResourceList(
						corev1.ResourceList{
							"cpu": resource.MustParse("8"),
						},
					),
					Used: resource_info.ResourceFromResourceList(
						corev1.ResourceList{
							"cpu":    resource.MustParse("2"),
							"memory": resource.MustParse("0"),
						},
					),
					Releasing: resource_info.ResourceFromResourceList(
						corev1.ResourceList{
							"cpu":    resource.MustParse("0"),
							"memory": resource.MustParse("0"),
						},
					),
				},
			},
			resultPodsLen: 1,
			nodePoolName:  "pool-a",
		},
		"MIG Job": {
			objs: []runtime.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-1",
					},
					Status: corev1.NodeStatus{
						Allocatable: corev1.ResourceList{
							"cpu":                   resource.MustParse("10"),
							"nvidia.com/mig-1g.5gb": resource.MustParse("10"),
						},
					},
				},
				exampleMIGPod,
				exampleMIGPodWithPG,
			},
			resultNodes: []*node_info.NodeInfo{
				{
					Name: "node-1",
					Idle: resource_info.ResourceFromResourceList(
						corev1.ResourceList{
							"cpu":                   resource.MustParse("6"),
							"nvidia.com/mig-1g.5gb": resource.MustParse("6"),
						},
					),
					Used: resource_info.ResourceFromResourceList(
						corev1.ResourceList{
							"cpu":                   resource.MustParse("4"),
							"memory":                resource.MustParse("0"),
							"nvidia.com/mig-1g.5gb": resource.MustParse("4"),
						},
					),
					Releasing: resource_info.ResourceFromResourceList(
						corev1.ResourceList{
							"cpu":    resource.MustParse("0"),
							"memory": resource.MustParse("0"),
						},
					),
				},
			},
			resultPodsLen: 2,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			clusterInfo := newClusterInfoTestsInner(
				t, test.objs, []runtime.Object{}, []runtime.Object{},
				&conf.SchedulingNodePoolParams{
					NodePoolLabelKey:   defaultNodePoolName,
					NodePoolLabelValue: test.nodePoolName,
				},
				true,
				nil, nil, // usage and usageErr
			)
			existingPods := map[common_info.PodID]*pod_info.PodInfo{}

			controller := gomock.NewController(t)
			clusterPodAffinityInfo := pod_affinity.NewMockClusterPodAffinityInfo(controller)
			clusterPodAffinityInfo.EXPECT().UpdateNodeAffinity(gomock.Any()).AnyTimes()
			clusterPodAffinityInfo.EXPECT().AddNode(gomock.Any(), gomock.Any()).AnyTimes()

			allPods, _ := clusterInfo.dataLister.ListPods()
			nodes, err := clusterInfo.snapshotNodes(clusterPodAffinityInfo)
			if err != nil {
				assert.FailNow(t, fmt.Sprintf("SnapshotNode got error in test %s", t.Name()), err)
			}
			pods, err := clusterInfo.addTasksToNodes(allPods, existingPods, nodes, nil)

			assert.Equal(t, len(test.resultNodes), len(nodes))
			assert.Equal(t, test.resultPodsLen, len(pods))

			for _, expectedNode := range test.resultNodes {
				assert.Equal(t, expectedNode.Idle, nodes[expectedNode.Name].Idle, "Expected idle resources to be equal")
				assert.Equal(t, expectedNode.Used, nodes[expectedNode.Name].Used, "Expected used resources to be equal")
				assert.Equal(t, expectedNode.Releasing, nodes[expectedNode.Name].Releasing, "Expected releasing resources to be equal")
			}
		})
	}
}

func TestBindRequests(t *testing.T) {
	examplePodName := "pod-1"
	namespace1 := "namespace-1"
	podGroupName := "podgroup-1"
	examplePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      examplePodName,
			Namespace: namespace1,
			UID:       types.UID(examplePodName),
			Annotations: map[string]string{
				commonconstants.PodGroupAnnotationForPod: podGroupName,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							"cpu": resource.MustParse("2"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}

	exampleQueue := &enginev2.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "queue-0",
		},
	}

	tests := map[string]struct {
		kubeObjects             []runtime.Object
		kaiSchedulerObjects     []runtime.Object
		kueueObjects            []runtime.Object
		expectedProcessing      int
		expectedStale           int
		expectedForDeletedNodes int
		expectedPodStatus       map[string]map[string]pod_status.PodStatus
		resultNodes             map[string]*resource.Quantity
	}{
		"Pod with PodGroup Waiting For Binding": {
			kubeObjects: []runtime.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-1",
					},
					Status: corev1.NodeStatus{
						Allocatable: corev1.ResourceList{
							"cpu": resource.MustParse("10"),
						},
					},
				},
				examplePod,
			},
			kaiSchedulerObjects: []runtime.Object{
				exampleQueue,
				&enginev2alpha2.PodGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podGroupName,
						Namespace: namespace1,
					},
					Spec: enginev2alpha2.PodGroupSpec{
						Queue: exampleQueue.Name,
					},
				},
				&schedulingv1alpha2.BindRequest{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-pod-1234",
						Namespace: namespace1,
					},
					Spec: schedulingv1alpha2.BindRequestSpec{
						PodName:      examplePodName,
						SelectedNode: "node-1",
					},
				},
			},
			expectedProcessing: 1,
			expectedPodStatus: map[string]map[string]pod_status.PodStatus{
				namespace1: {
					examplePodName: pod_status.Binding,
				},
			},
			resultNodes: map[string]*resource.Quantity{
				"node-1": ptr.To(resource.MustParse("8")),
			},
		},
		"Pod with PodGroup Waiting For Binding that is failing but no at backoff limit": {
			kubeObjects: []runtime.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-1",
					},
					Status: corev1.NodeStatus{
						Allocatable: corev1.ResourceList{
							"cpu": resource.MustParse("10"),
						},
					},
				},
				examplePod,
			},
			kaiSchedulerObjects: []runtime.Object{
				exampleQueue,
				&enginev2alpha2.PodGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podGroupName,
						Namespace: namespace1,
					},
					Spec: enginev2alpha2.PodGroupSpec{
						Queue: exampleQueue.Name,
					},
				},
				&schedulingv1alpha2.BindRequest{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-pod-1234",
						Namespace: namespace1,
					},
					Spec: schedulingv1alpha2.BindRequestSpec{
						PodName:      examplePodName,
						SelectedNode: "node-1",
						BackoffLimit: ptr.To(int32(5)),
					},
					Status: schedulingv1alpha2.BindRequestStatus{
						Phase:          schedulingv1alpha2.BindRequestPhaseFailed,
						FailedAttempts: 2,
					},
				},
			},
			expectedProcessing: 1,
			expectedPodStatus: map[string]map[string]pod_status.PodStatus{
				namespace1: {
					examplePodName: pod_status.Binding,
				},
			},
			resultNodes: map[string]*resource.Quantity{
				"node-1": ptr.To(resource.MustParse("8")),
			},
		},
		"Pod with PodGroup Waiting For Binding that is failing and reached backoff limit": {
			kubeObjects: []runtime.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-1",
					},
					Status: corev1.NodeStatus{
						Allocatable: corev1.ResourceList{
							"cpu": resource.MustParse("10"),
						},
					},
				},
				examplePod,
			},
			kaiSchedulerObjects: []runtime.Object{
				exampleQueue,
				&enginev2alpha2.PodGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podGroupName,
						Namespace: namespace1,
					},
					Spec: enginev2alpha2.PodGroupSpec{
						Queue: exampleQueue.Name,
					},
				},
				&schedulingv1alpha2.BindRequest{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-pod-1234",
						Namespace: namespace1,
					},
					Spec: schedulingv1alpha2.BindRequestSpec{
						PodName:      examplePodName,
						SelectedNode: "node-1",
						BackoffLimit: ptr.To(int32(5)),
					},
					Status: schedulingv1alpha2.BindRequestStatus{
						Phase:          schedulingv1alpha2.BindRequestPhaseFailed,
						FailedAttempts: 5,
					},
				},
			},
			expectedProcessing: 0,
			expectedStale:      1,
			expectedPodStatus: map[string]map[string]pod_status.PodStatus{
				namespace1: {
					examplePodName: pod_status.Pending,
				},
			},
			resultNodes: map[string]*resource.Quantity{
				"node-1": ptr.To(resource.MustParse("10")),
			},
		},
		"Pod pending and BindRequest to a different pod": {
			kubeObjects: []runtime.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-1",
					},
					Status: corev1.NodeStatus{
						Allocatable: corev1.ResourceList{
							"cpu": resource.MustParse("10"),
						},
					},
				},
				examplePod,
				func() *corev1.Pod {
					pod := examplePod.DeepCopy()
					pod.Name = "not-" + examplePod.Name
					pod.UID = types.UID(fmt.Sprintf("not-%s", examplePod.UID))
					return pod
				}(),
			},
			kaiSchedulerObjects: []runtime.Object{
				exampleQueue,
				&enginev2alpha2.PodGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podGroupName,
						Namespace: namespace1,
					},
					Spec: enginev2alpha2.PodGroupSpec{
						Queue: exampleQueue.Name,
					},
				},
				&schedulingv1alpha2.BindRequest{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("not-%s-1234", examplePodName),
						Namespace: namespace1,
					},
					Spec: schedulingv1alpha2.BindRequestSpec{
						PodName:      fmt.Sprintf("not-%s", examplePodName),
						SelectedNode: "node-1",
					},
				},
			},
			expectedProcessing: 1,
			expectedStale:      0,
			expectedPodStatus: map[string]map[string]pod_status.PodStatus{
				namespace1: {
					examplePodName: pod_status.Pending,
				},
			},
			resultNodes: map[string]*resource.Quantity{
				"node-1": ptr.To(resource.MustParse("8")),
			},
		},
		"Pod pending and BindRequest to non existing node and is failed": {
			kubeObjects: []runtime.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-1",
					},
					Status: corev1.NodeStatus{
						Allocatable: corev1.ResourceList{
							"cpu": resource.MustParse("10"),
						},
					},
				},
				examplePod,
			},
			kaiSchedulerObjects: []runtime.Object{
				exampleQueue,
				&enginev2alpha2.PodGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podGroupName,
						Namespace: namespace1,
					},
					Spec: enginev2alpha2.PodGroupSpec{
						Queue: exampleQueue.Name,
					},
				},
				&schedulingv1alpha2.BindRequest{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-pod-1234",
						Namespace: namespace1,
					},
					Spec: schedulingv1alpha2.BindRequestSpec{
						PodName:      examplePodName,
						SelectedNode: "node-2",
						BackoffLimit: ptr.To(int32(5)),
					},
					Status: schedulingv1alpha2.BindRequestStatus{
						Phase:          schedulingv1alpha2.BindRequestPhaseFailed,
						FailedAttempts: 5,
					},
				},
			},
			expectedStale:           0,
			expectedForDeletedNodes: 1,
			expectedPodStatus: map[string]map[string]pod_status.PodStatus{
				namespace1: {
					examplePodName: pod_status.Pending,
				},
			},
			resultNodes: map[string]*resource.Quantity{
				"node-1": ptr.To(resource.MustParse("10")),
			},
		},
		"Pod pending with stale bind request from another shard and node is not in our shard": {
			kubeObjects: []runtime.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-1",
					},
					Status: corev1.NodeStatus{
						Allocatable: corev1.ResourceList{
							"cpu": resource.MustParse("10"),
						},
					},
				},
				examplePod,
			},
			kaiSchedulerObjects: []runtime.Object{
				exampleQueue,
				&enginev2alpha2.PodGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podGroupName,
						Namespace: namespace1,
					},
					Spec: enginev2alpha2.PodGroupSpec{
						Queue: exampleQueue.Name,
					},
				},
				&schedulingv1alpha2.BindRequest{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-pod-1234",
						Namespace: namespace1,
						Labels: map[string]string{
							nodePoolNameLabel: "other-value",
						},
					},
					Spec: schedulingv1alpha2.BindRequestSpec{
						PodName:      examplePodName,
						SelectedNode: "node-2",
					},
				},
			},
			expectedProcessing: 0,
			expectedPodStatus: map[string]map[string]pod_status.PodStatus{
				namespace1: {
					examplePodName: pod_status.Pending,
				},
			},
			resultNodes: map[string]*resource.Quantity{
				"node-1": ptr.To(resource.MustParse("10")),
			},
		},
		"Pod pending with stale bind request from another shard and node is actually in our shard": {
			kubeObjects: []runtime.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-1",
					},
					Status: corev1.NodeStatus{
						Allocatable: corev1.ResourceList{
							"cpu": resource.MustParse("10"),
						},
					},
				},
				examplePod,
			},
			kaiSchedulerObjects: []runtime.Object{
				exampleQueue,
				&enginev2alpha2.PodGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podGroupName,
						Namespace: namespace1,
					},
					Spec: enginev2alpha2.PodGroupSpec{
						Queue: exampleQueue.Name,
					},
				},
				&schedulingv1alpha2.BindRequest{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-pod-1234",
						Namespace: namespace1,
						Labels: map[string]string{
							nodePoolNameLabel: "other-value",
						},
					},
					Spec: schedulingv1alpha2.BindRequestSpec{
						PodName:      examplePodName,
						SelectedNode: "node-1",
					},
					Status: schedulingv1alpha2.BindRequestStatus{
						Phase: schedulingv1alpha2.BindRequestPhaseFailed,
					},
				},
			},
			expectedStale: 1,
			expectedPodStatus: map[string]map[string]pod_status.PodStatus{
				namespace1: {
					examplePodName: pod_status.Pending,
				},
			},
			resultNodes: map[string]*resource.Quantity{
				"node-1": ptr.To(resource.MustParse("10")),
			},
		},
	}

	for name, test := range tests {
		t.Logf("Running test %s", name)
		clusterInfo := newClusterInfoTests(t,
			clusterInfoTestParams{
				kubeObjects:         test.kubeObjects,
				kaiSchedulerObjects: test.kaiSchedulerObjects,
				kueueObjects:        test.kueueObjects,
			},
		)
		snapshot, err := clusterInfo.Snapshot()
		assert.Equal(t, nil, err)

		processingBindRequests := 0
		staleBindRequests := 0
		for _, bindRequest := range snapshot.BindRequests {
			if bindRequest.IsFailed() {
				staleBindRequests++
			} else {
				processingBindRequests++
			}
		}
		assert.Equal(t, test.expectedProcessing, processingBindRequests)
		assert.Equal(t, test.expectedStale, staleBindRequests)

		assert.Equal(t, test.expectedForDeletedNodes, len(snapshot.BindRequestsForDeletedNodes))

		assertedPods := 0
		for _, podGroup := range snapshot.PodGroupInfos {
			for _, podInfo := range podGroup.GetAllPodsMap() {
				byNamespace, found := test.expectedPodStatus[podInfo.Pod.Namespace]
				if !found {
					continue
				}
				expectedPodStatus, found := byNamespace[podInfo.Pod.Name]
				if !found {
					continue
				}
				assert.Equal(t, expectedPodStatus, podInfo.Status)
				assertedPods++
			}
		}

		expectedPodAsserts := 0
		for _, pods := range test.expectedPodStatus {
			expectedPodAsserts += len(pods)
		}
		assert.Equal(t, assertedPods, expectedPodAsserts)

		for _, node := range snapshot.Nodes {
			assert.Equal(t, float64(test.resultNodes[node.Name].MilliValue()), node.Idle.Cpu())
		}
	}
}

func TestSnapshotPodGroups(t *testing.T) {
	tests := map[string]struct {
		objs      []runtime.Object
		kubeObjs  []runtime.Object
		kueueObjs []runtime.Object
		results   []*podgroup_info.PodGroupInfo
	}{
		"BasicUsage": {
			objs: []runtime.Object{
				&enginev2alpha2.PodGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name: "podGroup-0",
						UID:  "ABC",
					},
					Spec: enginev2alpha2.PodGroupSpec{
						Queue: "queue-0",
					},
				},
			},
			kubeObjs: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							commonconstants.PodGroupAnnotationForPod: "podGroup-0",
						},
					},
				},
			},
			results: []*podgroup_info.PodGroupInfo{
				{
					Name:  "podGroup-0",
					Queue: "queue-0",
					SubGroups: map[string]*podgroup_info.SubGroupInfo{
						podgroup_info.DefaultSubGroup: podgroup_info.NewSubGroupInfo(podgroup_info.DefaultSubGroup, 1).WithPodInfos(pod_info.PodsMap{
							"test-pod": {
								UID: "test-pod",
							},
						}),
					},
				},
			},
		},
		"NotExistingQueue": {
			objs: []runtime.Object{
				&enginev2alpha2.PodGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name: "podGroup-0",
						UID:  "ABC",
					},
					Spec: enginev2alpha2.PodGroupSpec{
						Queue: "queue-1",
					},
				},
			},
			kubeObjs: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							commonconstants.PodGroupAnnotationForPod: "podGroup-0",
						},
					},
				},
			},
			results: []*podgroup_info.PodGroupInfo{},
		},
		"filter unassigned pod groups - no scheduling backoff": {
			objs: []runtime.Object{
				&enginev2alpha2.PodGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name: "podGroup-0",
						UID:  "ABC",
					},
					Spec: enginev2alpha2.PodGroupSpec{
						Queue:             "queue-0",
						SchedulingBackoff: ptr.To(int32(utils.NoSchedulingBackoff)),
					},
					Status: enginev2alpha2.PodGroupStatus{
						SchedulingConditions: []enginev2alpha2.SchedulingCondition{
							{
								NodePool: defaultNodePoolName,
							},
						},
					},
				},
			},
			results: []*podgroup_info.PodGroupInfo{
				{
					Name:  "podGroup-0",
					Queue: "queue-0",
					SubGroups: map[string]*podgroup_info.SubGroupInfo{
						podgroup_info.DefaultSubGroup: podgroup_info.NewSubGroupInfo(podgroup_info.DefaultSubGroup, 1),
					},
				},
			},
		},
		"filter unassigned pod groups - no scheduling conditions": {
			objs: []runtime.Object{
				&enginev2alpha2.PodGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name: "podGroup-0",
						UID:  "ABC",
					},
					Spec: enginev2alpha2.PodGroupSpec{
						Queue:             "queue-0",
						SchedulingBackoff: ptr.To(int32(utils.SingleSchedulingBackoff)),
					},
				},
			},
			results: []*podgroup_info.PodGroupInfo{
				{
					Name:  "podGroup-0",
					Queue: "queue-0",
					SubGroups: map[string]*podgroup_info.SubGroupInfo{
						podgroup_info.DefaultSubGroup: podgroup_info.NewSubGroupInfo(podgroup_info.DefaultSubGroup, 1),
					},
				},
			},
		},
		"filter unassigned pod groups - unschedulable in different nodepool": {
			objs: []runtime.Object{
				&enginev2alpha2.PodGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name: "podGroup-0",
						UID:  "ABC",
					},
					Spec: enginev2alpha2.PodGroupSpec{
						Queue:             "queue-0",
						SchedulingBackoff: ptr.To(int32(utils.SingleSchedulingBackoff)),
					},
					Status: enginev2alpha2.PodGroupStatus{
						SchedulingConditions: []enginev2alpha2.SchedulingCondition{
							{
								NodePool: "some-node-pool",
							},
						},
					},
				},
			},
			results: []*podgroup_info.PodGroupInfo{
				{
					Name:  "podGroup-0",
					Queue: "queue-0",
					SubGroups: map[string]*podgroup_info.SubGroupInfo{
						podgroup_info.DefaultSubGroup: podgroup_info.NewSubGroupInfo(podgroup_info.DefaultSubGroup, 1),
					},
				},
			},
		},
		"filter unassigned pod groups - unassigned": {
			objs: []runtime.Object{
				&enginev2alpha2.PodGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name: "podGroup-0",
						UID:  "ABC",
					},
					Spec: enginev2alpha2.PodGroupSpec{
						Queue:             "queue-0",
						SchedulingBackoff: ptr.To(int32(utils.SingleSchedulingBackoff)),
					},
					Status: enginev2alpha2.PodGroupStatus{
						SchedulingConditions: []enginev2alpha2.SchedulingCondition{
							{
								NodePool: defaultNodePoolName,
							},
						},
					},
				},
			},
			results: []*podgroup_info.PodGroupInfo{},
		},
		"With sub groups": {
			objs: []runtime.Object{
				&enginev2alpha2.PodGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name: "podGroup-0",
						UID:  "ABC",
					},
					Spec: enginev2alpha2.PodGroupSpec{
						Queue:     "queue-0",
						MinMember: 3,
						SubGroups: []enginev2alpha2.SubGroup{
							{
								Name:      "SubGroup-0",
								MinMember: 1,
							},
							{
								Name:      "SubGroup-1",
								MinMember: 2,
							},
						},
					},
					Status: enginev2alpha2.PodGroupStatus{},
				},
			},
			kubeObjs: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: testNamespace,
						Name:      "pod-0",
						UID:       types.UID(fmt.Sprintf("%s/pod-0", testNamespace)),
						Annotations: map[string]string{
							commonconstants.PodGroupAnnotationForPod: "podGroup-0",
						},
						Labels: map[string]string{
							commonconstants.SubGroupLabelKey: "SubGroup-0",
						},
					},
				},
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: testNamespace,
						Name:      "pod-1",
						UID:       types.UID(fmt.Sprintf("%s/pod-1", testNamespace)),
						Annotations: map[string]string{
							commonconstants.PodGroupAnnotationForPod: "podGroup-0",
						},
						Labels: map[string]string{
							commonconstants.SubGroupLabelKey: "SubGroup-1",
						},
					},
				},
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: testNamespace,
						Name:      "pod-2",
						UID:       types.UID(fmt.Sprintf("%s/pod-2", testNamespace)),
						Annotations: map[string]string{
							commonconstants.PodGroupAnnotationForPod: "podGroup-0",
						},
						Labels: map[string]string{
							commonconstants.SubGroupLabelKey: "SubGroup-1",
						},
					},
				},
			},
			results: []*podgroup_info.PodGroupInfo{
				func() *podgroup_info.PodGroupInfo {
					subGroup0 := podgroup_info.NewSubGroupInfo("SubGroup-0", 1)
					subGroup1 := podgroup_info.NewSubGroupInfo("SubGroup-1", 2)

					subGroup0.AssignTask(&pod_info.PodInfo{UID: "pod-0", SubGroupName: "SubGroup-0"})
					subGroup1.AssignTask(&pod_info.PodInfo{UID: "pod-1", SubGroupName: "SubGroup-1"})
					subGroup1.AssignTask(&pod_info.PodInfo{UID: "pod-2", SubGroupName: "SubGroup-1"})

					return &podgroup_info.PodGroupInfo{
						Name:  "podGroup-0",
						Queue: "queue-0",
						SubGroups: map[string]*podgroup_info.SubGroupInfo{
							"SubGroup-0": subGroup0,
							"SubGroup-1": subGroup1,
						},
					}
				}(),
			},
		},
	}

	for name, test := range tests {
		clusterInfo := newClusterInfoTests(t,
			clusterInfoTestParams{
				kubeObjects:         test.kubeObjs,
				kaiSchedulerObjects: test.objs,
				kueueObjects:        test.kueueObjs,
			},
		)
		predefinedQueue := &queue_info.QueueInfo{Name: "queue-0"}
		existingPods := map[common_info.PodID]*pod_info.PodInfo{}
		podGroups, err := clusterInfo.snapshotPodGroups(
			map[common_info.QueueID]*queue_info.QueueInfo{"queue-0": predefinedQueue},
			existingPods)
		if err != nil {
			assert.FailNow(t, fmt.Sprintf("SnapshotNode got error in test %v", name), err)
		}

		assert.Equal(t, len(test.results), len(podGroups))
		for _, expected := range test.results {
			pg, found := podGroups[common_info.PodGroupID(expected.Name)]
			assert.True(t, found, "PodGroup not found", expected.Name)

			assert.Equal(t, expected.Name, pg.Name)
			assert.Equal(t, expected.Queue, pg.Queue)

			assert.Equal(t, len(expected.GetSubGroups()), len(pg.GetSubGroups()))
			for _, expectedSubGroup := range expected.GetSubGroups() {
				for _, subGroup := range pg.GetSubGroups() {
					if expectedSubGroup.GetName() != subGroup.GetName() {
						continue
					}
					assert.Equal(t, expectedSubGroup.GetMinAvailable(), subGroup.GetMinAvailable())
					assert.Equal(t, len(expectedSubGroup.GetPodInfos()), len(subGroup.GetPodInfos()))
					if subGroup.GetName() == podgroup_info.DefaultSubGroup {
						continue
					}
					for _, podInfo := range subGroup.GetPodInfos() {
						assert.Equal(t, subGroup.GetName(), podInfo.SubGroupName)
					}
				}
			}
		}

	}
}

func TestSnapshotQueues(t *testing.T) {
	objs := []runtime.Object{
		&enginev2.Queue{
			ObjectMeta: metav1.ObjectMeta{
				Name: "department0",
			},
			Spec: enginev2.QueueSpec{
				DisplayName: "department-zero",
				Resources: &enginev2.QueueResources{
					GPU: enginev2.QueueResource{
						Quota: 4,
					},
				},
			},
		},
		&enginev2.Queue{
			ObjectMeta: metav1.ObjectMeta{
				Name: "department0-a",
				Labels: map[string]string{
					nodePoolNameLabel: "nodepool-a",
				},
			},
			Spec: enginev2.QueueSpec{
				Resources: &enginev2.QueueResources{
					GPU: enginev2.QueueResource{
						Quota: 2,
					},
				},
			},
		},
		&enginev2.Queue{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "queue0",
				Labels: map[string]string{},
			},
			Spec: enginev2.QueueSpec{
				ParentQueue: "department0",
				Resources: &enginev2.QueueResources{
					GPU: enginev2.QueueResource{
						Quota: 2,
					},
				},
			},
		},
	}
	kubeObjs := []runtime.Object{}
	kueueObjs := []runtime.Object{}

	clusterInfo := newClusterInfoTests(t,
		clusterInfoTestParams{
			kubeObjects:         kubeObjs,
			kaiSchedulerObjects: objs,
			kueueObjects:        kueueObjs,
		},
	)
	snapshot, err := clusterInfo.Snapshot()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(snapshot.Queues))
	assert.Equal(t, common_info.QueueID("queue0"), snapshot.Queues["queue0"].UID)
	assert.Equal(t, common_info.QueueID("department0"), snapshot.Queues["department0"].UID)
	assert.Equal(t, "queue0", snapshot.Queues["queue0"].Name)
	assert.Equal(t, "department-zero", snapshot.Queues["department0"].Name)
	assert.Equal(t, common_info.QueueID(""), snapshot.Queues["department0"].ParentQueue)
	assert.Equal(t, common_info.QueueID("department0"), snapshot.Queues["queue0"].ParentQueue)
	assert.Equal(t, []common_info.QueueID{"queue0"}, snapshot.Queues["department0"].ChildQueues)
	assert.Equal(t, []common_info.QueueID{}, snapshot.Queues["queue0"].ChildQueues)
}

func TestSnapshotFlatHierarchy(t *testing.T) {
	parentQueue0 := &enginev2.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "department0",
			Labels: map[string]string{
				nodePoolNameLabel: "nodepool-a",
			},
		},
		Spec: enginev2.QueueSpec{
			Resources: &enginev2.QueueResources{
				GPU: enginev2.QueueResource{
					Quota:           4,
					OverQuotaWeight: 2,
					Limit:           10,
				},
			},
		},
	}
	parentQueue1 := parentQueue0.DeepCopy()
	parentQueue1.Name = "department1"

	queue0 := &enginev2.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "queue0",
			Labels: map[string]string{
				nodePoolNameLabel: "nodepool-a",
			},
		},
		Spec: enginev2.QueueSpec{
			ParentQueue: parentQueue0.Name,
		},
	}
	queue1 := &enginev2.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "queue1",
			Labels: map[string]string{
				nodePoolNameLabel: "nodepool-a",
			},
		},
		Spec: enginev2.QueueSpec{
			ParentQueue: parentQueue1.Name,
		},
	}
	objects := []runtime.Object{parentQueue0, parentQueue1, queue0, queue1}
	params := &conf.SchedulingNodePoolParams{
		NodePoolLabelKey:   nodePoolNameLabel,
		NodePoolLabelValue: "nodepool-a"}
	clusterInfo := newClusterInfoTestsInner(t,
		[]runtime.Object{},
		objects,
		[]runtime.Object{},
		params,
		false,
		nil, nil, // usage and usageErr
	)

	snapshot, err := clusterInfo.Snapshot()
	assert.Nil(t, err)
	assert.Equal(t, 3, len(snapshot.Queues))

	defaultParentQueueId := common_info.QueueID(defaultQueueName)
	parentQueue, found := snapshot.Queues[defaultParentQueueId]
	assert.True(t, found)
	assert.Equal(t, parentQueue.Name, defaultQueueName)
	assert.Equal(t, parentQueue.UID, defaultParentQueueId)
	assert.Equal(t, parentQueue.Resources, queue_info.QueueQuota{
		GPU: queue_info.ResourceQuota{
			Quota:           -1,
			OverQuotaWeight: 1,
			Limit:           -1,
		},
		CPU: queue_info.ResourceQuota{
			Quota:           -1,
			OverQuotaWeight: 1,
			Limit:           -1,
		},
		Memory: queue_info.ResourceQuota{
			Quota:           -1,
			OverQuotaWeight: 1,
			Limit:           -1,
		},
	})
	snapshotQueue0, found := snapshot.Queues[common_info.QueueID(queue0.Name)]
	assert.True(t, found)
	assert.Equal(t, snapshotQueue0.ParentQueue, defaultParentQueueId)

	snapshotQueue1, found := snapshot.Queues[common_info.QueueID(queue1.Name)]
	assert.True(t, found)
	assert.Equal(t, snapshotQueue1.ParentQueue, defaultParentQueueId)
}

func TestGetPodGroupPriority(t *testing.T) {
	kubeObjects := []runtime.Object{
		&v12.PriorityClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "my-priority",
			},
			Value: 2,
		},
	}
	podGroup := &enginev2alpha2.PodGroup{
		Spec: enginev2alpha2.PodGroupSpec{
			PriorityClassName: "my-priority",
		},
	}

	clusterInfo := newClusterInfoTests(t,
		clusterInfoTestParams{
			kubeObjects:         kubeObjects,
			kaiSchedulerObjects: []runtime.Object{},
			kueueObjects:        []runtime.Object{},
		},
	)

	priority := getPodGroupPriority(podGroup, 1, clusterInfo.dataLister)
	assert.Equal(t, int32(2), priority)
}

func TestSnapshotStorageObjects(t *testing.T) {
	kubeObjects := []runtime.Object{
		&storage.CSIDriver{
			ObjectMeta: metav1.ObjectMeta{Name: "csi-driver"},
			Spec: storage.CSIDriverSpec{
				StorageCapacity: ptr.To(true),
			},
		},
		&storage.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "storage-class",
			},
			Provisioner:       "csi-driver",
			VolumeBindingMode: (*storage.VolumeBindingMode)(ptr.To(string(storage.VolumeBindingWaitForFirstConsumer))),
		},
		&storage.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "non-csi-storage-class",
			},
			Provisioner:       "non-csi-driver",
			VolumeBindingMode: (*storage.VolumeBindingMode)(ptr.To(string(storage.VolumeBindingWaitForFirstConsumer))),
		},
		&storage.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "immediate-binding-storage-class",
			},
			Provisioner:       "csi-driver",
			VolumeBindingMode: (*storage.VolumeBindingMode)(ptr.To(string(storage.VolumeBindingImmediate))),
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      nonOwnedClaimName,
				Namespace: testNamespace,
				UID:       "csi-pvc-uid",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: ptr.To("storage-class"),
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ownedClaimName,
				Namespace: testNamespace,
				UID:       "owned-csi-pvc-uid",
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "v1",
						Kind:       "pod",
						Name:       "owner-pod",
						UID:        "owner-pod-uid",
					},
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: ptr.To("storage-class"),
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "owner-pod",
				Namespace: testNamespace,
				UID:       "owner-pod-uid",
				Annotations: map[string]string{
					commonconstants.PodGroupAnnotationForPod: "podGroup-0",
				},
			},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: ownedClaimName,
							},
						},
					},
				},
			},
		},
	}

	kubeAiSchedOjbs := []runtime.Object{
		&enginev2.Queue{
			ObjectMeta: metav1.ObjectMeta{
				Name: "queue-0",
			},
		},
		&enginev2alpha2.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: "podGroup-0",
				UID:  "ABC",
			},
			Spec: enginev2alpha2.PodGroupSpec{
				Queue: "queue-0",
			},
		},
	}

	kueueObjects := []runtime.Object{
		&kueuev1alpha1.Topology{
			ObjectMeta: metav1.ObjectMeta{
				Name: "topology-0",
			},
		},
	}

	clusterInfo := newClusterInfoTests(t,
		clusterInfoTestParams{
			kubeObjects:         kubeObjects,
			kaiSchedulerObjects: kubeAiSchedOjbs,
			kueueObjects:        kueueObjects,
		},
	)

	snapshot, err := clusterInfo.Snapshot()
	assert.Nil(t, err)

	expectedStorageClasses := map[common_info.StorageClassID]*storageclass_info.StorageClassInfo{
		"storage-class": {
			ID:          "storage-class",
			Provisioner: "csi-driver",
		},
	}

	assert.Equal(t, expectedStorageClasses, snapshot.StorageClasses)
	expectedStorageClaims := map[storageclaim_info.Key]*storageclaim_info.StorageClaimInfo{
		nonOwnedClaimKey: {
			Key:               nonOwnedClaimKey,
			Name:              nonOwnedClaimName,
			Namespace:         testNamespace,
			Size:              resource.NewQuantity(0, resource.BinarySI),
			Phase:             corev1.ClaimBound,
			StorageClass:      "storage-class",
			PodOwnerReference: nil,
			DeletedOwner:      true,
		},
		ownedClaimKey: {
			Key:          ownedClaimKey,
			Name:         ownedClaimName,
			Namespace:    testNamespace,
			Size:         resource.NewQuantity(0, resource.BinarySI),
			Phase:        corev1.ClaimBound,
			StorageClass: "storage-class",
			PodOwnerReference: &storageclaim_info.PodOwnerReference{
				PodID:        "owner-pod-uid",
				PodName:      "owner-pod",
				PodNamespace: testNamespace,
			},
			DeletedOwner: false,
		},
	}

	assert.Equal(t, expectedStorageClaims[ownedClaimKey], snapshot.StorageClaims[ownedClaimKey])
}

func TestGetPodGroupPriorityNotExistingPriority(t *testing.T) {
	podGroup := &enginev2alpha2.PodGroup{
		Spec: enginev2alpha2.PodGroupSpec{
			PriorityClassName: "my-priority",
		},
	}

	clusterInfo := newClusterInfoTests(t,
		clusterInfoTestParams{
			kubeObjects:         []runtime.Object{},
			kaiSchedulerObjects: []runtime.Object{},
			kueueObjects:        []runtime.Object{},
		},
	)

	priority := getPodGroupPriority(podGroup, 123, clusterInfo.dataLister)
	assert.Equal(t, int32(123), priority)
}

func TestGetDefaultPriority(t *testing.T) {
	kubeObjects := []runtime.Object{
		&v12.PriorityClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "my-priority",
			},
			Value:         2,
			GlobalDefault: true,
		},
	}

	clusterInfo := newClusterInfoTests(t,
		clusterInfoTestParams{
			kubeObjects:         kubeObjects,
			kaiSchedulerObjects: []runtime.Object{},
			kueueObjects:        []runtime.Object{},
		},
	)

	priority, err := getDefaultPriority(clusterInfo.dataLister)
	assert.Equal(t, nil, err)
	assert.Equal(t, int32(2), priority)
}

func TestGetDefaultPriorityNotExists(t *testing.T) {
	kubeObjects := []runtime.Object{
		&v12.PriorityClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "my-priority",
			},
			Value: 2,
		},
	}
	clusterInfo := newClusterInfoTests(t,
		clusterInfoTestParams{
			kubeObjects:         kubeObjects,
			kaiSchedulerObjects: []runtime.Object{},
			kueueObjects:        []runtime.Object{},
		},
	)
	priority, err := getDefaultPriority(clusterInfo.dataLister)
	assert.Equal(t, nil, err)
	assert.Equal(t, int32(50), priority)
}

func TestGetDefaultPriorityWithError(t *testing.T) {
	clusterInfo := newClusterInfoTests(t,
		clusterInfoTestParams{
			kubeObjects:         []runtime.Object{},
			kaiSchedulerObjects: []runtime.Object{},
			kueueObjects:        []runtime.Object{},
		},
	)
	priority, err := getDefaultPriority(clusterInfo.dataLister)
	assert.Equal(t, nil, err)
	assert.Equal(t, int32(50), priority)
}

func TestPodGroupWithIndex(t *testing.T) {
	podGroup := &enginev2alpha2.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			UID: "ABC",
		},
	}
	podGroupInfo := &podgroup_info.PodGroupInfo{
		PodGroupUID: "ABC",
	}

	clusterInfo := newClusterInfoTests(t,
		clusterInfoTestParams{
			kubeObjects:         []runtime.Object{},
			kaiSchedulerObjects: []runtime.Object{},
			kueueObjects:        []runtime.Object{},
		},
	)
	clusterInfo.setPodGroupWithIndex(podGroup, podGroupInfo)
}

func TestPodGroupWithIndexNonMatching(t *testing.T) {
	podGroup := &enginev2alpha2.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			UID: "ABC",
		},
	}
	podGroupInfo := &podgroup_info.PodGroupInfo{
		PodGroupUID: "MyTest",
	}

	clusterInfo := newClusterInfoTests(t,
		clusterInfoTestParams{
			kubeObjects:         []runtime.Object{},
			kaiSchedulerObjects: []runtime.Object{},
			kueueObjects:        []runtime.Object{},
		},
	)
	clusterInfo.setPodGroupWithIndex(podGroup, podGroupInfo)
}

func TestIsPodGroupUpForScheduler(t *testing.T) {
	testCases := []struct {
		testName                string
		schedulingBackoff       *int32
		nodePoolName            string
		lastSchedulingCondition *enginev2alpha2.SchedulingCondition
		expectedResult          bool
	}{
		{
			testName:          "Infinite schedulingBackoff",
			schedulingBackoff: ptr.To(int32(utils.NoSchedulingBackoff)),
			nodePoolName:      "nodepoola",
			lastSchedulingCondition: &enginev2alpha2.SchedulingCondition{
				NodePool: "nodepoola",
			},
			expectedResult: true,
		},
		{
			testName:          "Nil schedulingBackoff",
			schedulingBackoff: nil,
			nodePoolName:      "nodepoolb",
			lastSchedulingCondition: &enginev2alpha2.SchedulingCondition{
				NodePool: "nodepoolb",
			},
			expectedResult: true,
		},
		{
			testName:                "No last scheduling condition",
			schedulingBackoff:       ptr.To(int32(utils.SingleSchedulingBackoff)),
			nodePoolName:            "nodepoolb",
			lastSchedulingCondition: nil,
			expectedResult:          true,
		},
		{
			testName:                "No last scheduling condition - default node pool",
			schedulingBackoff:       ptr.To(int32(utils.SingleSchedulingBackoff)),
			nodePoolName:            defaultNodePoolName,
			lastSchedulingCondition: nil,
			expectedResult:          true,
		},
		{
			testName:          "unassigned by condition from different node pool",
			schedulingBackoff: ptr.To(int32(utils.SingleSchedulingBackoff)),
			nodePoolName:      "nodepoola",
			lastSchedulingCondition: &enginev2alpha2.SchedulingCondition{
				NodePool: "different-nodepool",
			},
			expectedResult: true,
		},
		{
			testName:          "unassigned by condition",
			schedulingBackoff: ptr.To(int32(utils.SingleSchedulingBackoff)),
			nodePoolName:      "nodepoolc",
			lastSchedulingCondition: &enginev2alpha2.SchedulingCondition{
				NodePool: "nodepoolc",
			},
			expectedResult: false,
		},
		{
			testName:          "unassigned by condition - default node pool",
			schedulingBackoff: ptr.To(int32(utils.SingleSchedulingBackoff)),
			nodePoolName:      defaultNodePoolName,
			lastSchedulingCondition: &enginev2alpha2.SchedulingCondition{
				NodePool: "different-nodepool",
			},
			expectedResult: true,
		},
		{
			testName:          "unassigned by condition - default node pool 2",
			schedulingBackoff: ptr.To(int32(utils.SingleSchedulingBackoff)),
			nodePoolName:      "nodepoolc",
			lastSchedulingCondition: &enginev2alpha2.SchedulingCondition{
				NodePool: defaultNodePoolName,
			},
			expectedResult: true,
		},
		{
			testName:          "unassigned by condition - default node pool",
			schedulingBackoff: ptr.To(int32(utils.SingleSchedulingBackoff)),
			nodePoolName:      defaultNodePoolName,
			lastSchedulingCondition: &enginev2alpha2.SchedulingCondition{
				NodePool: defaultNodePoolName,
			},
			expectedResult: false,
		},
	}

	for _, testData := range testCases {
		pg := createFakePodGroup("test-pg", testData.schedulingBackoff, testData.nodePoolName,
			testData.lastSchedulingCondition)
		ci := newClusterInfoTests(t,
			clusterInfoTestParams{
				kubeObjects:         []runtime.Object{},
				kaiSchedulerObjects: []runtime.Object{},
				kueueObjects:        []runtime.Object{},
			},
		)
		result := ci.isPodGroupUpForScheduler(pg)
		assert.Equal(t, result, testData.expectedResult,
			"Test: <%s>, expected pod group to be up for scheduler <%t>", testData.testName,
			testData.expectedResult)
	}
}

func TestNotSchedulingPodWithTerminatingPVC(t *testing.T) {
	kubeObjects := []runtime.Object{
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-1",
				Namespace: "test",
				UID:       "pod-1",
				Annotations: map[string]string{
					commonconstants.PodGroupAnnotationForPod: "podGroup-0",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "container-1",
					},
				},
				Volumes: []corev1.Volume{
					{
						Name: "pv-1",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "pvc-1",
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodPending,
			},
		},
		&storage.CSIDriver{
			ObjectMeta: metav1.ObjectMeta{Name: "csi-driver"},
			Spec: storage.CSIDriverSpec{
				StorageCapacity: ptr.To(true),
			},
		},
		&storage.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "storage-class",
			},
			Provisioner:       "csi-driver",
			VolumeBindingMode: (*storage.VolumeBindingMode)(ptr.To(string(storage.VolumeBindingWaitForFirstConsumer))),
		},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-1",
				Labels: map[string]string{
					"kubernetes.io/hostname": "node-1",
				},
			},
		},
		&storage.CSIStorageCapacity{
			ObjectMeta: metav1.ObjectMeta{
				Name: "capacity-node-1",
			},
			NodeTopology: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"kubernetes.io/hostname": "node-1",
				},
			},
			StorageClassName: "storage-class",
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-1",
			Namespace: "test",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       "pod-2",
					UID:        "pod-2",
				},
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:       "pv-1",
			StorageClassName: ptr.To("storage-class"),
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimPending,
		},
	}

	kubeAiSchedOjbs := []runtime.Object{
		&enginev2.Queue{
			ObjectMeta: metav1.ObjectMeta{
				Name: "queue-0",
			},
		},
		&enginev2alpha2.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: "podGroup-0",
				UID:  "ABC",
			},
			Spec: enginev2alpha2.PodGroupSpec{
				Queue: "queue-0",
			},
		},
	}

	clusterInfo := newClusterInfoTests(t,
		clusterInfoTestParams{
			kubeObjects:         append(kubeObjects, pvc),
			kaiSchedulerObjects: kubeAiSchedOjbs,
			kueueObjects:        []runtime.Object{},
		},
	)
	snapshot, err := clusterInfo.Snapshot()
	assert.Equal(t, nil, err)
	node := snapshot.Nodes["node-1"]
	task := snapshot.PodGroupInfos["podGroup-0"].GetAllPodsMap()["pod-1"]
	assert.Equal(t, node.IsTaskAllocatable(task), false)

	pvc.OwnerReferences = nil

	clusterInfo = newClusterInfoTests(t,
		clusterInfoTestParams{
			kubeObjects:         append(kubeObjects, pvc),
			kaiSchedulerObjects: kubeAiSchedOjbs,
			kueueObjects:        []runtime.Object{},
		},
	)
	snapshot, err = clusterInfo.Snapshot()
	assert.Equal(t, nil, err)
	node = snapshot.Nodes["node-1"]
	task = snapshot.PodGroupInfos["podGroup-0"].GetAllPodsMap()["pod-1"]
	assert.Equal(t, node.IsTaskAllocatable(task), true)

}

func createFakePodGroup(name string, schedulingBackoff *int32, nodePoolName string,
	lastSchedulingCondition *enginev2alpha2.SchedulingCondition) *enginev2alpha2.PodGroup {
	result := &enginev2alpha2.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{},
			Name:   name,
		},
		Spec: enginev2alpha2.PodGroupSpec{
			SchedulingBackoff: schedulingBackoff,
		},
		Status: enginev2alpha2.PodGroupStatus{
			SchedulingConditions: []enginev2alpha2.SchedulingCondition{},
		},
	}
	if nodePoolName != defaultNodePoolName && nodePoolName != "" {
		result.Labels[nodePoolNameLabel] = nodePoolName
	}
	if lastSchedulingCondition != nil {
		result.Status.SchedulingConditions = append(result.Status.SchedulingConditions,
			*lastSchedulingCondition)
	}
	return result
}

func TestSnapshotWithListerErrors(t *testing.T) {
	tests := map[string]struct {
		install func(*data_lister.MockDataLister)
	}{
		"listNodes": {
			func(mdl *data_lister.MockDataLister) {
				mdl.EXPECT().ListNodes().Return(nil, fmt.Errorf(successErrorMsg))
				mdl.EXPECT().ListPods().Return(nil, nil)
			},
		},
		"listPods": {
			func(mdl *data_lister.MockDataLister) {
				mdl.EXPECT().ListPods().Return(nil, fmt.Errorf(successErrorMsg))
			},
		},
		"twiceSamePod": {
			func(mdl *data_lister.MockDataLister) {
				mdl.EXPECT().ListNodes().Return([]*corev1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node-0",
						},
					},
				}, nil)
				mdl.EXPECT().ListPods().Return([]*corev1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "my-pod",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "my-pod",
						},
					},
				}, nil)
				mdl.EXPECT().ListBindRequests().Return([]*schedulingv1alpha2.BindRequest{}, nil)
				mdl.EXPECT().ListQueues().Return(nil, fmt.Errorf(successErrorMsg))
			},
		},
		"listQueues": {
			func(mdl *data_lister.MockDataLister) {
				mdl.EXPECT().ListNodes().Return([]*corev1.Node{}, nil)
				mdl.EXPECT().ListPods().Return([]*corev1.Pod{}, nil)
				mdl.EXPECT().ListBindRequests().Return([]*schedulingv1alpha2.BindRequest{}, nil)
				mdl.EXPECT().ListQueues().Return(nil, fmt.Errorf(successErrorMsg))
			},
		},
		"listPodGroups": {
			func(mdl *data_lister.MockDataLister) {
				mdl.EXPECT().ListNodes().Return([]*corev1.Node{}, nil)
				mdl.EXPECT().ListPods().Return([]*corev1.Pod{}, nil)
				mdl.EXPECT().ListBindRequests().Return([]*schedulingv1alpha2.BindRequest{}, nil)
				mdl.EXPECT().ListQueues().Return([]*enginev2.Queue{}, nil)
				mdl.EXPECT().ListResourceUsage().Return(nil, nil)
				mdl.EXPECT().ListPriorityClasses().Return([]*v12.PriorityClass{}, nil)
				mdl.EXPECT().ListPodGroups().Return(nil, fmt.Errorf(successErrorMsg))
			},
		},
		"defaultPriorityClass": {
			func(mdl *data_lister.MockDataLister) {
				mdl.EXPECT().ListNodes().Return([]*corev1.Node{}, nil)
				mdl.EXPECT().ListPods().Return([]*corev1.Pod{}, nil)
				mdl.EXPECT().ListBindRequests().Return([]*schedulingv1alpha2.BindRequest{}, nil)
				mdl.EXPECT().ListQueues().Return([]*enginev2.Queue{}, nil)
				mdl.EXPECT().ListResourceUsage().Return(nil, nil)
				mdl.EXPECT().ListPriorityClasses().Return(nil, fmt.Errorf(successErrorMsg))
			},
		},
		"getPriorityClassByNameAndPodByPodGroup": {
			func(mdl *data_lister.MockDataLister) {
				mdl.EXPECT().ListNodes().Return([]*corev1.Node{}, nil)
				mdl.EXPECT().ListPods().Return([]*corev1.Pod{}, nil)
				mdl.EXPECT().ListBindRequests().Return([]*schedulingv1alpha2.BindRequest{}, nil)
				mdl.EXPECT().ListQueues().Return([]*enginev2.Queue{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "queue-0",
						},
						Spec: enginev2.QueueSpec{
							ParentQueue: "default",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "default",
						},
						Spec: enginev2.QueueSpec{
							Resources: &enginev2.QueueResources{},
						},
					},
				}, nil).AnyTimes()
				mdl.EXPECT().ListResourceUsage().Return(nil, nil)
				mdl.EXPECT().ListPriorityClasses().Return([]*v12.PriorityClass{}, nil)
				mdl.EXPECT().ListPodGroups().Return([]*enginev2alpha2.PodGroup{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "my-pg",
						},
						Spec: enginev2alpha2.PodGroupSpec{Queue: "queue-0"},
					},
				}, nil)
				mdl.EXPECT().GetPriorityClassByName(gomock.Any()).Return(nil, fmt.Errorf(successErrorMsg))
				mdl.EXPECT().ListPodByIndex(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf(successErrorMsg))
			},
		},
		"ListBindRequests": {
			func(mdl *data_lister.MockDataLister) {
				mdl.EXPECT().ListPods().Return([]*corev1.Pod{}, nil)
				mdl.EXPECT().ListNodes().Return([]*corev1.Node{}, nil)
				mdl.EXPECT().ListBindRequests().Return(nil, fmt.Errorf(successErrorMsg))
			},
		},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	for name, test := range tests {
		t.Logf("Running test: %s", name)
		dl := data_lister.NewMockDataLister(ctrl)
		clusterInfo := newClusterInfoTests(t,
			clusterInfoTestParams{
				kubeObjects:         []runtime.Object{},
				kaiSchedulerObjects: []runtime.Object{},
				kueueObjects:        []runtime.Object{},
			},
		)
		test.install(dl)
		clusterInfo.dataLister = dl
		_, err := clusterInfo.Snapshot()
		assert.NotNil(t, err)
	}
}

func TestNewClusterInfoErrorPartitionSelector(t *testing.T) {
	kubeFakeClient, kubeAiFakeClient, kueueFakeClient := newFakeClients([]runtime.Object{}, []runtime.Object{}, []runtime.Object{})
	informerFactory := informers.NewSharedInformerFactory(kubeFakeClient, 0)
	kubeAiSchedulerInformerFactory := kubeAiSchedulerInfo.NewSharedInformerFactory(kubeAiFakeClient, 0)
	kueueInformerFactory := kueueinformer.NewSharedInformerFactory(kueueFakeClient, 0)

	controller := gomock.NewController(t)
	clusterPodAffinityInfo := pod_affinity.NewMockClusterPodAffinityInfo(controller)
	clusterPodAffinityInfo.EXPECT().UpdateNodeAffinity(gomock.Any()).AnyTimes()
	clusterPodAffinityInfo.EXPECT().AddNode(gomock.Any(), gomock.Any()).AnyTimes()

	params := &conf.SchedulingNodePoolParams{
		NodePoolLabelKey:   "@!A",
		NodePoolLabelValue: "!@#",
	}
	_, err := New(informerFactory, kubeAiSchedulerInformerFactory, kueueInformerFactory, nil, params, false, clusterPodAffinityInfo, false, true, nil)

	assert.NotNil(t, err)
}

func fakeIndexFunc(obj interface{}) ([]string, error) {
	return nil, nil
}

func TestNewClusterInfoAddIndexerFails(t *testing.T) {
	kubeFakeClient, kubeAiSchedulerFakeClient, kueueFakeClient := newFakeClients([]runtime.Object{}, []runtime.Object{}, []runtime.Object{})
	informerFactory := informers.NewSharedInformerFactory(kubeFakeClient, 0)
	kubeAiSchedulerInformerFactory := kubeAiSchedulerInfo.NewSharedInformerFactory(kubeAiSchedulerFakeClient, 0)
	kueueInformerFactory := kueueinformer.NewSharedInformerFactory(kueueFakeClient, 0)
	podInformer := informerFactory.Core().V1().Pods()
	go podInformer.Informer().Run(nil)
	for !podInformer.Informer().HasSynced() {
		time.Sleep(500 * time.Millisecond)
	}

	err := podInformer.Informer().AddIndexers(
		cache.Indexers{
			"podByPodGroupIndexer": fakeIndexFunc,
		})
	assert.Nil(t, err, "Failed to add fake indexer")

	controller := gomock.NewController(t)
	clusterPodAffinityInfo := pod_affinity.NewMockClusterPodAffinityInfo(controller)
	clusterPodAffinityInfo.EXPECT().UpdateNodeAffinity(gomock.Any()).AnyTimes()
	clusterPodAffinityInfo.EXPECT().AddNode(gomock.Any(), gomock.Any()).AnyTimes()

	_, err = New(informerFactory, kubeAiSchedulerInformerFactory, kueueInformerFactory, nil, nil, false,
		clusterPodAffinityInfo, false, true, nil)
	assert.NotNil(t, err, "Expected error for conflicting indexers")
}

type clusterInfoTestParams struct {
	kubeObjects         []runtime.Object
	kaiSchedulerObjects []runtime.Object
	kueueObjects        []runtime.Object
	clusterUsage        *queue_info.ClusterUsage
	clusterUsageErr     error
}

func newClusterInfoTests(t *testing.T, testParams clusterInfoTestParams) *ClusterInfo {
	nodePoolParams := &conf.SchedulingNodePoolParams{
		NodePoolLabelKey:   nodePoolNameLabel,
		NodePoolLabelValue: "",
	}
	return newClusterInfoTestsInner(
		t, testParams.kubeObjects, testParams.kaiSchedulerObjects,
		testParams.kueueObjects, nodePoolParams, true,
		testParams.clusterUsage, testParams.clusterUsageErr)
}

func newClusterInfoTestsInner(t *testing.T, kubeObjects, kaiSchedulerObjects, kueueObjects []runtime.Object,
	nodePoolParams *conf.SchedulingNodePoolParams, fullHierarchyFairness bool,
	clusterUsage *queue_info.ClusterUsage, clusterUsageErr error) *ClusterInfo {
	kubeFakeClient, kubeAiSchedulerFakeClient, kueueFakeClient := newFakeClients(kubeObjects, kaiSchedulerObjects, kueueObjects)
	informerFactory := informers.NewSharedInformerFactory(kubeFakeClient, 0)
	kubeAiSchedulerInformerFactory := kubeAiSchedulerInfo.NewSharedInformerFactory(kubeAiSchedulerFakeClient, 0)
	kueueInformerFactory := kueueinformer.NewSharedInformerFactory(kueueFakeClient, 0)

	controller := gomock.NewController(t)
	clusterPodAffinityInfo := pod_affinity.NewMockClusterPodAffinityInfo(controller)
	clusterPodAffinityInfo.EXPECT().UpdateNodeAffinity(gomock.Any()).AnyTimes()
	clusterPodAffinityInfo.EXPECT().AddNode(gomock.Any(), gomock.Any()).AnyTimes()

	fakeUsageClient := fakeusage.FakeClient{}
	fakeUsageClient.SetResourceUsage(clusterUsage, clusterUsageErr)
	usageLister := usagedb.NewUsageLister(&fakeUsageClient, ptr.To(10*time.Microsecond), ptr.To(10*time.Second), ptr.To(10*time.Second))

	clusterInfo, _ := New(informerFactory, kubeAiSchedulerInformerFactory, kueueInformerFactory, usageLister, nodePoolParams, false,
		clusterPodAffinityInfo, true, fullHierarchyFairness, nil)

	stopCh := context.Background().Done()
	informerFactory.Start(stopCh)
	informerFactory.WaitForCacheSync(stopCh)
	kubeAiSchedulerInformerFactory.Start(stopCh)
	kubeAiSchedulerInformerFactory.WaitForCacheSync(stopCh)
	kueueInformerFactory.Start(stopCh)
	kueueInformerFactory.WaitForCacheSync(stopCh)
	usageLister.Start(stopCh)
	usageLister.WaitForCacheSync(stopCh)

	return clusterInfo
}

func newFakeClients(kubernetesObjects, kaiSchedulerObjects, kueueObjects []runtime.Object) (kubernetes.Interface, kubeAiSchedulerClient.Interface, kueueclient.Interface) {
	return fake.NewSimpleClientset(kubernetesObjects...), kubeAiSchedulerClientFake.NewSimpleClientset(kaiSchedulerObjects...), kueuefake.NewSimpleClientset(kueueObjects...)
}

func TestSnapshotPodsInPartition(t *testing.T) {
	clusterObjects := []runtime.Object{
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node1",
				Labels: map[string]string{
					nodePoolNameLabel: "foo",
				},
			},
		},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node2",
				Labels: map[string]string{
					nodePoolNameLabel: "bar",
				},
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod1",
				Labels: map[string]string{
					nodePoolNameLabel: "foo",
				},
			},
			Spec: corev1.PodSpec{
				NodeName: "node1",
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod2",
				Labels: map[string]string{
					nodePoolNameLabel: "bar",
				},
			},
			Spec: corev1.PodSpec{
				NodeName: "node2",
			},
		},
	}

	clusterInfo := newClusterInfoTestsInner(
		t, clusterObjects,
		[]runtime.Object{},
		[]runtime.Object{},
		&conf.SchedulingNodePoolParams{
			NodePoolLabelKey:   nodePoolNameLabel,
			NodePoolLabelValue: "foo",
		},
		true,
		nil, nil, // usage and usageErr
	)
	snapshot, err := clusterInfo.Snapshot()
	assert.Nil(t, err)
	assert.Len(t, snapshot.Pods, 1)
	assert.Equal(t, "pod1", snapshot.Pods[0].Name)
}

func newCompletedPod(pod *corev1.Pod) *corev1.Pod {
	newPod := pod.DeepCopy()
	newPod.Status.Phase = corev1.PodSucceeded
	newPod.Status.Conditions = []corev1.PodCondition{
		{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		},
	}
	return newPod
}

func newPodOnNode(pod *corev1.Pod, nodeName string) *corev1.Pod {
	newPod := pod.DeepCopy()
	newPod.Spec.NodeName = nodeName
	newPod.Name = fmt.Sprintf("%s-%s", pod.Name, nodeName)
	return newPod
}
