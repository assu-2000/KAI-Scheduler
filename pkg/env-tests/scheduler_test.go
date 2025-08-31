// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package env_tests

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/xyproto/randomstring"
	corev1 "k8s.io/api/core/v1"
	resourcev1beta1 "k8s.io/api/resource/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kaiv1alpha2 "github.com/NVIDIA/KAI-scheduler/pkg/apis/scheduling/v1alpha2"
	schedulingv2 "github.com/NVIDIA/KAI-scheduler/pkg/apis/scheduling/v2"
	schedulingv2alpha2 "github.com/NVIDIA/KAI-scheduler/pkg/apis/scheduling/v2alpha2"
	"github.com/NVIDIA/KAI-scheduler/pkg/common/constants"
	"github.com/NVIDIA/KAI-scheduler/pkg/env-tests/dynamicresource"
	"github.com/NVIDIA/KAI-scheduler/pkg/env-tests/scheduler"
	kueuev1alpha1 "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
)

var _ = Describe("Scheduler", Ordered, func() {
	// Define a timeout for eventually assertions
	const interval = time.Millisecond * 10
	const defaultTimeout = interval * 200

	var (
		testNamespace  *corev1.Namespace
		testDepartment *schedulingv2.Queue
		testQueue      *schedulingv2.Queue
		testNode       *corev1.Node
		stopCh         chan struct{}
	)

	BeforeEach(func(ctx context.Context) {
		// Create a test namespace
		testNamespace = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-" + randomstring.HumanFriendlyEnglishString(10),
			},
		}
		Expect(ctrlClient.Create(ctx, testNamespace)).To(Succeed())

		testDepartment = CreateQueueObject("test-department", "")
		Expect(ctrlClient.Create(ctx, testDepartment)).To(Succeed(), "Failed to create test department")

		testQueue = CreateQueueObject("test-queue", testDepartment.Name)
		Expect(ctrlClient.Create(ctx, testQueue)).To(Succeed(), "Failed to create test queue")

		testNode = CreateNodeObject(ctx, ctrlClient, DefaultNodeConfig("test-node"))
		Expect(ctrlClient.Create(ctx, testNode)).To(Succeed(), "Failed to create test node")

		stopCh = make(chan struct{})
		err := scheduler.RunScheduler(cfg, stopCh)
		Expect(err).NotTo(HaveOccurred(), "Failed to run scheduler")
	})

	AfterEach(func(ctx context.Context) {
		Expect(ctrlClient.Delete(ctx, testDepartment)).To(Succeed(), "Failed to delete test department")
		Expect(ctrlClient.Delete(ctx, testQueue)).To(Succeed(), "Failed to delete test queue")
		Expect(ctrlClient.Delete(ctx, testNode)).To(Succeed(), "Failed to delete test node")

		err := WaitForObjectDeletion(ctx, ctrlClient, testDepartment, defaultTimeout, interval)
		Expect(err).NotTo(HaveOccurred(), "Failed to wait for test department to be deleted")

		err = WaitForObjectDeletion(ctx, ctrlClient, testQueue, defaultTimeout, interval)
		Expect(err).NotTo(HaveOccurred(), "Failed to wait for test queue to be deleted")

		err = WaitForObjectDeletion(ctx, ctrlClient, testNode, defaultTimeout, interval)
		Expect(err).NotTo(HaveOccurred(), "Failed to wait for test node to be deleted")

		close(stopCh)
	})

	Context("simple pods", func() {
		AfterEach(func(ctx context.Context) {
			err := DeleteAllInNamespace(ctx, ctrlClient, testNamespace.Name,
				&corev1.Pod{},
				&schedulingv2alpha2.PodGroup{},
				&resourcev1beta1.ResourceClaim{},
				&kaiv1alpha2.BindRequest{},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete test resources")

			err = WaitForNoObjectsInNamespace(ctx, ctrlClient, testNamespace.Name, defaultTimeout, interval,
				&corev1.PodList{},
				&schedulingv2alpha2.PodGroupList{},
				&resourcev1beta1.ResourceClaimList{},
				&kaiv1alpha2.BindRequestList{},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to wait for test resources to be deleted")
		})

		It("Should create a bind request for the pod", func(ctx context.Context) {
			// Create your pod as before
			testPod := CreatePodObject(testNamespace.Name, "test-pod", corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					constants.GpuResource: resource.MustParse("1"),
				},
			})
			Expect(ctrlClient.Create(ctx, testPod)).To(Succeed(), "Failed to create test pod")

			podGroupConfig := podGroupConfig{
				queueName:          testQueue.Name,
				podgroupName:       "test-podgroup",
				minMember:          1,
				topologyConstraint: nil,
			}
			err := GroupPods(ctx, ctrlClient, podGroupConfig, []*corev1.Pod{testPod})
			Expect(err).NotTo(HaveOccurred(), "Failed to group pods")

			// Wait for bind request to be created instead of checking pod binding
			err = WaitForPodScheduled(ctx, ctrlClient, testPod.Name, testNamespace.Name, defaultTimeout, interval)
			Expect(err).NotTo(HaveOccurred(), "Failed to wait for test pod to be scheduled")

			// list bind requests
			bindRequests := &kaiv1alpha2.BindRequestList{}
			Expect(ctrlClient.List(ctx, bindRequests, client.InNamespace(testNamespace.Name))).
				To(Succeed(), "Failed to list bind requests")

			Expect(len(bindRequests.Items)).To(Equal(1), "Expected 1 bind request", PrettyPrintBindRequestList(bindRequests))
		})

		It("Should respect scheduling gates - simple pod", func(ctx context.Context) {
			// Create your pod as before
			testPod := CreatePodObject(testNamespace.Name, "test-pod", corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					constants.GpuResource: resource.MustParse("1"),
				},
			})
			testPod.Spec.SchedulingGates = []corev1.PodSchedulingGate{
				{
					Name: "gated",
				},
			}
			Expect(ctrlClient.Create(ctx, testPod)).To(Succeed(), "Failed to create test pod")

			podGroupConfig := podGroupConfig{
				queueName:          testQueue.Name,
				podgroupName:       "test-podgroup",
				minMember:          1,
				topologyConstraint: nil,
			}
			err := GroupPods(ctx, ctrlClient, podGroupConfig, []*corev1.Pod{testPod})
			Expect(err).NotTo(HaveOccurred(), "Failed to group pods")
			err = wait.PollUntilContextTimeout(ctx, interval, defaultTimeout, true, func(ctx context.Context) (bool, error) {
				var events corev1.EventList
				Expect(ctrlClient.List(ctx, &events, client.InNamespace(testNamespace.Name))).
					To(Succeed(), "Failed to list events")

				for _, event := range events.Items {
					if event.InvolvedObject.Kind != "PodGroup" || event.Reason != "NotReady" {
						continue
					}

					return true, nil
				}

				return false, nil
			})
			Expect(err).NotTo(HaveOccurred(), "Failed to wait for unready podgroup event")

			ctrlClient.Get(ctx, client.ObjectKey{Name: testPod.Name, Namespace: testNamespace.Name}, testPod)
			ungatedPod := testPod.DeepCopy()
			ungatedPod.Spec.SchedulingGates = nil
			patch := client.MergeFrom(testPod)
			Expect(ctrlClient.Patch(ctx, ungatedPod, patch)).To(Succeed(), "Failed to remove scheduling gates from test pod")

			err = WaitForPodScheduled(ctx, ctrlClient, testPod.Name, testNamespace.Name, defaultTimeout, interval)
			Expect(err).NotTo(HaveOccurred(), "Failed to wait for test pod to be scheduled")

			// list bind requests
			bindRequests := &kaiv1alpha2.BindRequestList{}
			Expect(ctrlClient.List(ctx, bindRequests, client.InNamespace(testNamespace.Name))).
				To(Succeed(), "Failed to list bind requests")

			Expect(len(bindRequests.Items)).To(Equal(1), "Expected 1 bind request", PrettyPrintBindRequestList(bindRequests))
		})

		It("Should respect scheduling gates - podgroup", func(ctx context.Context) {
			testPod1 := CreatePodObject(testNamespace.Name, "test-pod-1", corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					constants.GpuResource: resource.MustParse("1"),
				},
			})
			testPod1.Spec.SchedulingGates = []corev1.PodSchedulingGate{
				{
					Name: "gated",
				},
			}
			Expect(ctrlClient.Create(ctx, testPod1)).To(Succeed(), "Failed to create test pod")

			testPod2 := CreatePodObject(testNamespace.Name, "test-pod-2", corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					constants.GpuResource: resource.MustParse("1"),
				},
			})
			testPod2.Spec.SchedulingGates = []corev1.PodSchedulingGate{
				{
					Name: "gated",
				},
			}
			Expect(ctrlClient.Create(ctx, testPod2)).To(Succeed(), "Failed to create test pod")

			podGroupConfig := podGroupConfig{
				queueName:          testQueue.Name,
				podgroupName:       "test-podgroup",
				minMember:          1,
				topologyConstraint: nil,
			}
			err := GroupPods(ctx, ctrlClient, podGroupConfig, []*corev1.Pod{testPod1, testPod2})
			Expect(err).NotTo(HaveOccurred(), "Failed to group pods")

			err = wait.PollUntilContextTimeout(ctx, interval, defaultTimeout, true, func(ctx context.Context) (bool, error) {
				var events corev1.EventList
				Expect(ctrlClient.List(ctx, &events, client.InNamespace(testNamespace.Name))).
					To(Succeed(), "Failed to list events")

				for _, event := range events.Items {
					if event.InvolvedObject.Kind != "PodGroup" || event.Reason != "NotReady" {
						continue
					}

					return true, nil
				}

				return false, nil
			})
			Expect(err).NotTo(HaveOccurred(), "Failed to wait for unready podgroup event")

			ctrlClient.Get(ctx, client.ObjectKey{Name: testPod1.Name, Namespace: testNamespace.Name}, testPod1)
			ungatedPod := testPod1.DeepCopy()
			ungatedPod.Spec.SchedulingGates = nil
			patch := client.MergeFrom(testPod1)
			Expect(ctrlClient.Patch(ctx, ungatedPod, patch)).To(Succeed(), "Failed to remove scheduling gates from test pod")

			err = WaitForPodScheduled(ctx, ctrlClient, testPod1.Name, testNamespace.Name, defaultTimeout, interval)
			Expect(err).NotTo(HaveOccurred(), "Failed to wait for test pod to be scheduled")

			ctrlClient.Get(ctx, client.ObjectKey{Name: testPod2.Name, Namespace: testNamespace.Name}, testPod2)
			// expect testPod2 to not be scheduled
			Expect(testPod2.Status.Phase).To(Equal(corev1.PodPending), "Expected test pod 2 to not be scheduled")

			// ungate testPod2
			ungatedPod2 := testPod2.DeepCopy()
			ungatedPod2.Spec.SchedulingGates = nil
			patch2 := client.MergeFrom(testPod2)
			Expect(ctrlClient.Patch(ctx, ungatedPod2, patch2)).To(Succeed(), "Failed to remove scheduling gates from test pod")

			err = WaitForPodScheduled(ctx, ctrlClient, testPod2.Name, testNamespace.Name, defaultTimeout, interval)
			Expect(err).NotTo(HaveOccurred(), "Failed to wait for test pod to be scheduled")
		})
	})

	Context("dynamic resource", Ordered, func() {
		const (
			deviceClassName = "gpu.nvidia.com"
			driverName      = "nvidia"
			deviceNum       = 8
		)

		BeforeAll(func(ctx context.Context) {
			deviceClass := dynamicresource.CreateDeviceClass(deviceClassName)
			Expect(ctrlClient.Create(ctx, deviceClass)).To(Succeed(), "Failed to create test device class")

			nodeResourceSlice := dynamicresource.CreateNodeResourceSlice(
				"test-node-resource-slice", driverName, testNode.Name, deviceNum)
			Expect(ctrlClient.Create(ctx, nodeResourceSlice)).
				To(Succeed(), "Failed to create test node resource slice")
		})

		AfterEach(func(ctx context.Context) {
			err := DeleteAllInNamespace(ctx, ctrlClient, testNamespace.Name,
				&corev1.Pod{},
				&schedulingv2alpha2.PodGroup{},
				&resourcev1beta1.ResourceClaim{},
				&kaiv1alpha2.BindRequest{},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete test resources")

			err = WaitForNoObjectsInNamespace(ctx, ctrlClient, testNamespace.Name, defaultTimeout, interval,
				&corev1.PodList{},
				&schedulingv2alpha2.PodGroupList{},
				&resourcev1beta1.ResourceClaimList{},
				&kaiv1alpha2.BindRequestList{},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to wait for test resources to be deleted")
		})

		It("Should create a bind request for the pod with DeviceAllocationModeAll resource claim", func(ctx context.Context) {
			resourceClaim := dynamicresource.CreateResourceClaim(
				"test-resource-claim", testNamespace.Name,
				dynamicresource.DeviceRequest{Class: deviceClassName, Count: -1},
			)
			err := ctrlClient.Create(ctx, resourceClaim)
			Expect(err).NotTo(HaveOccurred(), "Failed to create test resource claim")

			testPod := CreatePodObject(testNamespace.Name, "test-pod", corev1.ResourceRequirements{})
			dynamicresource.UseClaim(testPod, resourceClaim)
			Expect(ctrlClient.Create(ctx, testPod)).To(Succeed(), "Failed to create test pod")

			podGroupConfig := podGroupConfig{
				queueName:          testQueue.Name,
				podgroupName:       "test-podgroup",
				minMember:          1,
				topologyConstraint: nil,
			}
			err = GroupPods(ctx, ctrlClient, podGroupConfig, []*corev1.Pod{testPod})
			Expect(err).NotTo(HaveOccurred(), "Failed to group pods")

			err = WaitForPodScheduled(ctx, ctrlClient, testPod.Name, testNamespace.Name, defaultTimeout, interval)
			Expect(err).NotTo(HaveOccurred(), "Failed to wait for test pod to be scheduled")

			// list bind requests
			bindRequests := &kaiv1alpha2.BindRequestList{}
			Expect(ctrlClient.List(ctx, bindRequests, client.InNamespace(testNamespace.Name))).
				To(Succeed(), "Failed to list bind requests")

			Expect(len(bindRequests.Items)).To(Equal(1), "Expected 1 bind request", PrettyPrintBindRequestList(bindRequests))
		})

		It("Should schedule multiple pods with ExactCount resource claims", func(ctx context.Context) {
			var createdPods []*corev1.Pod
			for i := range 20 {
				resourceClaim := dynamicresource.CreateResourceClaim(
					fmt.Sprintf("test-resource-claim-%d", i),
					testNamespace.Name,
					dynamicresource.DeviceRequest{
						Class: deviceClassName,
						Count: 1,
					},
				)
				err := ctrlClient.Create(ctx, resourceClaim)
				Expect(err).NotTo(HaveOccurred(), "Failed to create test resource claim")

				testPod := CreatePodObject(testNamespace.Name, fmt.Sprintf("test-pod-%d", i), corev1.ResourceRequirements{})
				dynamicresource.UseClaim(testPod, resourceClaim)
				Expect(ctrlClient.Create(ctx, testPod)).To(Succeed(), "Failed to create test pod")
				createdPods = append(createdPods, testPod)

				podGroupConfig := podGroupConfig{
					queueName:          testQueue.Name,
					podgroupName:       fmt.Sprintf("test-podgroup-%d", i),
					minMember:          1,
					topologyConstraint: nil,
				}
				err = GroupPods(ctx, ctrlClient, podGroupConfig, []*corev1.Pod{testPod})
				Expect(err).NotTo(HaveOccurred(), "Failed to group pods")
			}

			podMap := map[string]bool{}
			for _, pod := range createdPods {
				err := WaitForPodScheduled(ctx, ctrlClient, pod.Name, testNamespace.Name, defaultTimeout, interval)
				if err != nil {
					continue
				}
				podMap[pod.Name] = true
			}

			Expect(podMap).To(HaveLen(deviceNum), "Expected exactly %d pods to be scheduled", deviceNum)
		})
	})

	Context("topology", func() {
		const (
			topologyName     = "gb2000-topology"
			rackLabelKey     = "nvidia.com/gpu.clique"
			hostnameLabelKey = "kubernetes.io/hostname"

			gpusPerNode     = 4
			numRacks        = 2
			numNodesPerRack = 18
			numNodes        = numRacks * numNodesPerRack
		)

		var (
			topologyNodes []*corev1.Node
			topologyTree  *kueuev1alpha1.Topology
		)

		BeforeAll(func(ctx context.Context) {
			topologyNodes = []*corev1.Node{}

			nodeToRack := map[int]string{}
			for i := range numNodes {
				nodeToRack[i] = fmt.Sprintf("%d", i%numRacks)
			}
			rand.Shuffle(len(nodeToRack), func(i, j int) {
				nodeToRack[i], nodeToRack[j] = nodeToRack[j], nodeToRack[i]
			})

			for i := range numNodes {
				topologyNode := DefaultNodeConfig(fmt.Sprintf("t-node-%d", i))
				topologyNode.GPUs = gpusPerNode
				topologyNode.Labels[rackLabelKey] = fmt.Sprintf("clusterUuid.%s", nodeToRack[i])
				topologyNode.Labels[hostnameLabelKey] = topologyNode.Name
				nodeObj := CreateNodeObject(ctx, ctrlClient, topologyNode)
				Expect(ctrlClient.Create(ctx, nodeObj)).To(Succeed(), "Failed to create topology test node")
				topologyNodes = append(topologyNodes, nodeObj)
			}

			topologyTree = &kueuev1alpha1.Topology{
				ObjectMeta: metav1.ObjectMeta{
					Name: topologyName,
				},
				Spec: kueuev1alpha1.TopologySpec{
					Levels: []kueuev1alpha1.TopologyLevel{
						{NodeLabel: rackLabelKey},
						{NodeLabel: hostnameLabelKey},
					},
				},
			}
			Expect(ctrlClient.Create(ctx, topologyTree)).To(Succeed(), "Failed to create topology tree")
		})

		AfterAll(func(ctx context.Context) {
			if topologyTree != nil {
				Expect(ctrlClient.Delete(ctx, topologyTree)).To(Succeed(), "Failed to delete topology tree")
			}

			for _, node := range topologyNodes {
				Expect(ctrlClient.Delete(ctx, node)).To(Succeed(), "Failed to delete topology test node")
			}

			err := DeleteAllInNamespace(ctx, ctrlClient, testNamespace.Name,
				&corev1.Pod{},
				&schedulingv2alpha2.PodGroup{},
				&resourcev1beta1.ResourceClaim{},
				&kaiv1alpha2.BindRequest{},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete test resources")

			err = WaitForNoObjectsInNamespace(ctx, ctrlClient, testNamespace.Name, defaultTimeout, interval,
				&corev1.PodList{},
				&schedulingv2alpha2.PodGroupList{},
				&resourcev1beta1.ResourceClaimList{},
				&kaiv1alpha2.BindRequestList{},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to wait for test resources to be deleted")
		})

		It("Schedule pods with rack topology constraints", func(ctx context.Context) {
			singlePodResourceRequirements := corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					constants.GpuResource: resource.MustParse(fmt.Sprintf("%d", gpusPerNode)),
				},
				Requests: corev1.ResourceList{
					constants.GpuResource: resource.MustParse(fmt.Sprintf("%d", gpusPerNode)),
				},
			}

			numWorkloadPods := 4
			workloadPods := []*corev1.Pod{}
			for i := 0; i < numWorkloadPods; i++ {
				testPod := CreatePodObject(testNamespace.Name, fmt.Sprintf("test-pod-%d", i), singlePodResourceRequirements)
				Expect(ctrlClient.Create(ctx, testPod)).To(Succeed(), "Failed to create test pod %s", testPod.Name)
				workloadPods = append(workloadPods, testPod)
			}

			podGroupConfig := podGroupConfig{
				queueName:    testQueue.Name,
				podgroupName: "test-podgroup",
				minMember:    1,
				topologyConstraint: &schedulingv2alpha2.TopologyConstraint{
					Topology:              topologyName,
					RequiredTopologyLevel: rackLabelKey,
				},
			}
			err := GroupPods(ctx, ctrlClient, podGroupConfig, workloadPods)
			Expect(err).NotTo(HaveOccurred(), "Failed to group pods")

			for _, pod := range workloadPods {
				err = WaitForPodScheduled(ctx, ctrlClient, pod.Name, testNamespace.Name, defaultTimeout, interval)
				Expect(err).NotTo(HaveOccurred(), "Failed to wait for test pod to be scheduled")
			}

			pods := &corev1.PodList{}
			Expect(ctrlClient.List(ctx, pods, client.InNamespace(testNamespace.Name))).
				To(Succeed(), "Failed to list pods")

			Expect(len(pods.Items)).To(Equal(numWorkloadPods), "Expected %d pods to be created in the test", numWorkloadPods)

			scheduledRacks := map[string]int{}
			for _, pod := range pods.Items {
				scheduledRacks[pod.Labels[rackLabelKey]]++
			}

			Expect(scheduledRacks).To(HaveLen(1), "Expected all pods to be scheduled on the same rack. Got %v", scheduledRacks)
		})
	})
})
