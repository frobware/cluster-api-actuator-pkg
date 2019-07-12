package autoscaler

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/golang/glog"
	g "github.com/onsi/ginkgo"
	o "github.com/onsi/gomega"
	e2e "github.com/openshift/cluster-api-actuator-pkg/pkg/e2e/framework"
	mapiv1beta1 "github.com/openshift/cluster-api/pkg/apis/machine/v1beta1"
	caov1 "github.com/openshift/cluster-autoscaler-operator/pkg/apis/autoscaling/v1"
	caov1beta1 "github.com/openshift/cluster-autoscaler-operator/pkg/apis/autoscaling/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/utils/pointer"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	autoscalingTestLabel                  = "test.autoscaling.label"
	clusterAutoscalerComponent            = "cluster-autoscaler"
	clusterAutoscalerObjectKind           = "ConfigMap"
	clusterAutoscalerScaledUpGroup        = "ScaledUpGroup"
	clusterAutoscalerScaleDownEmpty       = "ScaleDownEmpty"
	clusterAutoscalerMaxNodesTotalReached = "MaxNodesTotalReached"
	pollingInterval                       = 3 * time.Second
	clusterKey                            = "machine.openshift.io/cluster-api-cluster"
	machineSetKey                         = "machine.openshift.io/cluster-api-machineset"

	// TODO(frobware) either share these from cluster-autoscaler, or cluster-api.
	nodeGroupInstanceCPUCapacity    = "machine.openshift.io/instance-cpu-capacity"
	nodeGroupInstanceGPUCapacity    = "machine.openshift.io/instance-gpu-capacity"
	nodeGroupInstanceMemoryCapacity = "machine.openshift.io/instance-memory-capacity"
	nodeGroupInstancePodCapacity    = "machine.openshift.io/instance-pod-capacity"
	nodeGroupScaleFromZero          = "machine.openshift.io/scale-from-zero"
)

func newWorkLoad() *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "workload",
			Namespace: "default",
			Labels:    map[string]string{autoscalingTestLabel: ""},
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "Job",
			APIVersion: "batch/v1",
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "workload",
							Image: "busybox",
							Command: []string{
								"sleep",
								"86400", // 1 day
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									"memory": resource.MustParse("500Mi"),
									"cpu":    resource.MustParse("500m"),
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicy("Never"),
					NodeSelector: map[string]string{
						e2e.WorkerNodeRoleLabel: "",
					},
					Tolerations: []corev1.Toleration{
						{
							Key:      "kubemark",
							Operator: corev1.TolerationOpExists,
						},
					},
				},
			},
			BackoffLimit: pointer.Int32Ptr(4),
			Completions:  pointer.Int32Ptr(100),
			Parallelism:  pointer.Int32Ptr(100),
		},
	}
}

// Build default CA resource to allow fast scaling up and down
func clusterAutoscalerResource(maxNodesTotal int) *caov1.ClusterAutoscaler {
	tenSecondString := "10s"

	// Choose a time that is at least twice as the sync period
	// and that has high least common multiple to avoid a case
	// when a node is considered to be empty even if there are
	// pods already scheduled and running on the node.
	unneededTimeString := "23s"
	return &caov1.ClusterAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: e2e.TestContext.MachineApiNamespace,
			Labels: map[string]string{
				autoscalingTestLabel: "",
			},
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "ClusterAutoscaler",
			APIVersion: "autoscaling.openshift.io/v1",
		},
		Spec: caov1.ClusterAutoscalerSpec{
			ScaleDown: &caov1.ScaleDownConfig{
				Enabled:           true,
				DelayAfterAdd:     &tenSecondString,
				DelayAfterDelete:  &tenSecondString,
				DelayAfterFailure: &tenSecondString,
				UnneededTime:      &unneededTimeString,
			},
			ResourceLimits: &caov1.ResourceLimits{
				MaxNodesTotal: pointer.Int32Ptr(int32(maxNodesTotal)),
			},
		},
	}
}

// Build MA resource from targeted machineset
func machineAutoscalerResource(targetMachineSet *mapiv1beta1.MachineSet, minReplicas, maxReplicas int32) *caov1beta1.MachineAutoscaler {
	return &caov1beta1.MachineAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("autoscale-%s", targetMachineSet.Name),
			Namespace:    e2e.TestContext.MachineApiNamespace,
			Labels: map[string]string{
				autoscalingTestLabel: "",
			},
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "MachineAutoscaler",
			APIVersion: "autoscaling.openshift.io/v1beta1",
		},
		Spec: caov1beta1.MachineAutoscalerSpec{
			MaxReplicas: maxReplicas,
			MinReplicas: minReplicas,
			ScaleTargetRef: caov1beta1.CrossVersionObjectReference{
				Name:       targetMachineSet.Name,
				Kind:       "MachineSet",
				APIVersion: "machine.openshift.io/v1beta1",
			},
		},
	}
}

func newScaleUpCounter(w *eventWatcher, v uint32, scaledGroups map[string]bool) *eventCounter {
	isAutoscalerScaleUpEvent := func(event *corev1.Event) bool {
		return event.Source.Component == clusterAutoscalerComponent &&
			event.Reason == clusterAutoscalerScaledUpGroup &&
			event.InvolvedObject.Kind == clusterAutoscalerObjectKind &&
			strings.HasPrefix(event.Message, "Scale-up: setting group")
	}

	matchGroup := func(event *corev1.Event) bool {
		if !isAutoscalerScaleUpEvent(event) {
			return false
		}
		for k := range scaledGroups {
			if !scaledGroups[k] && strings.HasPrefix(event.Message, fmt.Sprintf("Scale-up: group %s size set to", k)) {
				scaledGroups[k] = true
			}
		}
		return true
	}

	c := newEventCounter(w, matchGroup, v, increment)
	c.enable()

	return c
}

func newScaleDownCounter(w *eventWatcher, v uint32) *eventCounter {
	isAutoscalerScaleDownEvent := func(event *corev1.Event) bool {
		return event.Source.Component == clusterAutoscalerComponent &&
			event.Reason == clusterAutoscalerScaleDownEmpty &&
			event.InvolvedObject.Kind == clusterAutoscalerObjectKind &&
			strings.HasPrefix(event.Message, "Scale-down: empty node")
	}

	c := newEventCounter(w, isAutoscalerScaleDownEvent, v, decrement)
	c.enable()
	return c
}

func newMaxNodesTotalReachedCounter(w *eventWatcher, v uint32) *eventCounter {
	isAutoscalerMaxNodesTotalEvent := func(event *corev1.Event) bool {
		return event.Source.Component == clusterAutoscalerComponent &&
			event.Reason == clusterAutoscalerMaxNodesTotalReached &&
			event.InvolvedObject.Kind == clusterAutoscalerObjectKind &&
			strings.HasPrefix(event.Message, "Max total nodes in cluster reached")
	}

	c := newEventCounter(w, isAutoscalerMaxNodesTotalEvent, v, increment)
	c.enable()
	return c
}

func remaining(t time.Time) time.Duration {
	return t.Sub(time.Now()).Round(time.Second)
}

func newMachineSet(
	clusterName, namespace, name string,
	selectorLabels map[string]string,
	templateLabels map[string]string,
	providerSpec *mapiv1beta1.ProviderSpec,
) *mapiv1beta1.MachineSet {
	ms := mapiv1beta1.MachineSet{
		TypeMeta: metav1.TypeMeta{
			Kind:       "MachineSet",
			APIVersion: "machine.openshift.io/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				clusterKey: clusterName,
			},
		},
		Spec: mapiv1beta1.MachineSetSpec{
			Selector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					clusterKey:    clusterName,
					machineSetKey: name,
				},
			},
			Template: mapiv1beta1.MachineTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						clusterKey:    clusterName,
						machineSetKey: name,
					},
				},
				Spec: mapiv1beta1.MachineSpec{
					ProviderSpec: *providerSpec.DeepCopy(),
				},
			},
			Replicas: pointer.Int32Ptr(0),
		},
	}

	// Copy additional labels but do not overwrite those that
	// already exist.
	for k, v := range selectorLabels {
		if _, exists := ms.Spec.Selector.MatchLabels[k]; !exists {
			ms.Spec.Selector.MatchLabels[k] = v
		}
	}
	for k, v := range templateLabels {
		if _, exists := ms.Spec.Template.ObjectMeta.Labels[k]; !exists {
			ms.Spec.Template.ObjectMeta.Labels[k] = v
		}
	}

	return &ms
}

func dumpClusterAutoscalerLogs(client runtimeclient.Client, restClient *rest.RESTClient) {
	pods := corev1.PodList{}
	caLabels := map[string]string{
		"app": "cluster-autoscaler",
	}
	if err := client.List(context.TODO(), &pods, []runtimeclient.ListOptionFunc{runtimeclient.MatchingLabels(caLabels)}...); err != nil {
		glog.Errorf("Error querying api for clusterAutoscaler pod object: %v", err)
		return
	}
	// We're only expecting one pod but let's log from all that
	// are found. If we see more than one that's indicative of
	// some unexpected problem and we may as well dump its logs.
	for i, pod := range pods.Items {
		req := restClient.Get().Namespace(e2e.TestContext.MachineApiNamespace).Resource("pods").Name(pod.Name).SubResource("log")
		res := req.Do()
		raw, err := res.Raw()
		if err != nil {
			glog.Errorf("Unable to get pod logs: %v", err)
			continue
		}
		glog.Infof("\n\nDumping pod logs: %d/%d, logs from %q:\n%v", i, len(pods.Items), pod.Name, string(raw))
	}
}

var _ = g.Describe("[Feature:Machines][Serial] Autoscaler should", func() {
	g.It("scale up and down", func() {
		defer g.GinkgoRecover()

		clientset, err := e2e.LoadClientset()
		o.Expect(err).NotTo(o.HaveOccurred())

		var client runtimeclient.Client
		client, err = e2e.LoadClient()
		o.Expect(err).NotTo(o.HaveOccurred())

		var restClient *rest.RESTClient
		restClient, err = e2e.LoadRestClient()
		o.Expect(err).NotTo(o.HaveOccurred())

		// Anything we create we must cleanup
		var cleanupObjects []runtime.Object
		defer func() {
			cascadeDelete := metav1.DeletePropagationForeground
			for _, obj := range cleanupObjects {
				switch obj.(type) {
				case *caov1.ClusterAutoscaler:
					dumpClusterAutoscalerLogs(client, restClient)
				}
				if err = client.Delete(context.TODO(), obj, func(opt *runtimeclient.DeleteOptions) {
					opt.PropagationPolicy = &cascadeDelete
				}); err != nil {
					glog.Errorf("error deleting object: %v", err)
				}
			}
		}()

		g.By("Getting machinesets")
		platformMachineSets, err := e2e.GetMachineSets(context.TODO(), client)
		o.Expect(err).NotTo(o.HaveOccurred())
		o.Expect(len(platformMachineSets)).To(o.BeNumerically(">=", 1))

		g.By("Getting machines")
		machines, err := e2e.GetMachines(context.TODO(), client)
		o.Expect(err).NotTo(o.HaveOccurred())
		o.Expect(len(machines)).To(o.BeNumerically(">=", 1))

		g.By("Getting nodes")
		nodes, err := e2e.GetNodes(client)
		o.Expect(err).NotTo(o.HaveOccurred())
		o.Expect(len(nodes)).To(o.BeNumerically(">=", 1))

		g.By("Deriving CPU and Memory capacity from a real node")
		targetMachineSet := platformMachineSets[0]
		workerNodes, err := e2e.GetWorkerNodes(client)
		o.Expect(err).NotTo(o.HaveOccurred())
		cpuCapacity := workerNodes[0].Status.Capacity[corev1.ResourceCPU]
		memCapacity := workerNodes[0].Status.Capacity[corev1.ResourceMemory]
		o.Expect(cpuCapacity).ShouldNot(o.BeNil())
		o.Expect(cpuCapacity.String()).ShouldNot(o.BeEmpty())
		o.Expect(memCapacity).ShouldNot(o.BeNil())
		o.Expect(memCapacity.String()).ShouldNot(o.BeEmpty())

		var machineSets [3]*mapiv1beta1.MachineSet
		g.By(fmt.Sprintf("Creating %v MachineSets", len(machineSets)))
		for i := 0; i < 3; i++ {
			machineSets[i] = newMachineSet(targetMachineSet.Labels[clusterKey],
				targetMachineSet.Namespace,
				fmt.Sprintf("autoscaler-%d-%s", i, targetMachineSet.Name),
				targetMachineSet.Spec.Selector.MatchLabels,
				targetMachineSet.Spec.Template.ObjectMeta.Labels,
				&targetMachineSet.Spec.Template.Spec.ProviderSpec)
			machineSets[i].Annotations = map[string]string{
				nodeGroupInstanceCPUCapacity:    cpuCapacity.String(),
				nodeGroupInstanceMemoryCapacity: memCapacity.String(),
				nodeGroupScaleFromZero:          "true",
			}
			machineSets[i].Spec.Template.Spec.ObjectMeta.Labels = map[string]string{
				e2e.WorkerNodeRoleLabel: "",
			}
			o.Expect(client.Create(context.TODO(), machineSets[i])).Should(o.Succeed())
			cleanupObjects = append(cleanupObjects, runtime.Object(machineSets[i]))
		}

		g.By(fmt.Sprintf("Creating %v machineautoscalers", len(machineSets)))
		var clusterExpansionSize int
		for i := range machineSets {
			min := pointer.Int32PtrDerefOr(machineSets[i].Spec.Replicas, 1)
			// We only want each machineautoscaler
			// resource to be able to grow by one
			// additional node.
			max := min + 1
			clusterExpansionSize += 1

			glog.Infof("Create MachineAutoscaler backed by MachineSet %s/%s - min:%v, max:%v", machineSets[i].Namespace, machineSets[i].Name, min, max)
			asr := machineAutoscalerResource(machineSets[i], min, max)
			o.Expect(client.Create(context.TODO(), asr)).Should(o.Succeed())
			cleanupObjects = append(cleanupObjects, runtime.Object(asr))
		}
		o.Expect(clusterExpansionSize).To(o.BeNumerically(">", 1))

		// We want to scale out to max-cluster-size-1. We
		// choose max-1 because we want to test that
		// maxNodesTotal is respected by the
		// cluster-autoscaler. If maxNodesTotal ==
		// max-cluster-size then no MaxNodesTotalReached
		// event will be generated.
		maxNodesTotal := len(nodes) + clusterExpansionSize - 1

		eventWatcher := newEventWatcher(clientset)
		o.Expect(eventWatcher.run()).Should(o.BeTrue())
		defer eventWatcher.stop()

		// Log cluster-autoscaler events
		eventWatcher.onEvent(matchAnyEvent, func(e *corev1.Event) {
			if e.Source.Component == clusterAutoscalerComponent {
				glog.Infof("%s: %s", e.InvolvedObject.Name, e.Message)
			}
		}).enable()

		g.By(fmt.Sprintf("Creating ClusterAutoscaler configured with maxNodesTotal:%v", maxNodesTotal))
		clusterAutoscaler := clusterAutoscalerResource(maxNodesTotal)
		o.Expect(client.Create(context.TODO(), clusterAutoscaler)).Should(o.Succeed())
		cleanupObjects = append(cleanupObjects, runtime.Object(clusterAutoscaler))

		g.By("Creating scale-out workload")
		scaledGroups := map[string]bool{}
		for i := range machineSets {
			scaledGroups[path.Join(machineSets[i].Namespace, machineSets[i].Name)] = false
		}
		scaleUpCounter := newScaleUpCounter(eventWatcher, 0, scaledGroups)
		maxNodesTotalReachedCounter := newMaxNodesTotalReachedCounter(eventWatcher, 0)
		workload := newWorkLoad()
		o.Expect(client.Create(context.TODO(), workload)).Should(o.Succeed())
		cleanupObjects = append(cleanupObjects, runtime.Object(workload))
		testDuration := time.Now().Add(time.Duration(e2e.WaitLong))
		o.Eventually(func() bool {
			v := scaleUpCounter.get()
			glog.Infof("[%s remaining] Expecting %v %q events; observed %v",
				remaining(testDuration), clusterExpansionSize-1, clusterAutoscalerScaledUpGroup, v)
			return v == uint32(clusterExpansionSize-1)
		}, e2e.WaitLong, pollingInterval).Should(o.BeTrue())

		// The cluster-autoscaler can keep on generating
		// ScaledUpGroup events but in this scenario we are
		// expecting no more as we explicitly capped the
		// cluster size with maxNodesTotal (i.e.,
		// clusterExpansionSize -1). We run for a period of
		// time asserting that the cluster does not exceed the
		// capped size.
		testDuration = time.Now().Add(time.Duration(e2e.WaitShort))
		o.Eventually(func() uint32 {
			v := maxNodesTotalReachedCounter.get()
			glog.Infof("[%s remaining] Waiting for %s to generate a %q event; observed %v",
				remaining(testDuration), clusterAutoscalerComponent, clusterAutoscalerMaxNodesTotalReached, v)
			return v
		}, e2e.WaitShort, 3*time.Second).Should(o.BeNumerically(">=", 1))

		testDuration = time.Now().Add(time.Duration(e2e.WaitShort))
		o.Consistently(func() bool {
			v := scaleUpCounter.get()
			glog.Infof("[%s remaining] At max cluster size and expecting no more %q events; currently have %v, max=%v",
				remaining(testDuration), clusterAutoscalerScaledUpGroup, v, clusterExpansionSize-1)
			return v == uint32(clusterExpansionSize-1)
		}, e2e.WaitShort, pollingInterval).Should(o.BeTrue())

		g.By("Deleting workload")
		scaleDownCounter := newScaleDownCounter(eventWatcher, uint32(clusterExpansionSize-1))
		o.Expect(e2e.DeleteObjectsByLabels(context.TODO(), client, map[string]string{autoscalingTestLabel: ""}, &batchv1.JobList{})).Should(o.Succeed())
		if len(cleanupObjects) > 1 && cleanupObjects[len(cleanupObjects)-1] == workload {
			cleanupObjects = cleanupObjects[:len(cleanupObjects)-1]
		}
		testDuration = time.Now().Add(time.Duration(e2e.WaitLong))
		o.Eventually(func() uint32 {
			v := scaleDownCounter.get()
			glog.Infof("[%s remaining] Waiting for %s to generate %v more %q events",
				remaining(testDuration), clusterAutoscalerComponent, v, clusterAutoscalerScaleDownEmpty)
			return v
		}, e2e.WaitLong, pollingInterval).Should(o.BeZero())

		g.By("Waiting for scaled up nodes to be deleted")
		testDuration = time.Now().Add(time.Duration(e2e.WaitMedium))
		o.Eventually(func() int {
			currentNodes, err := e2e.GetNodes(client)
			o.Expect(err).NotTo(o.HaveOccurred())
			glog.Infof("[%s remaining] Waiting fo cluster to reach original node count of %v; currently have %v",
				remaining(testDuration), len(nodes), len(currentNodes))
			return len(currentNodes)
		}, e2e.WaitMedium, pollingInterval).Should(o.Equal(len(nodes)))

		g.By("Waiting for scaled up machines to be deleted")
		testDuration = time.Now().Add(time.Duration(e2e.WaitMedium))
		o.Eventually(func() int {
			currentMachines, err := e2e.GetMachines(context.TODO(), client)
			o.Expect(err).NotTo(o.HaveOccurred())
			glog.Infof("[%s remaining] Waiting fo cluster to reach original machine count of %v; currently have %v",
				remaining(testDuration), len(machines), len(currentMachines))
			return len(currentMachines)
		}, e2e.WaitMedium, pollingInterval).Should(o.Equal(len(machines)))
	})
})
