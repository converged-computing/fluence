// Admission control, the rule the flux-quantum jobtap plugin applies at submit.
//
// A quantum group is admitted only if the cores all of its members will need,
// plus what is promised to admitted groups that have not started, fit in what
// is reachable. Reachable is the free cores plus the cores held by running
// pods that are unprotected and below the priority workers are ungated with.
// The budget is the allocatable cores of schedulable nodes less ReserveCores.
// A refusal is an admission denial, so it lands at kubectl apply or on the
// owning Job, not as a pod left pending with a backend it cannot use.
//
// Every quantum pod is marked protected, which keeps it out of preemptible,
// and a pod that arrives already marked is refused.
package webhook

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/converged-computing/fluence/pkg/placement"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// ProtectedAnnotation marks a pod as one of ours and not preemptible. Set by
	// the webhook on every quantum pod. A submission with it is refused.
	ProtectedAnnotation = "fluence.flux-framework.org/protected"
	ProtectedValue      = "quantum"

	// AdmittedCoresAnnotation records, on the group's PodGroup (or on a
	// standalone pod), the core total the group was admitted with.
	AdmittedCoresAnnotation = "fluence.flux-framework.org/admitted-cores"

	// AdmittedGroupAnnotation names, on every member pod, the record it belongs
	// to as namespace/name, so running members are subtracted from the promise.
	AdmittedGroupAnnotation = "fluence.flux-framework.org/admitted-group"

	// PreemptiblePriorityCeiling is the fluence-quantum-classical priority that
	// workers are ungated with. Running work below it is what they could
	// preempt, so that is what counts as preemptible.
	PreemptiblePriorityCeiling int32 = 1000000
)

// Capacity is one reading of the budget, in whole cores.
type Capacity struct {
	Budget      int // allocatable cores of schedulable nodes, less the reserve
	Running     int // cores of every bound, non-terminal pod on those nodes
	Free        int // Budget - Running, never negative
	Preemptible int // running cores that are unprotected and below the ceiling
	Promised    int // admitted to quantum groups whose members are not all running
}

// Reachable is what a new group could have. Free cores plus what preemption
// could free.
func (c Capacity) Reachable() int { return c.Free + c.Preemptible }

// Admit refuses the group named key when its cores do not fit. With admission
// control off, or no client, every group is admitted. An API error admits too
// with a log line, since a webhook that cannot read the cluster must not block
// pods.
func (m *Mutator) Admit(ctx context.Context, pod *corev1.Pod, key string, cores int) error {
	if !m.AdmissionControl || m.Clientset == nil || cores <= 0 {
		return nil
	}
	c, err := m.Capacity(ctx)
	if err != nil {
		log.Printf("[fluence-webhook] admission: cannot read capacity, admitting %s: %v", key, err)
		return nil
	}
	if cores+c.Promised > c.Reachable() {
		return fmt.Errorf("quantum: no room for another pair. %s needs %d cores, %d are already "+
			"promised to pairs that have not started, and only %d are reachable (%d free, %d that "+
			"could be preempted). Wait for one to finish or ask for fewer cores",
			key, cores, c.Promised, c.Reachable(), c.Free, c.Preemptible)
	}
	log.Printf("[fluence-webhook] admission: %s admitted with %d cores (free=%d preemptible=%d promised=%d budget=%d)",
		key, cores, c.Free, c.Preemptible, c.Promised, c.Budget)
	return nil
}

// RecordAdmission writes the admitted total on the PodGroup once. Later
// members of the group skip the check because of it, and later groups count
// it as promised.
func (m *Mutator) RecordAdmission(ctx context.Context, namespace, podGroup string, cores int) {
	if m.Clientset == nil || cores <= 0 {
		return
	}
	pg, err := m.Clientset.SchedulingV1alpha2().PodGroups(namespace).Get(ctx, podGroup, metav1.GetOptions{})
	if err != nil {
		log.Printf("[fluence-webhook] admission: no PodGroup %s/%s to record %d cores on: %v", namespace, podGroup, cores, err)
		return
	}
	if pg.Annotations[AdmittedCoresAnnotation] != "" {
		return
	}
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, AdmittedCoresAnnotation, strconv.Itoa(cores))
	if _, err := m.Clientset.SchedulingV1alpha2().PodGroups(namespace).Patch(
		ctx, podGroup, types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		log.Printf("[fluence-webhook] admission: recording %d cores on PodGroup %s/%s failed: %v", cores, namespace, podGroup, err)
	}
}

// Capacity reads the cluster and computes the budget.
func (m *Mutator) Capacity(ctx context.Context) (Capacity, error) {
	nodes, err := m.Clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return Capacity{}, fmt.Errorf("list nodes: %w", err)
	}
	pods, err := m.Clientset.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return Capacity{}, fmt.Errorf("list pods: %w", err)
	}
	groups, err := m.Clientset.SchedulingV1alpha2().PodGroups(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return Capacity{}, fmt.Errorf("list podgroups: %w", err)
	}

	var c Capacity
	schedulable := map[string]bool{}
	for _, n := range nodes.Items {
		if !nodeSchedulable(n) {
			continue
		}
		schedulable[n.Name] = true
		c.Budget += nodeCores(n)
	}
	c.Budget -= m.ReserveCores
	if c.Budget < 0 {
		c.Budget = 0
	}

	// cores running per admission record, keyed namespace/name
	runningOf := map[string]int{}
	// the records themselves. PodGroups, and standalone pods that have their own
	records := map[string]int{}
	for _, pg := range groups.Items {
		if n, err := strconv.Atoi(pg.Annotations[AdmittedCoresAnnotation]); err == nil && n > 0 {
			records[pg.Namespace+"/"+pg.Name] = n
		}
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if n, err := strconv.Atoi(p.Annotations[AdmittedCoresAnnotation]); err == nil && n > 0 {
			records[p.Namespace+"/"+p.Name] = n
		}
		if !podBound(p) || !schedulable[p.Spec.NodeName] {
			continue
		}
		cores := placement.PodCores(p)
		c.Running += cores
		if p.Annotations[ProtectedAnnotation] == "" && podPriority(p) < PreemptiblePriorityCeiling {
			c.Preemptible += cores
		}
		if g := p.Annotations[AdmittedGroupAnnotation]; g != "" {
			runningOf[g] += cores
		}
	}
	for key, total := range records {
		if left := total - runningOf[key]; left > 0 {
			c.Promised += left
		}
	}
	c.Free = c.Budget - c.Running
	if c.Free < 0 {
		c.Free = 0
	}
	return c, nil
}

// nodeSchedulable mirrors the scheduler. Cordoned nodes and nodes with a
// NoSchedule or NoExecute taint are not part of the budget.
func nodeSchedulable(n corev1.Node) bool {
	if n.Spec.Unschedulable {
		return false
	}
	for _, t := range n.Spec.Taints {
		if t.Effect == corev1.TaintEffectNoSchedule || t.Effect == corev1.TaintEffectNoExecute {
			return false
		}
	}
	return true
}

// nodeCores is the node's allocatable cpu in whole cores, rounded down so a
// fractional core is never promised.
func nodeCores(n corev1.Node) int {
	q := n.Status.Allocatable[corev1.ResourceCPU]
	return int(q.MilliValue() / 1000)
}

// podBound reports a pod that holds its node, bound and not finished.
func podBound(p *corev1.Pod) bool {
	return p.Spec.NodeName != "" && p.Status.Phase != corev1.PodSucceeded && p.Status.Phase != corev1.PodFailed
}

func podPriority(p *corev1.Pod) int32 {
	if p.Spec.Priority != nil {
		return *p.Spec.Priority
	}
	return 0
}
