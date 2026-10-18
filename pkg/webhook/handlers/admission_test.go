// Admission control tests.
package handlers

import (
	"context"
	"strings"
	"testing"

	"github.com/converged-computing/fluence/pkg/webhook"
	"github.com/converged-computing/fluence/pkg/webhook/spec"

	corev1 "k8s.io/api/core/v1"
	schedulingv1alpha2 "k8s.io/api/scheduling/v1alpha2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func cpuQty(cores int64) resource.Quantity { return *resource.NewQuantity(cores, resource.DecimalSI) }

func runningPod(name, node string, cores int64, priority int32, protected bool) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Annotations: map[string]string{}},
		Spec: corev1.PodSpec{
			NodeName: node,
			Priority: &priority,
			Containers: []corev1.Container{{Name: "c", Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: cpuQty(cores)}}}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if protected {
		p.Annotations[webhook.ProtectedAnnotation] = webhook.ProtectedValue
	}
	return p
}

func budgetCluster() []runtime.Object {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Status:     corev1.NodeStatus{Allocatable: corev1.ResourceList{corev1.ResourceCPU: cpuQty(8)}},
	}
	cordoned := &corev1.Node{ // not part of the budget
		ObjectMeta: metav1.ObjectMeta{Name: "n2"},
		Spec:       corev1.NodeSpec{Unschedulable: true},
		Status:     corev1.NodeStatus{Allocatable: corev1.ResourceList{corev1.ResourceCPU: cpuQty(100)}},
	}
	admitted := &schedulingv1alpha2.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Name: "earlier", Namespace: "default",
		Annotations: map[string]string{webhook.AdmittedCoresAnnotation: "3"}}}
	return []runtime.Object{
		node, cordoned, admitted,
		runningPod("batch", "n1", 2, 0, false),                                     // preemptible
		runningPod("pair", "n1", 2, webhook.PreemptiblePriorityCeiling, true),     // protected, high priority
		runningPod("elsewhere", "n2", 50, 0, false),                                // on the cordoned node, ignored
	}
}

func budgetMutator(objs ...runtime.Object) *webhook.Mutator {
	return &webhook.Mutator{Clientset: fake.NewSimpleClientset(objs...), AdmissionControl: true}
}

func standaloneQPU(cores int64) *corev1.Pod {
	p := qpuPod("fluence")
	p.Name, p.Namespace = "sampler", "default"
	p.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = cpuQty(cores)
	return p
}

func TestCapacityArithmetic(t *testing.T) {
	m := budgetMutator(budgetCluster()...)
	c, err := m.Capacity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := webhook.Capacity{Budget: 8, Running: 4, Free: 4, Preemptible: 2, Promised: 3}
	if c != want {
		t.Fatalf("capacity = %+v, want %+v", c, want)
	}
	if c.Reachable() != 6 {
		t.Fatalf("reachable = %d, want 6", c.Reachable())
	}
}

func TestReserveComesOffTheBudget(t *testing.T) {
	m := budgetMutator(budgetCluster()...)
	m.ReserveCores = 3
	c, err := m.Capacity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c.Budget != 5 || c.Free != 1 {
		t.Fatalf("with 3 reserved: budget=%d free=%d, want 5 and 1", c.Budget, c.Free)
	}
}

func TestStandaloneAdmittedWhenItFits(t *testing.T) {
	m := budgetMutator(budgetCluster()...)
	ops, err := m.MutateOrDeny(context.Background(), standaloneQPU(3)) // 3 + 3 promised <= 6 reachable
	if err != nil {
		t.Fatalf("expected admission, got refusal: %v", err)
	}
	ann := annotationsAdded(ops)
	if ann[webhook.ProtectedAnnotation] != webhook.ProtectedValue {
		t.Errorf("admitted pod must be marked protected, got %v", ann)
	}
	if ann[webhook.AdmittedCoresAnnotation] != "3" {
		t.Errorf("standalone pod must have its own record, got %v", ann)
	}
	if ann[webhook.AdmittedGroupAnnotation] != "default/sampler" {
		t.Errorf("standalone pod must point at its own record, got %v", ann)
	}
}

func TestStandaloneRefusedWhenItDoesNotFit(t *testing.T) {
	m := budgetMutator(budgetCluster()...)
	_, err := m.MutateOrDeny(context.Background(), standaloneQPU(4)) // 4 + 3 > 6
	if err == nil {
		t.Fatal("expected refusal")
	}
	if !strings.Contains(err.Error(), "no room for another pair") || !strings.Contains(err.Error(), "needs 4 cores") {
		t.Errorf("refusal should explain the budget, got: %v", err)
	}
}

func TestPromiseIsReleasedAsMembersRun(t *testing.T) {
	// the earlier group's 3 promised cores shrink by what its members run
	objs := budgetCluster()
	member := runningPod("earlier-0", "n1", 2, webhook.PreemptiblePriorityCeiling, true)
	member.Annotations[webhook.AdmittedGroupAnnotation] = "default/earlier"
	m := budgetMutator(append(objs, member)...)
	c, err := m.Capacity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c.Promised != 1 || c.Running != 6 {
		t.Fatalf("promised=%d running=%d, want 1 and 6", c.Promised, c.Running)
	}
}

func TestSelfProtectedPodRefused(t *testing.T) {
	m := budgetMutator(budgetCluster()...)
	p := standaloneQPU(1)
	p.Annotations = map[string]string{webhook.ProtectedAnnotation: "please"}
	if _, err := m.MutateOrDeny(context.Background(), p); err == nil {
		t.Fatal("a pod that marks itself protected must be refused")
	}
}

func TestAdmissionOffAdmitsEverything(t *testing.T) {
	m := budgetMutator(budgetCluster()...)
	m.AdmissionControl = false
	if _, err := m.MutateOrDeny(context.Background(), standaloneQPU(100)); err != nil {
		t.Fatalf("admission control off must not refuse: %v", err)
	}
}

func TestNonQuantumPodIsNotBudgeted(t *testing.T) {
	m := budgetMutator(budgetCluster()...)
	p := cpuPod("fluence")
	p.Name, p.Namespace = "classical", "default"
	p.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = cpuQty(100)
	if _, err := m.MutateOrDeny(context.Background(), p); err != nil {
		t.Fatalf("the budget is for quantum groups only: %v", err)
	}
}

func TestRecordAdmissionWritesOnce(t *testing.T) {
	pg := &schedulingv1alpha2.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "qg", Namespace: "default"}}
	m := budgetMutator(pg)
	ctx := context.Background()
	m.RecordAdmission(ctx, "default", "qg", 6)
	m.RecordAdmission(ctx, "default", "qg", 99) // ignored, already recorded
	got, err := m.Clientset.SchedulingV1alpha2().PodGroups("default").Get(ctx, "qg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Annotations[webhook.AdmittedCoresAnnotation] != "6" {
		t.Fatalf("record = %q, want 6", got.Annotations[webhook.AdmittedCoresAnnotation])
	}
}

// annotationsAdded collects the annotations a patch adds, whether as one map op
// or per-key ops.
func annotationsAdded(ops []spec.Op) map[string]string {
	out := map[string]string{}
	for _, op := range ops {
		if op.Op != "add" {
			continue
		}
		if op.Path == "/metadata/annotations" {
			if m, ok := op.Value.(map[string]string); ok {
				for k, v := range m {
					out[k] = v
				}
			}
			continue
		}
		const prefix = "/metadata/annotations/"
		if strings.HasPrefix(op.Path, prefix) {
			key := strings.ReplaceAll(strings.ReplaceAll(strings.TrimPrefix(op.Path, prefix), "~1", "/"), "~0", "~")
			if v, ok := op.Value.(string); ok {
				out[key] = v
			}
		}
	}
	return out
}
