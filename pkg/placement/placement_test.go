package placement

import (
	"testing"

	"github.com/converged-computing/fluence/pkg/jobspec"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func podWith(name string, req corev1.ResourceList) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Resources: corev1.ResourceRequirements{Requests: req}}}},
	}
}

func qty(n int64) resource.Quantity { return *resource.NewQuantity(n, resource.DecimalSI) }

// withType returns the count for a given Fluxion type in a jobspec slot's
// `with`.
func withType(js *jobspec.Jobspec, t string) (int, bool) {
	for _, w := range js.Resources[0].With {
		if w.Type == t {
			return w.Count, true
		}
	}
	return 0, false
}

// constraintProps returns the constraint property list from a jobspec's
// attributes.system.constraints.properties.
func constraintProps(t *testing.T, js *jobspec.Jobspec) []string {
	t.Helper()
	sys, ok := js.Attributes["system"].(map[string]interface{})
	if !ok {
		t.Fatalf("no attributes.system: %#v", js.Attributes)
	}
	cons, ok := sys["constraints"].(map[string]interface{})
	if !ok {
		t.Fatalf("no constraints: %#v", sys)
	}
	props, ok := cons["properties"].([]string)
	if !ok {
		t.Fatalf("properties not []string: %#v", cons["properties"])
	}
	return props
}

func hasProp(props []string, want string) bool {
	for _, p := range props {
		if p == want {
			return true
		}
	}
	return false
}

// A classical group (no virtual resources) yields exactly one jobspec: the
// compute slot, constrained to virtual=false.
func TestClassicalSingleMatch(t *testing.T) {
	pods := []corev1.Pod{
		podWith("p0", corev1.ResourceList{corev1.ResourceCPU: qty(4), "nvidia.com/gpu": qty(1)}),
		podWith("p1", corev1.ResourceList{corev1.ResourceCPU: qty(4), "nvidia.com/gpu": qty(1)}),
	}
	specs, err := JobspecsForGroup("grp", pods, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 1 {
		t.Fatalf("classical group should yield 1 jobspec, got %d", len(specs))
	}
	js := specs[0]
	if js.Resources[0].Count != 2 {
		t.Errorf("slot count = %d, want 2", js.Resources[0].Count)
	}
	if c, _ := withType(js, "core"); c != 4 {
		t.Errorf("core = %d, want 4", c)
	}
	if c, _ := withType(js, "gpu"); c != 1 {
		t.Errorf("gpu = %d, want 1", c)
	}
	if !hasProp(constraintProps(t, js), VirtualPropertyFalse) {
		t.Errorf("compute jobspec must constrain virtual=false; got %v", constraintProps(t, js))
	}
}

// A device request yields a second jobspec constrained to virtual=true (the
// requested type rides the slot's `with`, not a class= constraint), while the
// compute jobspec stays virtual=false and the device type does NOT leak into the
// compute slot.
// A quantum gang lists pods in arbitrary (informer) order, and only the leader
// requests the qpu. The device match must be emitted even when the device
// requester is not pods[0]; otherwise the group schedules with no backend.
func TestGroupDeviceMatchWhenLeaderNotFirst(t *testing.T) {
	worker := podWith("w0", corev1.ResourceList{corev1.ResourceCPU: qty(1)})
	leader := podWith("leader", corev1.ResourceList{
		corev1.ResourceCPU:            qty(1),
		FluxionResourcePrefix + "qpu": qty(1),
	})
	// Leader deliberately placed last.
	pods := []corev1.Pod{worker, worker, leader}
	specs, err := JobspecsForGroup("qgrp", pods, map[string]bool{"qpu": true})
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 2 {
		t.Fatalf("expected 2 jobspecs (compute + device) even with leader last, got %d", len(specs))
	}
	device := specs[1]
	if !hasProp(constraintProps(t, device), "class=qpu") {
		t.Errorf("device match must select class=qpu; got %v", constraintProps(t, device))
	}
}

func TestDeviceProducesSecondMatch(t *testing.T) {
	p := podWith("q", corev1.ResourceList{
		corev1.ResourceCPU:            qty(1),
		FluxionResourcePrefix + "qpu": qty(1),
	})
	known := map[string]bool{"qpu": true}
	specs, err := JobspecsForGroup("qgrp", []corev1.Pod{p}, known)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 2 {
		t.Fatalf("expected 2 jobspecs (compute + device), got %d", len(specs))
	}

	compute := specs[0]
	if !hasProp(constraintProps(t, compute), VirtualPropertyFalse) {
		t.Errorf("compute constraint = %v, want virtual=false", constraintProps(t, compute))
	}
	if _, ok := withType(compute, "qpu"); ok {
		t.Error("qpu must not appear in the compute jobspec")
	}
	if c, _ := withType(compute, "core"); c != 1 {
		t.Errorf("compute core = %d, want 1", c)
	}

	device := specs[1]
	props := constraintProps(t, device)
	if !hasProp(props, VirtualPropertyTrue) {
		t.Errorf("device constraint must include virtual=true; got %v", props)
	}
	// The device is selected by class=<requested-type>: every virtual resource is
	// a node carrying its class, so the constraint picks the right one. The slot
	// requests a node (every virtual resource is a node), count from the request.
	if !hasProp(props, "class=qpu") {
		t.Errorf("device constraint must include class=qpu; got %v", props)
	}
	if len(props) != 2 {
		t.Errorf("device constraint should be [virtual=true class=qpu]; got %v", props)
	}
	if c, ok := withType(device, "node"); !ok || c != 1 {
		t.Errorf("device should request node count 1; got %d (ok=%v)", c, ok)
	}
}

// A device-only request still forces a compute jobspec (the probing pod needs a
// node), so there are two matches: compute (core=1, virtual=false) and device.
func TestDeviceOnlyStillForcesCompute(t *testing.T) {
	p := podWith("q", corev1.ResourceList{FluxionResourcePrefix + "qpu": qty(1)})
	specs, err := JobspecsForGroup("qonly", []corev1.Pod{p}, map[string]bool{"qpu": true})
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 2 {
		t.Fatalf("expected 2 jobspecs, got %d", len(specs))
	}
	if c, _ := withType(specs[0], "core"); c != 1 {
		t.Errorf("forced compute core = %d, want 1", c)
	}
}

// Requesting a device type the graph does not model is a hard error.
func TestUnknownDeviceErrors(t *testing.T) {
	p := podWith("q", corev1.ResourceList{FluxionResourcePrefix + "fpga": qty(1)})
	_, err := JobspecsForGroup("grp", []corev1.Pod{p}, map[string]bool{"qpu": true})
	if err == nil {
		t.Fatal("expected an error for an unmodeled device type")
	}
}

// duration is 0 (hold until cancel) on every generated jobspec.
func TestHoldDurationZero(t *testing.T) {
	p := podWith("q", corev1.ResourceList{
		corev1.ResourceCPU:            qty(1),
		FluxionResourcePrefix + "qpu": qty(1),
	})
	specs, err := JobspecsForGroup("g", []corev1.Pod{p}, map[string]bool{"qpu": true})
	if err != nil {
		t.Fatal(err)
	}
	for i, js := range specs {
		sys := js.Attributes["system"].(map[string]interface{})
		if sys["duration"] != 0 {
			t.Errorf("jobspec %d duration = %v, want 0", i, sys["duration"])
		}
	}
}

// A compute allocation: node vertices carrying virtual=false are bind targets.
func TestPlacementComputeNodes(t *testing.T) {
	alloc := `{"graph":{"nodes":[
	  {"metadata":{"type":"node","name":"node-a","properties":{"virtual=false":""}}},
	  {"metadata":{"type":"core","name":"core0"}},
	  {"metadata":{"type":"node","name":"node-b","properties":{"virtual=false":""}}}]}}`
	p, err := PlacementFromAllocation(alloc)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Nodes) != 2 || p.Nodes[0] != "node-a" || p.Nodes[1] != "node-b" {
		t.Fatalf("nodes = %v, want [node-a node-b]", p.Nodes)
	}
	if p.Backend != "" {
		t.Fatalf("compute allocation should have no backend, got %q", p.Backend)
	}
}

// A device allocation: the virtual=true node is the backend identity, not a bind
// target. Its qpu/qubit children do not become the backend name.
func TestPlacementVirtualBackend(t *testing.T) {
	alloc := `{"graph":{"nodes":[
	  {"metadata":{"type":"node","name":"rigetti_cepheus","properties":{"virtual=true":"","class=qdevice":""}}},
	  {"metadata":{"type":"qpu","name":"qpu0"}},
	  {"metadata":{"type":"qubit","name":"qubit0"}}]}}`
	p, err := PlacementFromAllocation(alloc)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Nodes) != 0 {
		t.Fatalf("device allocation should bind no compute node, got %v", p.Nodes)
	}
	if p.Backend != "rigetti_cepheus" {
		t.Fatalf("backend = %q, want rigetti_cepheus (the virtual=true node)", p.Backend)
	}
}

// An unmarked node (graph built without markers) is treated as a bind target, so
// a plain classical graph still places correctly.
func TestPlacementUnmarkedNodeIsCompute(t *testing.T) {
	alloc := `{"graph":{"nodes":[
	  {"metadata":{"type":"node","name":"plain-node"}}]}}`
	p, err := PlacementFromAllocation(alloc)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Nodes) != 1 || p.Nodes[0] != "plain-node" {
		t.Fatalf("nodes = %v, want [plain-node]", p.Nodes)
	}
	if p.Backend != "" {
		t.Fatalf("unmarked node should not be a backend, got %q", p.Backend)
	}
}
