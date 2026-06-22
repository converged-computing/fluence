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

func qpuPodWithRequires(name string, requires map[string]string) corev1.Pod {
	p := podWith(name, corev1.ResourceList{
		corev1.ResourceCPU:            qty(1),
		FluxionResourcePrefix + "qpu": qty(1),
	})
	if len(requires) > 0 && p.Annotations == nil {
		p.Annotations = map[string]string{}
	}
	for k, v := range requires {
		p.Annotations[RequireAnnotationPrefix+k] = v
	}
	return p
}

// Zero require- annotations: the device match must carry ONLY the base
// constraints, nothing extra (over-constraining would break unconstrained runs).
func TestNoRequireAnnotationsAddsNoConstraints(t *testing.T) {
	p := qpuPodWithRequires("q", nil)
	specs, err := JobspecsForGroup("g", []corev1.Pod{p}, map[string]bool{"qpu": true})
	if err != nil {
		t.Fatal(err)
	}
	props := constraintProps(t, specs[1])
	if len(props) != 2 {
		t.Errorf("expected exactly [virtual=true class=qpu] with no requires; got %v", props)
	}
}

// Exactly one require- constraint.
func TestSingleRequireConstraint(t *testing.T) {
	p := qpuPodWithRequires("q", map[string]string{"qrmi_type": "braket-gate"})
	specs, err := JobspecsForGroup("g", []corev1.Pod{p}, map[string]bool{"qpu": true})
	if err != nil {
		t.Fatal(err)
	}
	props := constraintProps(t, specs[1])
	if len(props) != 3 {
		t.Errorf("expected base 2 + 1 require = 3 constraints; got %v", props)
	}
	if !hasProp(props, "fluxion.flux-framework.org/qrmi_type=braket-gate") {
		t.Errorf("missing the single required constraint; got %v", props)
	}
}

// Several require- constraints, and the same constraint repeated across pods in
// the group must be de-duplicated (not added twice).
func TestMultipleRequireConstraintsAreDeduped(t *testing.T) {
	leader := qpuPodWithRequires("leader", map[string]string{
		"qrmi_type": "braket-gate",
		"vendor":    "amazon",
		"backend":   "sv1",
	})
	// a worker that happens to repeat one of the same require- annotations
	worker := qpuPodWithRequires("w0", map[string]string{"vendor": "amazon"})
	specs, err := JobspecsForGroup("g", []corev1.Pod{leader, worker},
		map[string]bool{"qpu": true})
	if err != nil {
		t.Fatal(err)
	}
	props := constraintProps(t, specs[1])
	// base 2 + three distinct requires = 5 (vendor=amazon counted once)
	if len(props) != 5 {
		t.Errorf("expected 5 constraints (2 base + 3 distinct requires, deduped); got %v", props)
	}
	for _, want := range []string{
		"fluxion.flux-framework.org/qrmi_type=braket-gate",
		"fluxion.flux-framework.org/vendor=amazon",
		"fluxion.flux-framework.org/backend=sv1",
	} {
		if !hasProp(props, want) {
			t.Errorf("missing %q; got %v", want, props)
		}
	}
	// dedup: vendor=amazon must appear exactly once
	n := 0
	for _, p := range props {
		if p == "fluxion.flux-framework.org/vendor=amazon" {
			n++
		}
	}
	if n != 1 {
		t.Errorf("vendor=amazon should appear once after dedup, appeared %d times", n)
	}
}

func TestRequireAnnotationConstrainsDevice(t *testing.T) {
	leader := podWith("leader", corev1.ResourceList{
		corev1.ResourceCPU:            qty(1),
		FluxionResourcePrefix + "qpu": qty(1),
	})
	if leader.Annotations == nil {
		leader.Annotations = map[string]string{}
	}
	leader.Annotations[RequireAnnotationPrefix+"qrmi_type"] = "braket-gate"
	leader.Annotations[RequireAnnotationPrefix+"vendor"] = "amazon"

	specs, err := JobspecsForGroup("qgrp", []corev1.Pod{leader},
		map[string]bool{"qpu": true})
	if err != nil {
		t.Fatal(err)
	}
	props := constraintProps(t, specs[1])
	for _, want := range []string{
		"fluxion.flux-framework.org/qrmi_type=braket-gate",
		"fluxion.flux-framework.org/vendor=amazon",
	} {
		if !hasProp(props, want) {
			t.Errorf("device match missing required constraint %q; got %v", want, props)
		}
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
