package cluster

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// isControlPlane detects control-plane nodes by taint, independent of node name
// or type, and only for the well-known control-plane/master taint keys.
func TestIsControlPlane(t *testing.T) {
	cp := &corev1.Node{}
	cp.Spec.Taints = []corev1.Taint{
		{Key: "node-role.kubernetes.io/control-plane", Effect: corev1.TaintEffectNoSchedule},
	}
	if !isControlPlane(cp) {
		t.Error("expected control-plane node (control-plane taint) to be detected")
	}

	master := &corev1.Node{}
	master.Spec.Taints = []corev1.Taint{
		{Key: "node-role.kubernetes.io/master", Effect: corev1.TaintEffectNoSchedule},
	}
	if !isControlPlane(master) {
		t.Error("expected control-plane node (master taint) to be detected")
	}

	worker := &corev1.Node{}
	worker.Spec.Taints = []corev1.Taint{
		{Key: "some/other-taint", Effect: corev1.TaintEffectNoSchedule},
	}
	if isControlPlane(worker) {
		t.Error("worker with an unrelated taint should not be treated as control-plane")
	}

	plain := &corev1.Node{}
	if isControlPlane(plain) {
		t.Error("node with no taints should not be control-plane")
	}
}
