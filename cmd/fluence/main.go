// Command fluence is a Kubernetes scheduler built from the standard
// kube-scheduler with the Fluence placement plugin registered out-of-tree. Run
// it as a second scheduler (schedulerName: fluence) alongside the default one.
package main

import (
	"os"

	"github.com/converged-computing/fluence/pkg/fluence"

	"k8s.io/component-base/cli"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"
)

func main() {
	command := app.NewSchedulerCommand(
		app.WithPlugin(fluence.Name, fluence.New),
	)
	code := cli.Run(command)
	os.Exit(code)
}
