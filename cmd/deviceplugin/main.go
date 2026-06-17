// Command fluence-deviceplugin advertises exotic Fluxion resource types as
// counted extended resources on the node it runs on (one per type, e.g.
// fluxion.flux-framework.org/qpu and .../qubit). Deploy it as a DaemonSet
// alongside the fluence scheduler.
//
// The set of types is derived from the SAME resources config the scheduler uses
// to build its graph (FLUENCE_RESOURCES), so the advertised resources and the
// graph's resource types come from one source and cannot drift. If
// FLUENCE_RESOURCES is unset or the file is absent, nothing is advertised — the
// node stays classical-only.
//
// A quantum backend is a remote API reachable from any node, not a node-local
// device, so each type is advertised at a large per-node ceiling. That count is
// only a local admission gate (so NodeResourcesFit is satisfied); the real gates
// are Fluxion (which backend, is one available) and the user's API limit.
//
//	FLUENCE_RESOURCES          path to the shared resources config
//	                           (default /etc/fluence/resources.yaml)
//	FLUENCE_RESOURCE_CAPACITY  per-node ceiling for each type (default 1000)
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/converged-computing/fluence/pkg/cluster"
	"github.com/converged-computing/fluence/pkg/deviceplugin"
)

func main() {
	cfgPath := os.Getenv("FLUENCE_RESOURCES")
	if cfgPath == "" {
		cfgPath = "/etc/fluence/resources.yaml"
	}

	var names []string
	if data, err := os.ReadFile(cfgPath); err == nil {
		rc, perr := cluster.LoadResourcesConfig(data)
		if perr != nil {
			log.Fatalf("parse resources config %s: %v", cfgPath, perr)
		}
		names = cluster.FluxionResourceNames(rc.Resources)
		log.Printf("derived %d resource(s) from %s: %v", len(names), cfgPath, names)
	} else {
		log.Printf("no resources config at %s (%v); advertising nothing", cfgPath, err)
	}

	capacity := 1000
	if v := os.Getenv("FLUENCE_RESOURCE_CAPACITY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			capacity = n
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if len(names) == 0 {
		log.Print("no exotic resources to advertise; idling")
		<-ctx.Done()
		return
	}

	var wg sync.WaitGroup
	for _, name := range names {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			p := deviceplugin.New(name, capacity)
			log.Printf("advertising %s capacity=%d", name, capacity)
			if err := p.Run(ctx); err != nil {
				log.Printf("device plugin %s: %v", name, err)
				stop() // bring the process down so the DaemonSet restarts it
			}
		}(name)
	}
	wg.Wait()
}
