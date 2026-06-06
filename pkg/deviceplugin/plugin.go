package deviceplugin

// Package deviceplugin implements a Kubernetes device plugin that advertises
// "quantum reachability" as a counted extended resource on every node.
//
// A quantum backend is not hardware attached to a node — it is a remote API any
// node can call (subject to the user's access). So the plugin advertises a large
// per-node ceiling of a single counted resource (quantum.flux-framework.org/qpu):
// this is what lets a pod write `resources.requests: {quantum.flux-framework.org/qpu: "1"}`
// and have the in-tree NodeResourcesFit plugin be satisfied (no wrapper needed).
//
// The count is a local admission gate only. Whether a backend is actually
// available and which one is matched is decided by Fluxion in the scheduler, and
// the real per-user limit lives on the IBM API — neither of which is node-local,
// which is exactly why the ceiling is large rather than a true quota.

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

// Plugin is a device-plugin server for a single counted resource.
type Plugin struct {
	pluginapi.UnimplementedDevicePluginServer

	resourceName string
	capacity     int
	socket       string

	server  *grpc.Server
	devices []*pluginapi.Device
	stop    chan struct{}
}

// New builds a plugin advertising `capacity` units of resourceName. The socket
// and device IDs are derived from the resource name so multiple plugins can run
// in one process without colliding.
func New(resourceName string, capacity int) *Plugin {
	tag := sanitize(resourceName)
	devs := make([]*pluginapi.Device, 0, capacity)
	for i := 0; i < capacity; i++ {
		devs = append(devs, &pluginapi.Device{
			ID:     fmt.Sprintf("%s-%d", tag, i),
			Health: pluginapi.Healthy,
		})
	}
	sock := filepath.Join(pluginapi.DevicePluginPath, tag+".sock")
	return &Plugin{
		resourceName: resourceName,
		capacity:     capacity,
		socket:       sock,
		devices:      devs,
		stop:         make(chan struct{}),
	}
}

// sanitize turns a resource name into a filesystem/identifier-safe tag, e.g.
// "fluxion.flux-framework.org/qpu" -> "fluxion-flux-framework-org-qpu".
func sanitize(name string) string {
	repl := func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return r
		}
		return '-'
	}
	return strings.Map(repl, name)
}

// Run serves the plugin and registers it with the kubelet, blocking until the
// context is cancelled.
func (p *Plugin) Run(ctx context.Context) error {
	if err := p.serve(); err != nil {
		return err
	}
	defer p.server.Stop()

	if err := p.register(ctx); err != nil {
		return fmt.Errorf("register with kubelet: %w", err)
	}

	<-ctx.Done()
	close(p.stop)
	return nil
}

func (p *Plugin) serve() error {
	if err := os.Remove(p.socket); err != nil && !os.IsNotExist(err) {
		return err
	}
	lis, err := net.Listen("unix", p.socket)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", p.socket, err)
	}
	p.server = grpc.NewServer()
	pluginapi.RegisterDevicePluginServer(p.server, p)
	go func() { _ = p.server.Serve(lis) }()
	return nil
}

func (p *Plugin) register(ctx context.Context) error {
	conn, err := grpc.NewClient(
		"unix://"+pluginapi.KubeletSocket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pluginapi.NewRegistrationClient(conn)
	_, err = client.Register(ctx, &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     filepath.Base(p.socket),
		ResourceName: p.resourceName,
	})
	return err
}

// GetDevicePluginOptions: no pre-start hook needed.
func (p *Plugin) GetDevicePluginOptions(context.Context, *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{PreStartRequired: false}, nil
}

// ListAndWatch streams the (static) device list to the kubelet.
func (p *Plugin) ListAndWatch(_ *pluginapi.Empty, stream pluginapi.DevicePlugin_ListAndWatchServer) error {
	if err := stream.Send(&pluginapi.ListAndWatchResponse{Devices: p.devices}); err != nil {
		return err
	}
	// Static list: keep the stream open, re-sending periodically until shutdown.
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			return nil
		case <-ticker.C:
			if err := stream.Send(&pluginapi.ListAndWatchResponse{Devices: p.devices}); err != nil {
				return err
			}
		}
	}
}

// Allocate is a no-op: a quantum "device" is just a reachability token, so no
// env vars, mounts, or device nodes are injected. The workload gets its backend
// from the scheduler and its credentials from a Secret.
func (p *Plugin) Allocate(_ context.Context, req *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	resp := &pluginapi.AllocateResponse{}
	for range req.ContainerRequests {
		resp.ContainerResponses = append(resp.ContainerResponses, &pluginapi.ContainerAllocateResponse{})
	}
	return resp, nil
}

// GetPreferredAllocation: no preference.
func (p *Plugin) GetPreferredAllocation(context.Context, *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
	return &pluginapi.PreferredAllocationResponse{}, nil
}

// PreStartContainer: nothing to do.
func (p *Plugin) PreStartContainer(context.Context, *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return &pluginapi.PreStartContainerResponse{}, nil
}
