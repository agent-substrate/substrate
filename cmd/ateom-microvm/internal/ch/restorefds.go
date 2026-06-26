// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ch

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// RestoredNet identifies one fd-backed network device in a snapshot and the
// fresh tap FDs to back it with on restore. kata boots CH virtio-net devices
// from tap FDs, so the snapshot's config requires net_fds on restore
// (RestoreMissingRequiredNetId otherwise); CH reopens the device on the FDs we
// pass over the api-socket via SCM_RIGHTS.
type RestoredNet struct {
	// ID is the device id from the snapshot's config.json (e.g. "_net1").
	ID string
	// FDs are open tap fds (one per queue pair) for CH to adopt.
	FDs []int
}

// LaunchVMMOptions configures starting a bare VMM (no VM) for an FD-passing
// restore.
type LaunchVMMOptions struct {
	// Binary is the cloud-hypervisor executable (defaults to "cloud-hypervisor").
	Binary string
	// APISocket is the api-socket path the new VMM should listen on.
	APISocket string
	// Stdout/Stderr receive the VMM's output.
	Stdout, Stderr interface{ Write([]byte) (int, error) }
}

// LaunchVMM starts a cloud-hypervisor process with only an api-socket (no VM)
// and waits until it answers. Use Client.RestoreWithNetFDs to then restore a
// snapshot that has fd-backed net devices. The caller owns cmd.
func LaunchVMM(ctx context.Context, o LaunchVMMOptions) (*exec.Cmd, *Client, error) {
	if o.APISocket == "" {
		return nil, nil, fmt.Errorf("LaunchVMMOptions.APISocket is required")
	}
	bin := o.Binary
	if bin == "" {
		bin = "cloud-hypervisor"
	}
	_ = os.Remove(o.APISocket)
	// Deliberately NOT exec.CommandContext: the VMM must outlive the RPC whose
	// ctx launched it. The caller owns cmd; WaitReady honors ctx.
	cmd := exec.Command(bin, "--api-socket", o.APISocket)
	cmd.Stdout = o.Stdout
	cmd.Stderr = o.Stderr
	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("while starting cloud-hypervisor: %w", err)
	}
	client := NewClient(o.APISocket)
	if err := client.WaitReady(ctx, 15*time.Second); err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return nil, nil, fmt.Errorf("while waiting for VMM api-socket: %w", err)
	}
	return cmd, client, nil
}

// RestoreWithNetFDs issues vm.restore for a snapshot dir, passing fresh tap FDs
// for the snapshot's fd-backed net devices via SCM_RIGHTS on the api-socket
// (the only way CH accepts net FDs on restore; mirrors ch-remote's
// send_with_fds). The VM comes back paused; call Resume after.
//
// memMode selects guest-RAM restore: "" / "Copy" = eager copy (CH default), or
// "OnDemand" = userfaultfd demand-paging. OnDemand keeps the (memfd-backed) guest
// memory SPARSE — it only faults in the pages the guest touches, instead of eager
// copy densifying the whole memfd — so a subsequent snapshot writes just the
// working set (fast) instead of full RAM. Confirmed on CH v52: the REST
// RestoreConfig accepts memory_restore_mode (enum Copy|OnDemand) alongside the
// SCM_RIGHTS net_fds, so ondemand + fd-backed net DO compose over REST (an earlier
// note claimed memory_restore_mode was CLI-only; that was a pre-v52 limitation).
// NOTE: with OnDemand, CH demand-pages from the snapshot's memory file for the
// VM's whole lifetime, so sourceDir must stay present until the actor is torn down.
func (c *Client) RestoreWithNetFDs(ctx context.Context, sourceDir string, nets []RestoredNet, memMode string) error {
	type restoredNetConfig struct {
		ID     string `json:"id"`
		NumFDs int    `json:"num_fds"`
	}
	cfg := struct {
		SourceURL         string              `json:"source_url"`
		MemoryRestoreMode string              `json:"memory_restore_mode,omitempty"`
		NetFDs            []restoredNetConfig `json:"net_fds,omitempty"`
	}{SourceURL: SnapshotURL(sourceDir), MemoryRestoreMode: memMode}
	var fds []int
	for _, n := range nets {
		cfg.NetFDs = append(cfg.NetFDs, restoredNetConfig{ID: n.ID, NumFDs: len(n.FDs)})
		fds = append(fds, n.FDs...)
	}
	body, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	raddr, err := net.ResolveUnixAddr("unix", c.apiSocket)
	if err != nil {
		return err
	}
	conn, err := net.DialUnix("unix", nil, raddr)
	if err != nil {
		return fmt.Errorf("dialing api-socket: %w", err)
	}
	defer conn.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	} else {
		_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
	}

	// Raw HTTP/1.1 over the unix socket: net/http cannot attach SCM_RIGHTS, and
	// CH's micro_http collects fds from the recvmsg ancillary data of the
	// request that carries them.
	req := fmt.Sprintf("PUT /api/v1/vm.restore HTTP/1.1\r\nHost: localhost\r\nAccept: */*\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
	var oob []byte
	if len(fds) > 0 {
		oob = unix.UnixRights(fds...)
	}
	if _, _, err := conn.WriteMsgUnix([]byte(req), oob, nil); err != nil {
		return fmt.Errorf("sending vm.restore with fds: %w", err)
	}

	status, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("reading vm.restore response: %w", err)
	}
	parts := strings.SplitN(strings.TrimSpace(status), " ", 3)
	if len(parts) < 2 || !strings.HasPrefix(parts[1], "2") {
		return fmt.Errorf("vm.restore failed: %s", strings.TrimSpace(status))
	}
	return nil
}

// AddNetWithFDs hotplugs a virtio-net device into a freshly-created (pre-boot or
// running) VM, passing the tap FDs via SCM_RIGHTS — the boot-path analog of
// RestoreWithNetFDs. kata adds net this way between vm.create and vm.boot
// (clh.go vmAddNetPut). mac may be empty (CH assigns one); numQueues should be
// 2*queuePairs (rx+tx) and len(fds) == queuePairs.
func (c *Client) AddNetWithFDs(ctx context.Context, mac string, numQueues int, fds []int) error {
	cfg := struct {
		Mac       string `json:"mac,omitempty"`
		NumQueues int    `json:"num_queues,omitempty"`
		NumFDs    int    `json:"num_fds,omitempty"`
	}{Mac: mac, NumQueues: numQueues, NumFDs: len(fds)}
	body, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	raddr, err := net.ResolveUnixAddr("unix", c.apiSocket)
	if err != nil {
		return err
	}
	conn, err := net.DialUnix("unix", nil, raddr)
	if err != nil {
		return fmt.Errorf("dialing api-socket: %w", err)
	}
	defer conn.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	} else {
		_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	}
	req := fmt.Sprintf("PUT /api/v1/vm.add-net HTTP/1.1\r\nHost: localhost\r\nAccept: */*\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
	var oob []byte
	if len(fds) > 0 {
		oob = unix.UnixRights(fds...)
	}
	if _, _, err := conn.WriteMsgUnix([]byte(req), oob, nil); err != nil {
		return fmt.Errorf("sending vm.add-net with fds: %w", err)
	}
	status, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("reading vm.add-net response: %w", err)
	}
	parts := strings.SplitN(strings.TrimSpace(status), " ", 3)
	if len(parts) < 2 || !strings.HasPrefix(parts[1], "2") {
		return fmt.Errorf("vm.add-net failed: %s", strings.TrimSpace(status))
	}
	return nil
}

// SnapshotNetDevice describes one net device found in a CH snapshot's
// config.json. Restore must supply net_fds for every one of them.
type SnapshotNetDevice struct {
	// ID is the CH device id (e.g. "_net1").
	ID string
	// QueuePairs is the number of tap FDs the device needs (num_queues/2).
	QueuePairs int
	// MAC is the guest-visible MAC address of the device.
	MAC string
}

// SnapshotNetDevices parses a CH snapshot's config.json and returns its net
// devices, in order.
func SnapshotNetDevices(snapshotDir string) ([]SnapshotNetDevice, error) {
	b, err := os.ReadFile(snapshotDir + "/config.json")
	if err != nil {
		return nil, err
	}
	var cfg struct {
		Net []struct {
			ID        string `json:"id"`
			NumQueues int    `json:"num_queues"`
			MAC       string `json:"mac"`
		} `json:"net"`
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("parsing snapshot config.json: %w", err)
	}
	var out []SnapshotNetDevice
	for _, n := range cfg.Net {
		qp := n.NumQueues / 2
		if qp < 1 {
			qp = 1
		}
		out = append(out, SnapshotNetDevice{ID: n.ID, QueuePairs: qp, MAC: n.MAC})
	}
	return out, nil
}
