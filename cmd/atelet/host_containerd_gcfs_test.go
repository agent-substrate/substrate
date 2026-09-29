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

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protowire"
)

const sampleGKEContainerdConfig = `disabled_plugins = ['io.containerd.internal.v1.restart']
imports = ['/etc/containerd/conf.d/*.toml']
oom_score = -999
required_plugins = ['io.containerd.grpc.v1.cri']
version = 2

[plugins]
  [plugins.'io.containerd.grpc.v1.cri']
    sandbox_image = 'us-central1-artifactregistry.gcr.io/gke-release/gke-release/pause:3.8@sha256:880e63f94b145e46f1b1082bb71b85e21f16b99b180b9996407d61240ceb9830'

    [plugins.'io.containerd.grpc.v1.cri'.containerd]
      default_runtime_name = 'runc'
      disable_snapshot_annotations = false
      discard_unpacked_layers = true
      snapshotter = 'gcfs'

[proxy_plugins]
  [proxy_plugins.gcfs]
    address = '/run/containerd-gcfs-grpc/containerd-gcfs-grpc.sock'
    type = 'snapshot'
`

func TestDetachHostContainerdGCFSConfig(t *testing.T) {
	t.Run("detaches GKE single-quoted gcfs config and is idempotent", func(t *testing.T) {
		updated, changed := detachHostContainerdGCFSConfig(sampleGKEContainerdConfig)
		if !changed {
			t.Fatalf("expected changed=true for GKE gcfs config")
		}
		if !strings.Contains(updated, "snapshotter = 'overlayfs'") {
			t.Errorf("expected snapshotter = 'overlayfs' in updated config:\n%s", updated)
		}
		if strings.Contains(updated, "gcfs") {
			t.Errorf("expected no remaining gcfs references in updated config:\n%s", updated)
		}

		again, changedAgain := detachHostContainerdGCFSConfig(updated)
		if changedAgain {
			t.Errorf("expected second pass to be a no-op (changed=false)")
		}
		if again != updated {
			t.Errorf("expected second pass output to be identical")
		}
	})

	t.Run("detaches double-quoted gcfs config while preserving other proxy plugins", func(t *testing.T) {
		in := `[plugins."io.containerd.grpc.v1.cri".containerd]
  snapshotter = "gcfs"

[proxy_plugins]
  [proxy_plugins."gcfs"]
    address = "/run/containerd-gcfs-grpc/containerd-gcfs-grpc.sock"
    type = "snapshot"
  [proxy_plugins.soci]
    address = "/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock"
    type = "snapshot"
`
		updated, changed := detachHostContainerdGCFSConfig(in)
		if !changed {
			t.Fatalf("expected changed=true")
		}
		if !strings.Contains(updated, `snapshotter = "overlayfs"`) {
			t.Errorf("expected snapshotter = \"overlayfs\" in:\n%s", updated)
		}
		if strings.Contains(updated, "gcfs") {
			t.Errorf("expected gcfs section removed in:\n%s", updated)
		}
		if !strings.Contains(updated, "[proxy_plugins.soci]") || !strings.Contains(updated, "soci-snapshotter-grpc.sock") {
			t.Errorf("expected soci proxy_plugin preserved in:\n%s", updated)
		}
	})
}

func TestMaybeDetachHostContainerdGCFS_EndToEnd(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	cfgPath := filepath.Join(tmpDir, "config.toml")
	if err := os.WriteFile(cfgPath, []byte(sampleGKEContainerdConfig), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Start fake containerd gRPC server.
	containerdSock := filepath.Join(tmpDir, "containerd.sock")
	cLis, err := net.Listen("unix", containerdSock)
	if err != nil {
		t.Fatalf("Listen containerd: %v", err)
	}
	defer cLis.Close()

	var (
		mu            sync.Mutex
		deletedImages []string
		seenNamespace string
		versionProbes int
	)

	grpcSrv := grpc.NewServer(
		grpc.ForceServerCodec(hostContainerdRawCodec{}),
		grpc.UnknownServiceHandler(func(srv any, stream grpc.ServerStream) error {
			method, _ := grpc.MethodFromServerStream(stream)
			if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
				if ns := md.Get("containerd-namespace"); len(ns) > 0 {
					mu.Lock()
					seenNamespace = ns[0]
					mu.Unlock()
				}
			}
			var req []byte
			if err := stream.RecvMsg(&req); err != nil {
				return err
			}
			switch method {
			case containerdImagesListMethod:
				images := []string{
					"gke.gcr.io/pause:3.8@sha256:880e63f9",
					"us-central1-docker.pkg.dev/proj/repo/atelet@sha256:1111",
					"us-central1-docker.pkg.dev/proj/repo/ateom-gvisor@sha256:2222",
				}
				var resp []byte
				for _, img := range images {
					var msg []byte
					msg = protowire.AppendTag(msg, 1, protowire.BytesType)
					msg = protowire.AppendString(msg, img)
					resp = protowire.AppendTag(resp, 1, protowire.BytesType)
					resp = protowire.AppendBytes(resp, msg)
				}
				return stream.SendMsg(resp)
			case containerdImagesDeleteMethod:
				name := decodeFirstStringField(req)
				mu.Lock()
				deletedImages = append(deletedImages, name)
				mu.Unlock()
				return stream.SendMsg([]byte{})
			case containerdVersionMethod:
				mu.Lock()
				versionProbes++
				mu.Unlock()
				return stream.SendMsg([]byte{})
			default:
				return nil
			}
		}),
	)
	go grpcSrv.Serve(cLis)
	defer grpcSrv.Stop()

	// Start fake systemd private D-Bus socket server.
	systemdSock := filepath.Join(tmpDir, "systemd-private")
	sLis, err := net.Listen("unix", systemdSock)
	if err != nil {
		t.Fatalf("Listen systemd: %v", err)
	}
	defer sLis.Close()

	var (
		restartedUnit string
		restartedMode string
	)
	systemdDone := make(chan struct{})
	go func() {
		defer close(systemdDone)
		conn, err := sLis.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		br := bufio.NewReader(conn)
		authLine, err := br.ReadString('\n')
		if err != nil || !strings.HasSuffix(authLine, "AUTH EXTERNAL 30\r\n") {
			return
		}
		_, _ = io.WriteString(conn, "OK 0123456789abcdef0123456789abcdef\r\n")
		beginLine, err := br.ReadString('\n')
		if err != nil || beginLine != "BEGIN\r\n" {
			return
		}

		var hdr [16]byte
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			return
		}
		bodyLen := binary.LittleEndian.Uint32(hdr[4:8])
		fieldsLen := binary.LittleEndian.Uint32(hdr[12:16])
		paddedFields := int(fieldsLen)
		if rem := (16 + paddedFields) % 8; rem != 0 {
			paddedFields += 8 - rem
		}
		rest := make([]byte, paddedFields+int(bodyLen))
		if _, err := io.ReadFull(br, rest); err != nil {
			return
		}
		body := rest[paddedFields:]
		if len(body) >= 4 {
			uLen := binary.LittleEndian.Uint32(body[:4])
			restartedUnit = string(body[4 : 4+uLen])
			offset := 4 + int(uLen) + 1
			if rem := offset % 4; rem != 0 {
				offset += 4 - rem
			}
			if len(body) >= offset+4 {
				mLen := binary.LittleEndian.Uint32(body[offset : offset+4])
				restartedMode = string(body[offset+4 : offset+4+int(mLen)])
			}
		}

		// Emit a D-Bus SIGNAL frame (msgType = 4, e.g. JobNew) followed by METHOD_RETURN (msgType = 2).
		signalFrame := []byte{'l', 4, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0}
		_, _ = conn.Write(signalFrame)
		reply := []byte{'l', 2, 1, 1, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0}
		_, _ = conn.Write(reply)
	}()

	opts := hostContainerdDetachOptions{
		configPath:        cfgPath,
		containerdSocket:  containerdSock,
		systemdSocket:     systemdSock,
		readyPollInterval: 10 * time.Millisecond,
		readyTimeout:      2 * time.Second,
	}

	if err := maybeDetachHostContainerdGCFS(ctx, opts); err != nil {
		t.Fatalf("maybeDetachHostContainerdGCFS failed: %v", err)
	}
	<-systemdDone

	if restartedUnit != "containerd.service" || restartedMode != "replace" {
		t.Errorf("systemd RestartUnit got (%q, %q), want (\"containerd.service\", \"replace\")", restartedUnit, restartedMode)
	}

	mu.Lock()
	gotNS := seenNamespace
	gotDeleted := slices.Clone(deletedImages)
	gotProbes := versionProbes
	mu.Unlock()

	if gotNS != "k8s.io" {
		t.Errorf("containerd namespace = %q, want k8s.io", gotNS)
	}
	wantDeleted := []string{
		"us-central1-docker.pkg.dev/proj/repo/atelet@sha256:1111",
		"us-central1-docker.pkg.dev/proj/repo/ateom-gvisor@sha256:2222",
	}
	if !slices.Equal(gotDeleted, wantDeleted) {
		t.Errorf("deletedImages = %v, want %v (pause image must be preserved)", gotDeleted, wantDeleted)
	}
	if gotProbes < 1 {
		t.Errorf("expected at least 1 containerd readiness probe, got %d", gotProbes)
	}

	afterBytes, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("ReadFile after: %v", err)
	}
	if bytes.Contains(afterBytes, []byte("gcfs")) {
		t.Errorf("expected gcfs removed from %s, got:\n%s", cfgPath, string(afterBytes))
	}

	// Second call must be a no-op (does not dial systemdSocket again).
	if err := maybeDetachHostContainerdGCFS(ctx, opts); err != nil {
		t.Fatalf("second maybeDetachHostContainerdGCFS failed: %v", err)
	}
}
