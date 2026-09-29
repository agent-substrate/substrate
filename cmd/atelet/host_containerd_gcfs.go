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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	defaultHostContainerdConfigPath = "/host/etc/containerd/config.toml"
	defaultHostContainerdSocketPath = "/host/run/containerd/containerd.sock"
	defaultHostSystemdSocketPath    = "/host/run/systemd/private"

	containerdK8sNamespace       = "k8s.io"
	containerdImagesListMethod   = "/containerd.services.images.v1.Images/List"
	containerdImagesDeleteMethod = "/containerd.services.images.v1.Images/Delete"
	containerdVersionMethod      = "/containerd.services.version.v1.Version/Version"
)

type hostContainerdDetachOptions struct {
	configPath        string
	containerdSocket  string
	systemdSocket     string
	readyPollInterval time.Duration
	readyTimeout      time.Duration
}

func defaultHostContainerdDetachOptions() hostContainerdDetachOptions {
	return hostContainerdDetachOptions{
		configPath:        defaultHostContainerdConfigPath,
		containerdSocket:  defaultHostContainerdSocketPath,
		systemdSocket:     defaultHostSystemdSocketPath,
		readyPollInterval: 200 * time.Millisecond,
		readyTimeout:      15 * time.Second,
	}
}

// maybeDetachHostContainerdGCFS detaches the gcfs proxy plugin from the host
// containerd configuration and switches host containerd's CRI snapshotter to
// overlayfs if /host/etc/containerd/config.toml still references gcfs.
//
// On GKE nodes booted with Image Streaming (GCFS), host containerd registers
// [proxy_plugins.gcfs] pointing to /run/containerd-gcfs-grpc/containerd-gcfs-grpc.sock.
// Because containerd-gcfs-grpc's BoltDB is not namespace-partitioned, host
// containerd's snapshot garbage collector walks containerd-gcfs-grpc.sock and
// deletes Riptide snapshots prepared directly by atelet. Detaching gcfs from
// host containerd on dedicated Substrate worker nodes gives atelet exclusive
// ownership of containerd-gcfs-grpc and gcfsd while regular Kubernetes Pods on
// the node use overlayfs.
func maybeDetachHostContainerdGCFS(ctx context.Context, opts hostContainerdDetachOptions) error {
	raw, err := os.ReadFile(opts.configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("reading host containerd config %s: %w", opts.configPath, err)
	}

	updated, changed := detachHostContainerdGCFSConfig(string(raw))
	if !changed {
		slog.DebugContext(ctx, "Host containerd config already detached from gcfs",
			slog.String("config", opts.configPath))
		return nil
	}

	t0 := time.Now()
	slog.InfoContext(ctx, "Detaching host containerd from gcfs snapshotter for exclusive atelet Riptide ownership",
		slog.String("config", opts.configPath))

	// Any non-pause image pulled by host containerd while snapshotter='gcfs' was
	// active has an image record in containerd's k8s.io store without local layer
	// blobs in containerd's content store. Deleting those metadata records before
	// switching to overlayfs ensures kubelet re-pulls and unpacks into overlayfs
	// when starting new pods.
	purged, err := purgeHostContainerdNonPauseImages(ctx, opts.containerdSocket)
	if err != nil {
		slog.WarnContext(ctx, "Failed to purge gcfs-backed image records from host containerd before switch",
			slog.String("socket", opts.containerdSocket),
			slog.Any("err", err))
	}

	if err := writeFileAtomically(opts.configPath, []byte(updated), 0o644); err != nil {
		return fmt.Errorf("writing updated host containerd config %s: %w", opts.configPath, err)
	}

	if err := restartHostSystemdUnit(ctx, opts.systemdSocket, "containerd.service"); err != nil {
		return fmt.Errorf("restarting host containerd.service via %s: %w", opts.systemdSocket, err)
	}

	if err := waitForHostContainerdReady(ctx, opts.containerdSocket, opts.readyPollInterval, opts.readyTimeout); err != nil {
		return fmt.Errorf("waiting for host containerd to become ready after restart: %w", err)
	}

	slog.InfoContext(ctx, "Host containerd restarted with overlayfs; gcfs detached",
		slog.String("config", opts.configPath),
		slog.Int("purged_images", purged),
		slog.Int64("elapsed_ms", time.Since(t0).Milliseconds()))
	return nil
}

// detachHostContainerdGCFSConfig rewrites a containerd config.toml string so
// that snapshotter = 'gcfs' (or "gcfs") becomes 'overlayfs' (or "overlayfs")
// and any [proxy_plugins.gcfs] section is removed. It returns the updated
// config and true if any change was made.
func detachHostContainerdGCFSConfig(content string) (string, bool) {
	lines := strings.Split(content, "\n")
	out := make([]string, 0, len(lines))
	changed := false
	inGCFSProxySection := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			if isGCFSProxyPluginHeader(trimmed) {
				inGCFSProxySection = true
				changed = true
				continue
			}
			inGCFSProxySection = false
		}
		if inGCFSProxySection {
			continue
		}

		if replaced, ok := replaceGCFSSnapshotterLine(line); ok {
			out = append(out, replaced)
			changed = true
			continue
		}
		out = append(out, line)
	}

	return strings.Join(out, "\n"), changed
}

func isGCFSProxyPluginHeader(trimmedHeader string) bool {
	switch trimmedHeader {
	case "[proxy_plugins.gcfs]", `[proxy_plugins."gcfs"]`, "[proxy_plugins.'gcfs']":
		return true
	default:
		return false
	}
}

func replaceGCFSSnapshotterLine(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if strings.HasPrefix(trimmed, "#") {
		return line, false
	}
	eqIdx := strings.IndexByte(trimmed, '=')
	if eqIdx < 0 {
		return line, false
	}
	key := strings.TrimSpace(trimmed[:eqIdx])
	if key != "snapshotter" {
		return line, false
	}
	val := strings.TrimSpace(trimmed[eqIdx+1:])
	switch val {
	case "'gcfs'":
		return strings.Replace(line, "'gcfs'", "'overlayfs'", 1), true
	case `"gcfs"`:
		return strings.Replace(line, `"gcfs"`, `"overlayfs"`, 1), true
	default:
		return line, false
	}
}

func writeFileAtomically(path string, data []byte, perm os.FileMode) error {
	if fi, err := os.Stat(path); err == nil {
		perm = fi.Mode().Perm()
	}
	tmpPath := filepath.Clean(path) + ".atelet.tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, path)
}

type hostContainerdRawCodec struct{}

func (hostContainerdRawCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("hostContainerdRawCodec: expected []byte, got %T", v)
	}
	return b, nil
}

func (hostContainerdRawCodec) Unmarshal(data []byte, v any) error {
	p, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("hostContainerdRawCodec: expected *[]byte, got %T", v)
	}
	*p = append((*p)[:0], data...)
	return nil
}

func (hostContainerdRawCodec) Name() string { return "atelet-host-containerd-raw" }

func init() {
	encoding.RegisterCodec(hostContainerdRawCodec{})
}

func purgeHostContainerdNonPauseImages(ctx context.Context, socketPath string) (int, error) {
	if err := checkUnixSocket(socketPath); err != nil {
		return 0, err
	}
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	callCtx = metadata.AppendToOutgoingContext(callCtx, "containerd-namespace", containerdK8sNamespace)

	conn, err := grpc.NewClient(
		"unix://"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype(hostContainerdRawCodec{}.Name())),
	)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	var listResp []byte
	if err := conn.Invoke(callCtx, containerdImagesListMethod, []byte{}, &listResp); err != nil {
		return 0, fmt.Errorf("listing containerd images: %w", err)
	}

	names := decodeContainerdImageListNames(listResp)
	purged := 0
	for _, name := range names {
		if isPauseSandboxImage(name) {
			continue
		}
		req := encodeContainerdDeleteImageRequest(name)
		var delResp []byte
		if err := conn.Invoke(callCtx, containerdImagesDeleteMethod, req, &delResp); err != nil {
			slog.DebugContext(ctx, "Ignoring error deleting host containerd image record",
				slog.String("image", name),
				slog.Any("err", err))
			continue
		}
		purged++
	}
	return purged, nil
}

func isPauseSandboxImage(name string) bool {
	return strings.Contains(name, "/pause:") || strings.Contains(name, "/pause@")
}

func decodeContainerdImageListNames(b []byte) []string {
	var names []string
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return names
		}
		b = b[n:]
		if num == 1 && typ == protowire.BytesType {
			msg, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return names
			}
			b = b[m:]
			if name := decodeFirstStringField(msg); name != "" {
				names = append(names, name)
			}
			continue
		}
		m := protowire.ConsumeFieldValue(num, typ, b)
		if m < 0 {
			return names
		}
		b = b[m:]
	}
	return names
}

func decodeFirstStringField(b []byte) string {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return ""
		}
		b = b[n:]
		if num == 1 && typ == protowire.BytesType {
			s, m := protowire.ConsumeString(b)
			if m < 0 {
				return ""
			}
			return s
		}
		m := protowire.ConsumeFieldValue(num, typ, b)
		if m < 0 {
			return ""
		}
		b = b[m:]
	}
	return ""
}

func encodeContainerdDeleteImageRequest(name string) []byte {
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendString(b, name)
	b = protowire.AppendTag(b, 2, protowire.VarintType)
	b = protowire.AppendVarint(b, 1) // sync = true
	return b
}

func waitForHostContainerdReady(ctx context.Context, socketPath string, interval, timeout time.Duration) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := probeHostContainerd(deadlineCtx, socketPath); err == nil {
			return nil
		}
		select {
		case <-deadlineCtx.Done():
			return deadlineCtx.Err()
		case <-ticker.C:
		}
	}
}

func probeHostContainerd(ctx context.Context, socketPath string) error {
	callCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(
		"unix://"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype(hostContainerdRawCodec{}.Name())),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	var resp []byte
	return conn.Invoke(callCtx, containerdVersionMethod, []byte{}, &resp)
}

// restartHostSystemdUnit connects to systemd's private D-Bus socket
// (/run/systemd/private) as root (UID 0 via SO_PEERCRED) and invokes
// org.freedesktop.systemd1.Manager.RestartUnit(unit, "replace").
func restartHostSystemdUnit(ctx context.Context, systemdSocketPath, unit string) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", systemdSocketPath)
	if err != nil {
		return fmt.Errorf("dialing systemd socket: %w", err)
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	}

	// D-Bus EXTERNAL SASL authentication for UID 0 (hex "30").
	if _, err := io.WriteString(conn, "\x00AUTH EXTERNAL 30\r\n"); err != nil {
		return fmt.Errorf("writing D-Bus AUTH EXTERNAL: %w", err)
	}
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		return fmt.Errorf("reading D-Bus AUTH response: %w", err)
	}
	if !strings.HasPrefix(line, "OK ") {
		return fmt.Errorf("unexpected D-Bus AUTH response: %q", strings.TrimSpace(line))
	}
	if _, err := io.WriteString(conn, "BEGIN\r\n"); err != nil {
		return fmt.Errorf("writing D-Bus BEGIN: %w", err)
	}

	msg := buildDBusRestartUnitCall(unit, "replace")
	if _, err := conn.Write(msg); err != nil {
		return fmt.Errorf("writing D-Bus RestartUnit call: %w", err)
	}

	for {
		var hdr [16]byte
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			return fmt.Errorf("reading D-Bus reply header: %w", err)
		}
		msgType := hdr[1]
		bodyLen := binary.LittleEndian.Uint32(hdr[4:8])
		fieldsLen := binary.LittleEndian.Uint32(hdr[12:16])
		paddedFields := int(fieldsLen)
		if rem := (16 + paddedFields) % 8; rem != 0 {
			paddedFields += 8 - rem
		}
		if _, err := io.CopyN(io.Discard, br, int64(paddedFields)+int64(bodyLen)); err != nil {
			return fmt.Errorf("reading D-Bus reply body: %w", err)
		}
		// systemd emits asynchronous SIGNAL frames (msgType=4, e.g. JobNew) before
		// the synchronous METHOD_RETURN (msgType=2).
		if msgType == 4 {
			continue
		}
		if msgType != 2 {
			return fmt.Errorf("D-Bus RestartUnit returned message type %d (expected METHOD_RETURN=2)", msgType)
		}
		return nil
	}
}

func buildDBusRestartUnitCall(unit, mode string) []byte {
	var body []byte
	body = appendDBusString(body, unit)
	body = padTo(body, 4)
	body = appendDBusString(body, mode)

	var fields []byte
	fields = appendDBusHeaderFieldString(fields, 1, 'o', "/org/freedesktop/systemd1")
	fields = appendDBusHeaderFieldString(fields, 2, 's', "org.freedesktop.systemd1.Manager")
	fields = appendDBusHeaderFieldString(fields, 3, 's', "RestartUnit")
	fields = appendDBusHeaderFieldString(fields, 6, 's', "org.freedesktop.systemd1")
	fields = appendDBusHeaderFieldSignature(fields, 8, "ss")

	var pkt []byte
	pkt = append(pkt, 'l', 1, 0, 1) // little-endian, METHOD_CALL, flags=0, version=1
	pkt = binary.LittleEndian.AppendUint32(pkt, uint32(len(body)))
	pkt = binary.LittleEndian.AppendUint32(pkt, 1) // serial = 1
	pkt = binary.LittleEndian.AppendUint32(pkt, uint32(len(fields)))
	pkt = append(pkt, fields...)
	pkt = padTo(pkt, 8)
	pkt = append(pkt, body...)
	return pkt
}

func appendDBusHeaderFieldString(b []byte, code byte, sig byte, val string) []byte {
	b = padTo(b, 8)
	b = append(b, code, 1, sig, 0) // struct(byte, variant_sig): 4 bytes total, already 4-byte aligned
	b = appendDBusString(b, val)
	return b
}

func appendDBusHeaderFieldSignature(b []byte, code byte, sig string) []byte {
	b = padTo(b, 8)
	b = append(b, code, 1, 'g', 0)
	b = append(b, byte(len(sig)))
	b = append(b, sig...)
	b = append(b, 0)
	return b
}

func appendDBusString(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint32(b, uint32(len(s)))
	b = append(b, s...)
	b = append(b, 0)
	return b
}

func padTo(b []byte, align int) []byte {
	rem := len(b) % align
	if rem == 0 {
		return b
	}
	return append(b, make([]byte, align-rem)...)
}
