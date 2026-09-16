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

//go:build linux

package sandboxd

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
)

const DefaultDANConfigDir = "/run/kata-containers/dans"

type HostTapDANConfig struct {
	NetNSPath   string
	TapName     string
	GuestMAC    string
	IPAddresses []string
	MTU         uint64
	Routes      []DANRoute
}

type DANRoute struct {
	Destination string `json:"dest"`
	Gateway     string `json:"gateway,omitempty"`
	Source      string `json:"source,omitempty"`
	Scope       uint32 `json:"scope,omitempty"`
	Flags       uint32 `json:"flags,omitempty"`
	MTU         uint32 `json:"mtu,omitempty"`
}

type danConfig struct {
	NetNS   string      `json:"netns"`
	Devices []danDevice `json:"devices"`
}

type danDevice struct {
	Name        string         `json:"name"`
	GuestMAC    string         `json:"guest_mac"`
	Device      danHostTap     `json:"device"`
	NetworkInfo danNetworkInfo `json:"network_info"`
}

type danHostTap struct {
	Type      string `json:"type"`
	TapName   string `json:"tap_name"`
	QueueNum  uint32 `json:"queue_num,omitempty"`
	QueueSize uint32 `json:"queue_size,omitempty"`
}

type danNetworkInfo struct {
	Interface danInterface `json:"interface"`
	Routes    []DANRoute   `json:"routes"`
	Neighbors []any        `json:"neighbors"`
}

type danInterface struct {
	IPAddresses []string `json:"ip_addresses"`
	MTU         uint64   `json:"mtu"`
	Type        string   `json:"ntype"`
	Flags       uint32   `json:"flags"`
}

// WriteHostTapDANConfig atomically publishes the single-NIC host-tap schema
// consumed by runtime-rs from <dan_conf>/<sandbox-id>.json.
func WriteHostTapDANConfig(dir, sandboxID string, cfg HostTapDANConfig) (string, error) {
	if !filepath.IsAbs(dir) || !validID(sandboxID) {
		return "", errors.New("absolute DAN directory and valid sandbox id are required")
	}
	if err := validateHostTapDANConfig(cfg); err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create DAN directory: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() {
		return "", errors.New("DAN directory must be a directory, not a symlink")
	}

	document := danConfig{
		NetNS: cfg.NetNSPath,
		Devices: []danDevice{{
			Name:     "eth0",
			GuestMAC: cfg.GuestMAC,
			Device:   danHostTap{Type: "host-tap", TapName: cfg.TapName},
			NetworkInfo: danNetworkInfo{
				Interface: danInterface{IPAddresses: append([]string(nil), cfg.IPAddresses...), MTU: cfg.MTU, Type: "tuntap"},
				Routes:    append([]DANRoute(nil), cfg.Routes...), Neighbors: []any{},
			},
		}},
	}
	data, err := json.Marshal(document)
	if err != nil {
		return "", err
	}
	data = append(data, '\n')

	tmp, err := os.CreateTemp(dir, "."+sandboxID+"-*.tmp")
	if err != nil {
		return "", fmt.Errorf("create temporary DAN config: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	target := filepath.Join(dir, sandboxID+".json")
	if err := os.Rename(tmpPath, target); err != nil {
		return "", fmt.Errorf("publish DAN config: %w", err)
	}
	return target, nil
}

func RemoveDANConfig(dir, sandboxID string) error {
	if !filepath.IsAbs(dir) || !validID(sandboxID) {
		return errors.New("absolute DAN directory and valid sandbox id are required")
	}
	err := os.Remove(filepath.Join(dir, sandboxID+".json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func validateHostTapDANConfig(cfg HostTapDANConfig) error {
	if !filepath.IsAbs(cfg.NetNSPath) || !kataTapName.MatchString(cfg.TapName) {
		return errors.New("absolute netns path and Kata-owned tap name are required")
	}
	if _, err := net.ParseMAC(cfg.GuestMAC); err != nil {
		return fmt.Errorf("invalid guest MAC: %w", err)
	}
	if len(cfg.IPAddresses) == 0 || cfg.MTU == 0 {
		return errors.New("guest IP addresses and MTU are required")
	}
	for _, address := range cfg.IPAddresses {
		if _, _, err := net.ParseCIDR(address); err != nil {
			return fmt.Errorf("invalid guest address %q: %w", address, err)
		}
	}
	for _, route := range cfg.Routes {
		if route.Destination != "" {
			if _, _, err := net.ParseCIDR(route.Destination); err != nil {
				return fmt.Errorf("invalid route destination %q: %w", route.Destination, err)
			}
		}
		for name, value := range map[string]string{"gateway": route.Gateway, "source": route.Source} {
			if value != "" && net.ParseIP(value) == nil {
				return fmt.Errorf("invalid route %s %q", name, value)
			}
		}
	}
	return nil
}
