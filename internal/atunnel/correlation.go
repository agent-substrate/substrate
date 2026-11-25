//go:build linux

// Copyright 2026 The Agent Substrate Authors
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

package atunnel

import (
	"context"
	"log/slog"

	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/atunnel/fakeip"
	"go.ate.dev/substrate/internal/resources"
)

// NewFakeIPs returns the fake address table for an actor that starts without
// a snapshot. It returns nil unless the DnsCorrelationInWorker gate is on.
func (t *Tunnel) NewFakeIPs(actor resources.ActorAttribution) *fakeip.Table {
	if !t.dnsCorrelation {
		return nil
	}
	return fakeip.New(fakeIPsLogger(actor))
}

// LoadFakeIPs returns the fake address table for an actor that is restored
// from the snapshot in dir, so the addresses the actor kept still lead to the
// same names.
//
// A worker without the DnsCorrelationInWorker gate loads the table too. It does
// not serve it, but saves it with the actor's next checkpoint, so the actor
// still has it on a later worker that does.
func (t *Tunnel) LoadFakeIPs(ctx context.Context, actor resources.ActorAttribution, dir string) *fakeip.Table {
	table, err := fakeip.Load(dir, fakeIPsLogger(actor))
	if err != nil {
		// Not a reason to fail the restore. A new table could give an address the
		// actor kept to another name, so the actor gets one with none left.
		slog.LogAttrs(ctx, slog.LevelError, "Could not load the actor's fake addresses; giving it no new ones",
			append(ateattr.ActorLogAttrs(actor), slog.Any("err", err))...)
		return fakeip.NewUsedUp(fakeIPsLogger(actor))
	}
	if table == nil {
		return t.NewFakeIPs(actor)
	}
	return table
}

// fakeIPsLogger returns the logger for actor's fake address table.
func fakeIPsLogger(actor resources.ActorAttribution) *slog.Logger {
	return slog.New(slog.Default().Handler().WithAttrs(ateattr.ActorLogAttrs(actor)))
}

// ServedFakeIPs returns table if the DnsCorrelationInWorker gate is on, and
// nil otherwise: the actor's DNS then answers with real addresses, and its
// connections are tunneled by address.
func (t *Tunnel) ServedFakeIPs(table *fakeip.Table) *fakeip.Table {
	if !t.dnsCorrelation {
		return nil
	}
	return table
}

// SaveFakeIPs writes table into the actor's checkpoint in dir and returns the
// name of the snapshot file, or "" if there is nothing to save.
func SaveFakeIPs(ctx context.Context, actor resources.ActorAttribution, table *fakeip.Table, dir string) string {
	file, err := table.Save(dir)
	if err != nil {
		// Not a reason to fail the checkpoint, see LoadFakeIPs.
		slog.LogAttrs(ctx, slog.LevelError, "Could not save the actor's fake addresses",
			append(ateattr.ActorLogAttrs(actor), slog.Any("err", err))...)
		return ""
	}
	return file
}
