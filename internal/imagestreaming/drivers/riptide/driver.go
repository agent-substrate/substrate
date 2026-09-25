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

// Package riptide implements an ImageStreamer driver for Google Cloud Riptide
// via the remote snapshotter gRPC service (/run/containerd-gcfs-grpc).
package riptide

import (
	"context"

	"github.com/agent-substrate/substrate/internal/imagestreaming"
	"github.com/agent-substrate/substrate/internal/imagestreaming/drivers/remotesnapshotter"
)

const (
	// ProviderName identifies this streaming provider.
	ProviderName = remotesnapshotter.ProviderRiptide

	// DefaultGCFSSocket is the default UNIX socket path for containerd-gcfs-grpc.
	DefaultGCFSSocket = remotesnapshotter.DefaultRiptideSocket

	// DefaultWorkDir is the default base path where streamed layer mounts are wrapped.
	DefaultWorkDir = "/run/ate/streaming/riptide"
)

func init() {
	imagestreaming.Register(ProviderName, func(ctx context.Context, cfg imagestreaming.Config) (imagestreaming.ImageStreamer, error) {
		return remotesnapshotter.NewFromConfig(ctx, ProviderName, cfg)
	})
}
