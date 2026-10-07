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
	"context"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type registrationClient struct {
	ateapipb.ControlClient
	createErr error
	deleted   bool
}

func (c *registrationClient) CreateWorker(context.Context, *ateapipb.CreateWorkerRequest, ...grpc.CallOption) (*ateapipb.Worker, error) {
	return &ateapipb.Worker{}, c.createErr
}

func (c *registrationClient) DeleteWorker(context.Context, *ateapipb.DeleteWorkerRequest, ...grpc.CallOption) (*ateapipb.Worker, error) {
	c.deleted = true
	return &ateapipb.Worker{}, nil
}

func TestSmokeFailsClosedOnMissingCapacity(t *testing.T) {
	c := &registrationClient{}
	err := exercise(t.Context(), c, "mac.example:9443", "fixture")
	if err == nil || !strings.Contains(err.Error(), "registered capacity") || !c.deleted {
		t.Fatalf("missing capacity must fail and deregister owned Worker: err=%v deleted=%v", err, c.deleted)
	}
}

func TestSmokeDoesNotDeleteUnownedWorker(t *testing.T) {
	c := &registrationClient{createErr: status.Error(codes.AlreadyExists, "already exists")}
	if err := exercise(t.Context(), c, "mac.example:9443", "fixture"); status.Code(err) != codes.AlreadyExists || c.deleted {
		t.Fatalf("failed registration must not delete a Worker: err=%v deleted=%v", err, c.deleted)
	}
}
