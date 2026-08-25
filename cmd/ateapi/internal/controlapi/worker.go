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

package controlapi

import (
	"context"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *Service) ListWorkers(ctx context.Context, req *ateapipb.ListWorkersRequest) (*ateapipb.ListWorkersResponse, error) {
	if errs := validateListWorkersRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	page, err := s.persistence.ListWorkers(ctx, store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, fmt.Errorf("while listing workers in db: %w", err)
	}
	return &ateapipb.ListWorkersResponse{
		Workers:       page.Items,
		NextPageToken: page.NextPageToken,
	}, nil
}

func validateListWorkersRequest(req *ateapipb.ListWorkersRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.PageSize, fldPath.Child("page_size"); val < 0 {
		errs = append(errs, field.Invalid(fldPath, val, "must be greater than or equal to 0"))
	}

	return errs
}

func (s *Service) GetWorker(ctx context.Context, req *ateapipb.GetWorkerRequest) (*ateapipb.Worker, error) {
	return nil, status.Error(codes.Unimplemented, "GetWorker is not implemented yet")
}

func (s *Service) CreateWorker(ctx context.Context, req *ateapipb.CreateWorkerRequest) (*ateapipb.Worker, error) {
	return nil, status.Error(codes.Unimplemented, "CreateWorker is not implemented yet")
}

func (s *Service) UpdateWorker(ctx context.Context, req *ateapipb.UpdateWorkerRequest) (*ateapipb.Worker, error) {
	return nil, status.Error(codes.Unimplemented, "UpdateWorker is not implemented yet")
}

func (s *Service) DeleteWorker(ctx context.Context, req *ateapipb.DeleteWorkerRequest) (*ateapipb.Worker, error) {
	return nil, status.Error(codes.Unimplemented, "DeleteWorker is not implemented yet")
}

func (s *Service) DrainWorker(ctx context.Context, req *ateapipb.DrainWorkerRequest) (*ateapipb.Worker, error) {
	return nil, status.Error(codes.Unimplemented, "DrainWorker is not implemented yet")
}
