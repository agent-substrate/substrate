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
	"errors"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *Service) ListWorkers(ctx context.Context, req *ateapipb.ListWorkersRequest) (*ateapipb.ListWorkersResponse, error) {
	if errs := validateListWorkersRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	workers, nextToken, err := s.persistence.ListWorkers(ctx, effectivePageSize(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, fmt.Errorf("while listing workers in db: %w", err)
	}
	return &ateapipb.ListWorkersResponse{
		Workers:       workers,
		NextPageToken: nextToken,
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

func (s *Service) GetWorker(_ context.Context, req *ateapipb.GetWorkerRequest) (*ateapipb.Worker, error) {
	if errs := validateGetWorkerRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	worker, err := s.workerCache.Worker(req.GetWorkerNamespace(), req.GetWorkerPod())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "worker not found")
	}
	if err != nil {
		return nil, fmt.Errorf("get worker from cache: %w", err)
	}
	return worker, nil
}

func validateGetWorkerRequest(req *ateapipb.GetWorkerRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList
	if val, path := req.GetWorkerNamespace(), fldPath.Child("worker_namespace"); val == "" {
		errs = append(errs, field.Required(path, ""))
	} else {
		for _, msg := range content.IsDNS1123Label(val) {
			errs = append(errs, field.Invalid(path, val, msg))
		}
	}
	if val, path := req.GetWorkerPod(), fldPath.Child("worker_pod"); val == "" {
		errs = append(errs, field.Required(path, ""))
	} else {
		for _, msg := range content.IsDNS1123Subdomain(val) {
			errs = append(errs, field.Invalid(path, val, msg))
		}
	}
	return errs
}
