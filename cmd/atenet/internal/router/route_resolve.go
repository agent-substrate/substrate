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

package router

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
)

// ActorRouteResolver implements RouteResolver using the actor control plane.
// It parses the actor ID from the request authority, resumes the actor via the
// control plane, and resolves the worker pod backend.
type ActorRouteResolver struct {
	resumer *ActorResumer
}

// NewActorRouteResolver constructs an ActorRouteResolver backed by the given resumer.
func NewActorRouteResolver(resumer *ActorResumer) *ActorRouteResolver {
	return &ActorRouteResolver{resumer: resumer}
}

// ResolveRoute implements RouteResolver. It is safe for concurrent use.
func (r *ActorRouteResolver) ResolveRoute(ctx context.Context, req RouteRequest) RouteResolution {
	atespace, actorID, err := parseActorRef(req.Authority)
	if err != nil {
		return RouteResolution{Denial: invalidHostDenial(req.Authority, err)}
	}

	slog.InfoContext(ctx, "ResumeActor", slog.String("atespace", atespace), slog.String("actorID", actorID))
	actor, err := r.resumer.ResumeActor(ctx, atespace, actorID)
	if err != nil {
		return RouteResolution{Denial: mapResumeDenial(actorID, err)}
	}

	tmplNs := actor.GetActorTemplateNamespace()
	tmplName := actor.GetActorTemplateName()
	workerIP := actor.GetAteomPodIp()

	slog.InfoContext(ctx, "ResumeActor result",
		slog.String("actorID", actorID),
		slog.String("status", actor.GetStatus().String()),
		slog.String("workerIP", workerIP))

	if net.ParseIP(workerIP) == nil {
		return RouteResolution{Denial: &RouteDenial{
			HTTPStatus:  http.StatusInternalServerError,
			Message:     fmt.Sprintf("actor %q routing failed", actorID),
			OutcomeCode: "error",
		}}
	}

	return RouteResolution{Success: &RouteSuccess{
		ActorID: actorID,
		Backend: Backend{
			IP:   workerIP,
			Port: 80, // TODO(bowei): handle more than port 80 on the actor.
		},
		TemplateRef: ActorTemplateRef{
			Namespace: tmplNs,
			Name:      tmplName,
		},
	}}
}
