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

package k8sresolver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/grpc/resolver"
	v1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	discoverylisters "k8s.io/client-go/listers/discovery/v1"
	"k8s.io/client-go/tools/cache"
)

const Scheme = "k8s"

// Builder implements resolver.Builder for Kubernetes EndpointSlices backed by client-go Informers.
type Builder struct{ client kubernetes.Interface }

// NewBuilder creates a new gRPC resolver Builder for EndpointSlices using client-go Informers.
func NewBuilder(client kubernetes.Interface) *Builder { return &Builder{client: client} }
func (b *Builder) Scheme() string                     { return Scheme }

func (b *Builder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	if b == nil || b.client == nil {
		return nil, fmt.Errorf("k8sresolver: kubernetes client is required")
	}

	ns, svc, port, err := ParseTarget(target)
	if err != nil {
		return nil, fmt.Errorf("k8sresolver: %w", err)
	}

	cli := b.client

	selector := labels.Set{"kubernetes.io/service-name": svc}.AsSelector()
	factory := informers.NewSharedInformerFactoryWithOptions(cli, 0,
		informers.WithNamespace(ns),
		informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.LabelSelector = selector.String() }),
	)

	inf := factory.Discovery().V1().EndpointSlices()
	ctx, cancel := context.WithCancel(context.Background())
	r := &k8sResolver{
		ctx:      ctx,
		cancel:   cancel,
		service:  svc,
		port:     port,
		cc:       cc,
		informer: inf.Informer(),
		lister:   inf.Lister().EndpointSlices(ns),
		selector: selector,
	}

	_ = inf.Informer().SetWatchErrorHandler(func(_ *cache.Reflector, err error) {
		if ctx.Err() == nil {
			slog.Error("k8sresolver watch error", slog.String("namespace", ns), slog.String("service", svc), slog.Any("error", err))
			cc.ReportError(fmt.Errorf("k8sresolver: watch error for %s/%s: %w", ns, svc, err))
		}
	})

	reg, err := inf.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(_ any) { r.updateState() },
		UpdateFunc: func(_, _ any) { r.updateState() },
		DeleteFunc: func(_ any) { r.updateState() },
	})
	if err != nil {
		return nil, fmt.Errorf("k8sresolver: failed to add event handler for %s/%s: %w", ns, svc, err)
	}

	r.registration = reg

	factory.Start(ctx.Done())
	go func() {
		if cache.WaitForCacheSync(ctx.Done(), inf.Informer().HasSynced) {
			r.updateState()
		} else if ctx.Err() == nil {
			slog.Error("k8sresolver failed to sync EndpointSlice cache", slog.String("namespace", ns), slog.String("service", svc))
			cc.ReportError(fmt.Errorf("k8sresolver: failed to sync EndpointSlice cache for %s/%s", ns, svc))
		}
	}()
	return r, nil
}

// ParseTarget parses resolver.Target in canonical format k8s:///<namespace>/<service>[:port] or k8s:///<service>.<namespace>[:port].
func ParseTarget(target resolver.Target) (ns, svc, port string, err error) {
	ep := target.Endpoint()
	if target.URL.Host != "" && target.URL.Host != "localhost" {
		ep = target.URL.Host + "/" + strings.TrimPrefix(target.URL.Path, "/")
	}

	if parts := strings.SplitN(ep, "/", 2); len(parts) == 2 {
		ns, ep = parts[0], parts[1]
	} else {
		ns = "default"
	}

	svc, port, err = net.SplitHostPort(ep)
	if err != nil {
		svc, port = ep, "443"
	}

	if parts := strings.Split(svc, "."); len(parts) > 1 {
		svc, ns = parts[0], parts[1]
	}

	if ns == "" || svc == "" {
		return "", "", "", fmt.Errorf("invalid target %q, expected k8s:///<namespace>/<service>[:port]", target.String())
	}
	return ns, svc, port, nil
}

type k8sResolver struct {
	ctx          context.Context
	cancel       context.CancelFunc
	service      string
	port         string
	cc           resolver.ClientConn
	informer     cache.SharedIndexInformer
	registration cache.ResourceEventHandlerRegistration
	lister       discoverylisters.EndpointSliceNamespaceLister
	selector     labels.Selector
}

func (r *k8sResolver) ResolveNow(resolver.ResolveNowOptions) { r.updateState() }

func (r *k8sResolver) Close() {
	r.cancel()
	if r.registration != nil {
		_ = r.informer.RemoveEventHandler(r.registration)
	}
}

func (r *k8sResolver) updateState() {
	if r.ctx.Err() != nil {
		return
	}

	eps, err := r.lister.List(r.selector)
	if err != nil {
		if r.ctx.Err() == nil {
			r.cc.ReportError(err)
		}
		return
	}

	targetPort, _ := strconv.Atoi(r.port)
	var addrs []resolver.Address
	for _, slice := range eps {
		p := targetPort
		if p == 0 && len(slice.Ports) > 0 && slice.Ports[0].Port != nil {
			p = int(*slice.Ports[0].Port)
		}
		if p == 0 {
			p = 443
		}
		for _, ep := range slice.Endpoints {
			if !isHealthy(&ep) {
				continue
			}
			for _, ip := range ep.Addresses {
				addrs = append(addrs, resolver.Address{Addr: net.JoinHostPort(ip, strconv.Itoa(p))})
			}
		}
	}

	slices.SortFunc(addrs, func(a, b resolver.Address) int { return strings.Compare(a.Addr, b.Addr) })
	addrs = slices.CompactFunc(addrs, func(a, b resolver.Address) bool { return a.Addr == b.Addr })

	if err := r.cc.UpdateState(resolver.State{Addresses: addrs}); err != nil && r.ctx.Err() == nil {
		r.cc.ReportError(err)
	}
}

func isHealthy(ep *v1.Endpoint) bool {
	// ready indicates that this endpoint is prepared to receive traffic,
	// according to whatever system is managing the endpoint. A nil value
	// indicates an unknown state. In most cases consumers should interpret this
	// unknown state as ready.
	// More info: vendor/k8s.io/api/discovery/v1/types.go
	isReady := ep.Conditions.Ready == nil || *ep.Conditions.Ready
	// serving is identical to ready except that it is set regardless of the
	// terminating state of endpoints. This condition should be set to true for
	// a ready endpoint that is terminating. If nil, consumers should defer to
	// the ready condition.
	// More info: vendor/k8s.io/api/discovery/v1/types.go
	isServing := (ep.Conditions.Serving == nil && isReady) || (ep.Conditions.Serving != nil && *ep.Conditions.Serving)

	// Return healthy for endpoints that are either ready or serving. We can
	// still send traffic to terminating pods so don't immediately drop
	// connections.
	return isServing
}
