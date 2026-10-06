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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/hostruntimepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var actorUIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type childProcess interface {
	Wait() error
}

type commandRunner interface {
	Run(context.Context, io.Writer, string, ...string) error
	Start(io.Writer, string, ...string) (childProcess, error)
}

type execRunner struct{}

func (execRunner) Run(ctx context.Context, output io.Writer, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout, cmd.Stderr = output, output
	return cmd.Run()
}

func (execRunner) Start(output io.Writer, name string, args ...string) (childProcess, error) {
	cmd := exec.Command(name, args...) // Deliberately independent of the activation RPC context.
	cmd.Stdout, cmd.Stderr = output, output
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd, nil
}

type vmStatus struct {
	ActorID   string `json:"actorID"`
	Phase     string `json:"phase"`
	Ready     bool   `json:"ready"`
	IPAddress string `json:"ipAddress"`
	Detail    string `json:"detail"`
}

type actorProcess struct {
	done chan error
}

type actorProxy struct {
	listener net.Listener
	server   *http.Server
}

type server struct {
	hostruntimepb.UnimplementedHostRuntimeServer
	maclet, stateDir, image, sourceBundle string
	advertiseHost, proxyListen            string
	runner                                commandRunner
	pollInterval                          time.Duration

	mu       sync.Mutex
	locks    map[string]*sync.Mutex
	children map[string]*actorProcess
	proxies  map[string]*actorProxy
}

func newServer(maclet, stateDir, image, sourceBundle, advertiseHost, proxyListen string) *server {
	return &server{maclet: maclet, stateDir: stateDir, image: image, sourceBundle: sourceBundle,
		advertiseHost: advertiseHost, proxyListen: proxyListen, runner: execRunner{}, pollInterval: 200 * time.Millisecond,
		locks: make(map[string]*sync.Mutex), children: make(map[string]*actorProcess), proxies: make(map[string]*actorProxy)}
}

func (s *server) actorLock(uid string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.locks[uid] == nil {
		s.locks[uid] = &sync.Mutex{}
	}
	return s.locks[uid]
}

func validateUID(uid string) error {
	if !actorUIDPattern.MatchString(uid) || uid == "." || uid == ".." {
		return fmt.Errorf("actor_uid must be 1...128 safe path characters")
	}
	return nil
}

func validateActivate(req *hostruntimepb.ActivateRequest, image string) error {
	if req == nil {
		return errors.New("request is required")
	}
	if err := validateUID(req.GetActorUid()); err != nil {
		return err
	}
	if req.GetImage() != image {
		return fmt.Errorf("image must exactly match configured image")
	}
	p := req.GetReadinessProbe()
	if p == nil || p.GetPort() < 1 || p.GetPort() > 65535 || p.GetTimeoutSeconds() < 1 || p.GetTimeoutSeconds() > 3600 {
		return fmt.Errorf("readiness probe needs port 1...65535 and timeout 1...3600 seconds")
	}
	if !validProbePath(p.GetPath()) {
		return fmt.Errorf("readiness probe path must be an absolute URL path of at most 1024 bytes")
	}
	return nil
}

func validProbePath(path string) bool {
	if len(path) == 0 || len(path) > 1024 || path[0] != '/' {
		return false
	}
	const allowed = "/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~!$&'()*+,;=:@%"
	return strings.IndexFunc(path, func(r rune) bool { return !strings.ContainsRune(allowed, r) }) == -1
}

func (s *server) bundle(uid string) string { return filepath.Join(s.stateDir, uid) }

func (s *server) Activate(ctx context.Context, req *hostruntimepb.ActivateRequest) (*hostruntimepb.ActivateResponse, error) {
	if err := validateActivate(req, s.image); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	uid := req.GetActorUid()
	lock := s.actorLock(uid)
	lock.Lock()
	defer lock.Unlock()
	bundle := s.bundle(uid)

	exists, err := safeDirectory(bundle)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "actor bundle: %v", err)
	}
	if !exists {
		if err := s.runner.Run(ctx, io.Discard, s.maclet, "create", s.sourceBundle, bundle, uid); err != nil {
			return nil, status.Errorf(codes.Internal, "create actor bundle: %v", err)
		}
	}
	st, statusErr := s.readStatus(ctx, bundle)
	if statusErr == nil && st.ActorID != uid {
		return nil, status.Error(codes.FailedPrecondition, "actor bundle belongs to a different Actor")
	}
	if statusErr == nil && st.Ready {
		return s.endpoint(uid, st, req.GetReadinessProbe().GetPort())
	}

	s.mu.Lock()
	child := s.children[uid]
	s.mu.Unlock()
	// After a daemon restart, the bundle lock and status are authoritative. Do
	// not start a second owner when the original maclet is still starting/running.
	alreadyOwned := statusErr == nil && (st.Phase == "starting" || st.Phase == "running" || st.Phase == "stopping")
	if child == nil && !alreadyOwned {
		logFile, err := os.OpenFile(filepath.Join(bundle, "maclet.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "open actor log: %v", err)
		}
		p := req.GetReadinessProbe()
		proc, err := s.runner.Start(logFile, s.maclet, "start", bundle, strconv.Itoa(int(p.GetPort())), p.GetPath(), strconv.Itoa(int(p.GetTimeoutSeconds())))
		if err != nil {
			_ = logFile.Close()
			return nil, status.Errorf(codes.Internal, "start maclet: %v", err)
		}
		child = &actorProcess{done: make(chan error, 1)}
		s.mu.Lock()
		s.children[uid] = child
		s.mu.Unlock()
		go func() {
			child.done <- proc.Wait()
			_ = logFile.Close()
		}()
	}

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()
	var childDone <-chan error
	if child != nil {
		childDone = child.done
	}
	for {
		st, err := s.readStatus(ctx, bundle)
		if err == nil {
			if st.Ready {
				return s.endpoint(uid, st, req.GetReadinessProbe().GetPort())
			}
			if st.Phase == "failed" || (st.Phase == "stopped" && child == nil) {
				return nil, status.Errorf(codes.Internal, "maclet %s: %s", st.Phase, st.Detail)
			}
		}
		select {
		case err := <-childDone:
			s.mu.Lock()
			delete(s.children, uid)
			s.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "maclet exited before readiness: %v", err)
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-ticker.C:
		}
	}
}

func (s *server) endpoint(uid string, st *vmStatus, port int32) (*hostruntimepb.ActivateResponse, error) {
	if net.ParseIP(st.IPAddress) == nil {
		return nil, status.Error(codes.Internal, "ready maclet reported an invalid IP address")
	}
	s.mu.Lock()
	proxy := s.proxies[uid]
	s.mu.Unlock()
	if proxy == nil {
		target, err := url.Parse("http://" + net.JoinHostPort(st.IPAddress, strconv.Itoa(int(port))))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "build guest proxy target: %v", err)
		}
		listener, err := net.Listen("tcp", s.proxyListen)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "listen for Actor proxy: %v", err)
		}
		proxy = &actorProxy{listener: listener, server: &http.Server{
			Handler:           httputil.NewSingleHostReverseProxy(target),
			ReadHeaderTimeout: 10 * time.Second,
		}}
		s.mu.Lock()
		if existing := s.proxies[uid]; existing != nil {
			s.mu.Unlock()
			_ = listener.Close()
			proxy = existing
		} else {
			s.proxies[uid] = proxy
			s.mu.Unlock()
			go func() { _ = proxy.server.Serve(proxy.listener) }()
		}
	}
	_, portText, err := net.SplitHostPort(proxy.listener.Addr().String())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read Actor proxy address: %v", err)
	}
	proxyPort, err := strconv.ParseInt(portText, 10, 32)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read Actor proxy port: %v", err)
	}
	return &hostruntimepb.ActivateResponse{Endpoint: &hostruntimepb.ActorEndpoint{Host: s.advertiseHost, Port: int32(proxyPort)}}, nil
}

func (s *server) readStatus(ctx context.Context, bundle string) (*vmStatus, error) {
	var output strings.Builder
	if err := s.runner.Run(ctx, &output, s.maclet, "status", bundle); err != nil {
		return nil, err
	}
	var st vmStatus
	if err := json.Unmarshal([]byte(output.String()), &st); err != nil {
		return nil, err
	}
	return &st, nil
}

func (s *server) Terminate(ctx context.Context, req *hostruntimepb.TerminateRequest) (*hostruntimepb.TerminateResponse, error) {
	if req == nil || validateUID(req.GetActorUid()) != nil {
		return nil, status.Error(codes.InvalidArgument, "valid actor_uid is required")
	}
	uid := req.GetActorUid()
	lock := s.actorLock(uid)
	lock.Lock()
	defer lock.Unlock()
	bundle := s.bundle(uid)
	exists, err := safeDirectory(bundle)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "actor bundle: %v", err)
	}
	if !exists {
		return &hostruntimepb.TerminateResponse{}, nil
	}
	var output strings.Builder
	if err := s.runner.Run(ctx, &output, s.maclet, "stop", bundle); err != nil {
		return nil, status.Errorf(codes.Internal, "stop maclet: %v", err)
	}
	s.mu.Lock()
	child := s.children[uid]
	s.mu.Unlock()
	if child != nil {
		select {
		case <-child.done:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		s.mu.Lock()
		delete(s.children, uid)
		s.mu.Unlock()
	}
	s.mu.Lock()
	proxy := s.proxies[uid]
	delete(s.proxies, uid)
	s.mu.Unlock()
	if proxy != nil {
		if err := proxy.server.Shutdown(ctx); err != nil {
			return nil, status.Errorf(codes.Internal, "stop Actor proxy: %v", err)
		}
	}
	if err := os.RemoveAll(bundle); err != nil {
		return nil, status.Errorf(codes.Internal, "remove actor bundle: %v", err)
	}
	return &hostruntimepb.TerminateResponse{}, nil
}

func safeDirectory(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false, errors.New("path is not a real directory")
	}
	return true, nil
}
