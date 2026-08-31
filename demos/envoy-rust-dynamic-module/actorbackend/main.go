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

// actorbackend stands in for the worker pod hosting an actor.
//
// It listens on the port the router routes to (atunnel's ingress port, 443)
// and echoes back the actor DNS name it was addressed as, so the load generator
// can assert that a request actually reached the worker its actor is pinned to.
// It does as little work as possible on purpose: the demo measures the routing
// decision, so anything the backend spends would only dilute the difference
// between the two arms.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"
)

var (
	addr  = flag.String("addr", ":443", "listen address; matches the atunnel ingress port the router targets")
	name  = flag.String("name", "worker", "worker name reported in responses")
	delay = flag.Duration("delay", 0, "artificial per-request handling delay")
)

func main() {
	flag.Parse()

	srv := &http.Server{
		Addr: *addr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if *delay > 0 {
				time.Sleep(*delay)
			}
			w.Header().Set("x-ate-worker", *name)
			// Host is the actor's own DNS name: the router deliberately leaves
			// :authority untouched so the worker can authorize on it.
			fmt.Fprintf(w, "worker=%s actor=%s port=%s\n", *name, r.Host, r.Header.Get("x-ate-target-port"))
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("actorbackend %s listening on %s", *name, *addr)
	log.Fatal(srv.ListenAndServe())
}
