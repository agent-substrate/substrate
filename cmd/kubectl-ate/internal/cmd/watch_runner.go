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

package cmd

import (
	"bytes"
	"context"
	"io"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/wait"
)

const defaultGetWatchInterval = time.Second

type getWatchRunner[M proto.Message] struct {
	fetch        func(context.Context) ([]M, error)
	print        func(io.Writer, []M) error
	key          func(M) string
	out          io.Writer
	format       string
	pollInterval time.Duration
}

func printOrWatch[M proto.Message](
	ctx context.Context,
	out io.Writer,
	watch bool,
	format string,
	fetch func(context.Context) ([]M, error),
	print func(io.Writer, []M, string) error,
	key func(M) string,
) error {
	printFormatted := func(out io.Writer, resources []M) error {
		return print(out, resources, format)
	}
	if watch {
		return (&getWatchRunner[M]{
			fetch:  fetch,
			print:  printFormatted,
			key:    key,
			out:    out,
			format: format,
		}).Run(ctx)
	}

	resources, err := fetch(ctx)
	if err != nil {
		return err
	}
	return printFormatted(out, resources)
}

func (r *getWatchRunner[M]) Run(ctx context.Context) error {
	resources, err := r.fetch(ctx)
	if err != nil {
		return err
	}
	if err := r.print(r.out, resources); err != nil {
		return err
	}
	previous := r.index(resources)

	interval := r.pollInterval
	if interval <= 0 {
		interval = defaultGetWatchInterval
	}
	return wait.PollUntilContextCancel(ctx, interval, false, func(ctx context.Context) (bool, error) {
		resources, err := r.fetch(ctx)
		if err != nil {
			return false, err
		}
		current := r.index(resources)
		changed := changedResources(previous, current)
		if len(changed) > 0 {
			if err := r.printUpdate(changed); err != nil {
				return false, err
			}
		}
		previous = current
		return false, nil
	})
}

func (r *getWatchRunner[M]) index(resources []M) map[string]M {
	indexed := make(map[string]M, len(resources))
	for _, resource := range resources {
		indexed[r.key(resource)] = resource
	}
	return indexed
}

func changedResources[M proto.Message](previous, current map[string]M) []M {
	changed := make([]M, 0)
	for key, resource := range current {
		old, found := previous[key]
		if !found || !proto.Equal(old, resource) {
			changed = append(changed, resource)
		}
	}
	for key, resource := range previous {
		if _, found := current[key]; !found {
			changed = append(changed, resource)
		}
	}
	return changed
}

func (r *getWatchRunner[M]) printUpdate(resources []M) error {
	if r.format != "table" {
		return r.print(r.out, resources)
	}

	var buf bytes.Buffer
	if err := r.print(&buf, resources); err != nil {
		return err
	}
	output := buf.String()
	if newline := strings.IndexByte(output, '\n'); newline >= 0 {
		output = output[newline+1:]
	}
	_, err := io.WriteString(r.out, output)
	return err
}
