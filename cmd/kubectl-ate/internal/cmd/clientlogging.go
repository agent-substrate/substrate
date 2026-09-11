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
	"context"
	"fmt"
	"io"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// configureClientLogging keeps client-go's unhandled-error reports out of the
// user's terminal unless --verbose asks for them.
//
// client-go reports problems it cannot return to a caller through
// utilruntime.HandleError, whose default handler writes to stderr naming the
// client-go file that failed, for example:
//
//	E0520 15:40:33.154830 3507355 portforward.go:502] "Unhandled Error" err="..."
//
// Someone running `kubectl ate create actor` cannot act on that, and the
// port-forwarding it refers to is an implementation detail of this plugin.
//
// klog is deliberately left alone. client-go logs warnings a user can act on
// directly through it, such as a missing kubeconfig or a credential plugin that
// failed to refresh, and those should keep reaching the terminal. Replacing
// ErrorHandlers also drops upstream's 1ms rate limiter, which existed only
// because the default handler writes to stderr.
func configureClientLogging(errOut io.Writer, verbose bool) {
	utilruntime.ErrorHandlers = []utilruntime.ErrorHandler{
		func(_ context.Context, err error, msg string, keysAndValues ...any) {
			if !verbose {
				return
			}
			fmt.Fprintf(errOut, "kubectl-ate: %s\n", utilruntime.ErrorToString(err, msg, keysAndValues...))
		},
	}
}
