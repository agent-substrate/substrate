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

package e2e

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	namespacesToCleanup []string
	namespacesMu        sync.Mutex
	rnd                 = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// randomnamepaceName returns a random namespace name.
// Format is "aaaa-ddd" where a is a random letter and d is a random digit.
func randomNamespaceName() string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	const digits = "0123456789"

	var sb strings.Builder
	for range 4 {
		sb.WriteByte(letters[rnd.Intn(len(letters))])
	}
	sb.WriteByte('-')
	for range 3 {
		sb.WriteByte(digits[rnd.Intn(len(digits))])
	}
	return sb.String()
}

type Namespace struct {
	Name string
}

// CreateNamespace creates a new namespace with a randomized name using the K8s API
// and registers it for cleanup at the end of the test.
func CreateNamespace(t *testing.T) *Namespace {
	t.Helper()

	// Check that we didn't dupe a name in namespacesToCleanup
	namespacesMu.Lock()
	var nsName string
	for range 1000 {
		name := randomNamespaceName()
		if !slices.Contains(namespacesToCleanup, name) {
			namespacesToCleanup = append(namespacesToCleanup, name)
			nsName = name
			break
		}
	}
	namespacesMu.Unlock()

	if nsName == "" {
		// This should really never happen.
		t.Fatalf("Failed to create unique namespace name.")
	}

	t.Logf("Creating namespace: %s", nsName)

	clients := GetClients()

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: nsName,
		},
	}

	_, err := clients.K8s.CoreV1().Namespaces().Create(t.Context(), ns, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create namespace %s: %v", nsName, err)
	}

	// Wait for namespace to be active
	const timeout = 60 * time.Second
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	for {
		ns, err := clients.K8s.CoreV1().Namespaces().Get(ctx, nsName, metav1.GetOptions{})
		if err == nil && ns.Status.Phase == corev1.NamespaceActive {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Timed out waiting for namespace %q to be active after %v: %v", nsName, timeout, err)
		case <-time.After(200 * time.Millisecond):
			// Keep polling.
		}
	}

	return &Namespace{Name: nsName}
}

// Delete the namespace explicitly. This will fail the test if deletion fails.
func (ns *Namespace) Delete(t *testing.T) {
	t.Helper()
	err := sharedClients.K8s.CoreV1().Namespaces().Delete(t.Context(), ns.Name, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("Failed to delete namespace %s: %v", ns.Name, err)
	}
}

// CleanupNamespaces deletes all registered namespaces using the K8s API.
// This should be called at the end of RunTestMain.
func CleanupNamespaces() {
	namespacesMu.Lock()
	defer namespacesMu.Unlock()

	if len(namespacesToCleanup) == 0 {
		return
	}

	clients := GetClients()

	fmt.Printf("Cleaning up %d namespaces...\n", len(namespacesToCleanup))
	for _, ns := range namespacesToCleanup {
		fmt.Printf("Deleting namespace %s...\n", ns)
		err := clients.K8s.CoreV1().Namespaces().Delete(context.Background(), ns, metav1.DeleteOptions{})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to delete namespace %s: %v\n", ns, err)
		}
	}
	namespacesToCleanup = nil
}
