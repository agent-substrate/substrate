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
	"testing"

	"github.com/spf13/pflag"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/steps"
)

// An upgrade or a rollback moves the control plane one component at a time,
// so the components that only deploy ate-system used to install have their own
// subcommands.
func TestComponentDeploySubcommands(t *testing.T) {
	for _, tc := range []struct {
		arg  string
		want string
	}{
		{"podcertificate-controller", "podcertificate-controller"},
		{"podcert", "podcertificate-controller"},
		{"sandboxconfig", "sandboxconfig"},
	} {
		t.Run(tc.arg, func(t *testing.T) {
			sub, _, err := deployCmd.Find([]string{tc.arg})
			if err != nil {
				t.Fatalf("deploy %s: %v", tc.arg, err)
			}
			if sub.Name() != tc.want {
				t.Fatalf("deploy %s resolved to %q, want %q", tc.arg, sub.Name(), tc.want)
			}
			if err := sub.Args(sub, []string{"extra"}); err == nil {
				t.Errorf("deploy %s accepted a stray argument", tc.arg)
			}
		})
	}
}

func resetCommandFlags(t *testing.T, flags *pflag.FlagSet) {
	t.Helper()
	reset := func() {
		flags.VisitAll(func(f *pflag.Flag) {
			_ = f.Value.Set(f.DefValue)
			f.Changed = false
		})
	}
	reset()
	t.Cleanup(reset)
}

func TestDeploySkipMicroVMDepsFlag(t *testing.T) {
	t.Run("deploy ate-system", func(t *testing.T) {
		resetCommandFlags(t, deployAteSystemCmd.Flags())
		if err := deployAteSystemCmd.ParseFlags([]string{"--skip-microvm-deps"}); err != nil {
			t.Fatalf("ParseFlags(--skip-microvm-deps): %v", err)
		}
		r, err := config.Resolve(deployAteSystemCmd.Flags(), config.ResolveOptions{})
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		cfg := &config.Config{}
		cfg.SetResolved(r)

		prev := env
		env = &steps.Env{Cfg: cfg}
		t.Cleanup(func() { env = prev })

		opts, err := deployOptions()
		if err != nil {
			t.Fatalf("deployOptions(): %v", err)
		}
		if !opts.SkipMicroVMDeps {
			t.Error("opts.SkipMicroVMDeps = false, want true")
		}
	})

	t.Run("deploy sandboxconfig", func(t *testing.T) {
		if deploySandboxConfigCmd.Flags().Lookup("skip-microvm-deps") == nil {
			t.Fatal("deploy sandboxconfig is missing --skip-microvm-deps flag")
		}
		resetCommandFlags(t, deploySandboxConfigCmd.Flags())
		if err := deploySandboxConfigCmd.ParseFlags([]string{"--skip-microvm-deps"}); err != nil {
			t.Fatalf("ParseFlags(--skip-microvm-deps): %v", err)
		}
		r, err := config.Resolve(deploySandboxConfigCmd.Flags(), config.ResolveOptions{})
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if !r.Bool("skipMicroVMDeps") {
			t.Error("r.Bool(\"skipMicroVMDeps\") = false, want true")
		}
	})
}
