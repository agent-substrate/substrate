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
	"io"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
)

// runRoot executes the command tree with args, quietly.
//
// The command tree is package state, so this puts it back as it found it.
func runRoot(t *testing.T, args ...string) error {
	t.Helper()
	rootCmd.SetArgs(args)
	rootCmd.SetOut(io.Discard)
	rootCmd.SetErr(io.Discard)
	t.Cleanup(func() {
		rootCmd.SetArgs(nil)
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
	})
	return rootCmd.Execute()
}

// A driver name nothing can install has to be rejected during argument
// validation, which is the last step before the root command's
// PersistentPreRunE loads the configuration, fetches cluster credentials, and
// connects. Caught any later, a typo costs at least a credential fetch and --
// when SetupCSI ran partway through `deploy ate-system` -- at worst a
// half-installed cluster.
//
// env staying nil is how that is checked here: PersistentPreRunE is the only
// thing that sets it.
func TestAnUnknownCSIDriverIsRejectedBeforeAnythingRuns(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"deploy ate-system", []string{"deploy", "ate-system", "--setup-csi=hostpaht"}},
		{"setup csi", []string{"setup", "csi", "nfsv4"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if env != nil {
				t.Fatal("env was already populated; this test cannot tell whether the command connected")
			}

			err := runRoot(t, tc.args...)
			if err == nil {
				t.Fatal("the command succeeded, want an unknown-driver error")
			}
			if !strings.Contains(err.Error(), "unknown CSI driver") {
				t.Errorf("error = %v, want it to name the unknown driver", err)
			}
			if env != nil {
				t.Error("the command connected to a cluster before rejecting the driver")
			}
		})
	}
}

// The accepted spellings have to make it past the same check, including the
// bare `setup csi` that takes the default.
func TestAcceptedCSIDriversPassArgumentValidation(t *testing.T) {
	accepted := []string{"nfs", "hostpath", "both", "none", "true", "false", ""}

	t.Run("setup csi", func(t *testing.T) {
		for _, driver := range accepted {
			if err := setupCSICmd.Args(setupCSICmd, []string{driver}); err != nil {
				t.Errorf("setup csi %q: %v", driver, err)
			}
		}
		if err := setupCSICmd.Args(setupCSICmd, nil); err != nil {
			t.Errorf("setup csi with no argument: %v", err)
		}
	})

	t.Run("deploy ate-system", func(t *testing.T) {
		// Args reads the flag, not a package variable: it runs before the
		// configuration is loaded, so the flag is the only channel it can see.
		f := deployAteSystemCmd.Flags().Lookup("setup-csi")
		t.Cleanup(func() {
			_ = f.Value.Set("none")
			f.Changed = false
		})
		for _, driver := range accepted {
			if err := f.Value.Set(driver); err != nil {
				t.Fatalf("Set(%q): %v", driver, err)
			}
			f.Changed = true
			if err := deployAteSystemCmd.Args(deployAteSystemCmd, nil); err != nil {
				t.Errorf("--setup-csi=%q: %v", driver, err)
			}
		}
	})
}

// The --setup-csi check is additional to the argument checking cobra was doing
// before, not a replacement for it.
func TestCSICommandsStillRejectStrayArguments(t *testing.T) {
	for _, tc := range []struct {
		name string
		cmd  *cobra.Command
		args []string
	}{
		{"deploy ate-system", deployAteSystemCmd, []string{"nfs"}},
		{"setup csi", setupCSICmd, []string{"nfs", "hostpath"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cmd.Args(tc.cmd, tc.args); err == nil {
				t.Errorf("Args(%v) = nil, want an error", tc.args)
			}
		})
	}
}

// A driver supplied by the environment or a configuration file cannot be
// checked in Args, which runs before the configuration is loaded. It is
// checked in RunE instead, so the value still has to be rejected -- just after
// the credential fetch rather than before it.
func TestCSIDriverFromTheEnvironmentIsStillRejected(t *testing.T) {
	for _, tc := range []struct {
		name    string
		driver  string
		wantErr bool
	}{
		{name: "accepted", driver: "nfs"},
		{name: "rejected", driver: "hostpaht", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := config.Resolve(nil, config.ResolveOptions{
				Env: map[string]string{"SETUP_CSI": tc.driver},
			})
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			cfg := &config.Config{}
			cfg.SetResolved(r)

			prev := env
			env = &steps.Env{Cfg: cfg}
			t.Cleanup(func() { env = prev })

			_, err = deployOptions()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("deployOptions() = nil error for driver %q, want one", tc.driver)
				}
				// The value came from the environment, so the message has to
				// name the variable rather than --setup-csi.
				for _, want := range []string{"csi.setup", tc.driver, "SETUP_CSI"} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("deployOptions() = %q, missing %q", err, want)
					}
				}
				return
			}
			if err != nil {
				t.Errorf("deployOptions() = %v, want nil", err)
			}
		})
	}
}
