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
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// usageError marks an error caused by invalid CLI invocation (bad or missing
// flags, arguments, or subcommands), as opposed to a runtime failure.
// executeRoot prints the error and the offending command's usage to stderr.
type usageError struct {
	err error
}

func (e *usageError) Error() string { return e.err.Error() }
func (e *usageError) Unwrap() error { return e.err }

func usageErrorf(format string, args ...any) error {
	return &usageError{err: fmt.Errorf(format, args...)}
}

func wrapUsageError(err error) error {
	if err == nil {
		return nil
	}
	var u *usageError
	if errors.As(err, &u) {
		return err
	}
	return &usageError{err: err}
}

func formatFlagName(f *pflag.Flag) string {
	if f.Shorthand != "" {
		return fmt.Sprintf("-%s/--%s", f.Shorthand, f.Name)
	}
	return fmt.Sprintf("--%s", f.Name)
}

// handleFlagError translates pflag errors into user-friendly usage errors.
func handleFlagError(cmd *cobra.Command, err error) error {
	// When a user invokes a group command with an unknown subcommand and
	// additional flags (e.g. "kubectl ate logs my-actor -a space"), Find stops
	// at the group command and ParseFlags trips on the flag. Report the
	// unknown subcommand rather than the flag.
	if isGroupCommand(cmd) {
		if args := cmd.Flags().Args(); len(args) > 0 {
			return unknownSubcommandError(cmd, args[0])
		}
	}

	var valReq *pflag.ValueRequiredError
	if errors.As(err, &valReq) {
		if f := valReq.GetFlag(); f != nil {
			return usageErrorf("flag %q requires a value", formatFlagName(f))
		}
	}

	var notExist *pflag.NotExistError
	if errors.As(err, &notExist) {
		return usageErrorf("unknown flag %q", specifiedNotExistFlag(notExist))
	}

	return wrapUsageError(err)
}

func specifiedNotExistFlag(e *pflag.NotExistError) string {
	if e.GetSpecifiedShortnames() != "" {
		return "-" + e.GetSpecifiedName()
	}
	return "--" + e.GetSpecifiedName()
}

// checkSwallowedFlags detects when a string flag consumed the next flag token
// as its argument (for example, "kubectl ate create actor my-actor -a --template foo").
func checkSwallowedFlags(cmd *cobra.Command) error {
	var swallowedErr error
	cmd.Flags().Visit(func(f *pflag.Flag) {
		if swallowedErr != nil || f.Value.Type() != "string" {
			return
		}
		val := f.Value.String()
		if val == "-" && (f.Name == "filename" || f.Name == "token-file") {
			return
		}
		if strings.HasPrefix(val, "-") {
			swallowedErr = usageErrorf("flag %q requires a value, got flag %q", formatFlagName(f), val)
		}
	})
	return swallowedErr
}

func missingRequiredFlags(cmd *cobra.Command, requiredFlags []string) []string {
	var missing []string
	for _, name := range requiredFlags {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			continue
		}
		if !f.Changed || strings.TrimSpace(f.Value.String()) == "" {
			missing = append(missing, formatFlagName(f))
		}
	}
	return missing
}

func formatMissingFlagsClause(missing []string) string {
	if len(missing) == 1 {
		return "required flag " + missing[0]
	}
	return "required flags " + strings.Join(missing, ", ")
}

func joinQuoted(items []string) string {
	quoted := make([]string, len(items))
	for i, s := range items {
		quoted[i] = fmt.Sprintf("%q", s)
	}
	return strings.Join(quoted, ", ")
}

// exactlyOneArg validates that the command received exactly one positional
// argument (named argPlaceholder, e.g. "<actor-name>") and that all
// requiredFlags were set, reporting everything missing in a single error.
func exactlyOneArg(argPlaceholder string, requiredFlags ...string) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if err := checkSwallowedFlags(cmd); err != nil {
			return err
		}
		missingFlags := missingRequiredFlags(cmd, requiredFlags)
		if len(args) == 0 {
			if len(missingFlags) > 0 {
				return usageErrorf("missing required argument %s and %s", argPlaceholder, formatMissingFlagsClause(missingFlags))
			}
			return usageErrorf("missing required argument %s", argPlaceholder)
		}
		if len(args) > 1 {
			return usageErrorf("expected 1 %s argument, received %d (%s)", argPlaceholder, len(args), joinQuoted(args))
		}
		if len(missingFlags) > 0 {
			return usageErrorf("missing %s", formatMissingFlagsClause(missingFlags))
		}
		return nil
	}
}

// noArgs validates that the command received no positional arguments and that
// all requiredFlags were set. If extraArgHint is non-empty, it is included in
// the error when an unexpected positional argument is passed.
func noArgs(extraArgHint string, requiredFlags ...string) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if err := checkSwallowedFlags(cmd); err != nil {
			return err
		}
		if len(args) > 0 {
			if extraArgHint != "" {
				return usageErrorf("unexpected argument %q (%s)", args[0], extraArgHint)
			}
			return usageErrorf("%q takes no positional arguments (received %s)", cmd.CommandPath(), joinQuoted(args))
		}
		if missingFlags := missingRequiredFlags(cmd, requiredFlags); len(missingFlags) > 0 {
			return usageErrorf("missing %s", formatMissingFlagsClause(missingFlags))
		}
		return nil
	}
}

const isSubcommandAnnotation = "kubectl-ate/is-a-subcommand"

func isGroupCommand(cmd *cobra.Command) bool {
	return cmd.Annotations[isSubcommandAnnotation] != ""
}

// requireSubcommand configures a group command (such as root, a verb, or admin)
// so that invoking it with no subcommand or an unknown subcommand returns a
// descriptive usageError instead of exiting 0.
func requireSubcommand(cmd *cobra.Command) {
	if cmd.Annotations == nil {
		cmd.Annotations = map[string]string{}
	}
	cmd.Annotations[isSubcommandAnnotation] = "true"
	cmd.SuggestionsMinimumDistance = 1
	cmd.Args = func(c *cobra.Command, args []string) error {
		if len(args) == 0 {
			return usageErrorf("missing required subcommand")
		}
		return unknownSubcommandError(c, args[0])
	}
	cmd.RunE = func(c *cobra.Command, args []string) error {
		return usageErrorf("missing required subcommand")
	}
}

func unknownSubcommandError(cmd *cobra.Command, arg string) error {
	if cmd.SuggestionsMinimumDistance <= 0 {
		cmd.SuggestionsMinimumDistance = 1
	}
	if suggestions := cmd.SuggestionsFor(arg); len(suggestions) > 0 {
		return usageErrorf("unknown subcommand %q (did you mean %q?)", arg, suggestions[0])
	}
	var available []*cobra.Command
	for _, sub := range cmd.Commands() {
		if sub.IsAvailableCommand() && sub.Name() != "help" && sub.Name() != "completion" {
			available = append(available, sub)
		}
	}
	if len(available) == 1 {
		return usageErrorf("unknown subcommand %q (did you mean \"%s %s\"?)",
			arg, available[0].Name(), arg)
	}
	return usageErrorf("unknown subcommand %q", arg)
}

// validateAtespaceFlags checks the --atespace and -A/--all-atespaces flags
// for atespace-scoped "get" commands (actors, actor-templates, tags) before
// connecting to the control plane.
func validateAtespaceFlags(resourcePlural string, args []string, atespace string, allAtespaces bool) error {
	if len(args) > 0 {
		if allAtespaces {
			return usageErrorf("-A/--all-atespaces cannot be used when getting %s by name", resourcePlural)
		}
		if atespace == "" {
			return usageErrorf("-a/--atespace is required when getting %s by name", resourcePlural)
		}
		return nil
	}
	if allAtespaces && atespace != "" {
		return usageErrorf("-a/--atespace and -A/--all-atespaces are mutually exclusive")
	}
	if !allAtespaces && atespace == "" {
		return usageErrorf("-a/--atespace <name> or -A/--all-atespaces is required")
	}
	return nil
}
