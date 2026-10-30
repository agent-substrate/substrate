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

// Package upgradepolicy checks release transitions against the version skew
// policy.
package upgradepolicy

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/version"
)

// CheckUpgrade checks an upgrade from one final release to a later release.
func CheckUpgrade(from, to string) error {
	fromVersion, err := parseRelease("source", from)
	if err != nil {
		return err
	}
	toVersion, err := parseRelease("target", to)
	if err != nil {
		return err
	}

	if fromVersion.Major() < 1 || toVersion.Major() < 1 {
		return fmt.Errorf("the version skew policy applies from v1.0.0")
	}
	if fromVersion.Major() != toVersion.Major() {
		return fmt.Errorf("upgrade from %s to %s changes the major version", from, to)
	}
	if !toVersion.GreaterThan(fromVersion) {
		return fmt.Errorf("target release %s must be newer than source release %s", to, from)
	}
	if toVersion.Minor() > fromVersion.Minor() && toVersion.Minor()-fromVersion.Minor() > 1 {
		return fmt.Errorf("upgrade from %s to %s skips a minor release", from, to)
	}
	return nil
}

// CheckRollback checks a rollback to upgradeFrom, the release recorded before
// the upgrade started.
func CheckRollback(from, to, upgradeFrom string) error {
	targetVersion, err := parseRelease("target", to)
	if err != nil {
		return err
	}
	upgradeFromVersion, err := parseRelease("recorded source", upgradeFrom)
	if err != nil {
		return err
	}
	if !targetVersion.EqualTo(upgradeFromVersion) {
		return fmt.Errorf("rollback target %s does not match recorded source release %s", to, upgradeFrom)
	}
	if err := CheckUpgrade(upgradeFrom, from); err != nil {
		return fmt.Errorf("rollback from %s to %s is unsupported: %w", from, to, err)
	}
	return nil
}

func parseRelease(name, raw string) (*version.Version, error) {
	if raw == "" {
		return nil, fmt.Errorf("%s release is empty", name)
	}
	v, err := version.ParseSemantic(raw)
	if err != nil || raw != "v"+v.String() {
		return nil, fmt.Errorf("%s release %q must be a vMAJOR.MINOR.PATCH tag", name, raw)
	}
	if v.PreRelease() != "" || v.BuildMetadata() != "" {
		return nil, fmt.Errorf("%s release %q must be a final release", name, raw)
	}
	return v, nil
}
