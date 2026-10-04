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

package storesql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/pressly/goose/v3"
)

// MigrateToLatest applies provider's pending migrations and logs the versions
// before and after. database names the backend in errors and logs.
func MigrateToLatest(ctx context.Context, provider *goose.Provider, database string) (migrationErr error) {
	started := time.Now()
	current, latest, err := provider.GetVersions(ctx)
	if err != nil {
		return fmt.Errorf("get %s migration versions: %w", database, err)
	}
	starting := current

	applied := 0
	defer func() {
		attributes := []any{
			slog.String("database", database),
			slog.Int64("starting_version", starting),
			slog.Int64("current_version", current),
			slog.Int64("latest_version", latest),
			slog.Int("applied_migrations", applied),
			slog.Duration("duration", time.Since(started)),
		}
		if migrationErr != nil {
			attributes = append(attributes, slog.Any("err", migrationErr))
			slog.ErrorContext(ctx, "migrations failed", attributes...)
			return
		}
		slog.InfoContext(ctx, "migrations ready", attributes...)
	}()

	results, err := provider.Up(ctx)
	if err != nil {
		var partial *goose.PartialError
		if errors.As(err, &partial) {
			applied = len(partial.Applied)
		}
		applyErr := fmt.Errorf("apply %s migrations: %w", database, err)
		failedCurrent, _, versionErr := provider.GetVersions(ctx)
		if versionErr != nil {
			return errors.Join(applyErr, fmt.Errorf("get %s migration versions after a failure: %w", database, versionErr))
		}
		current = failedCurrent
		return applyErr
	}
	applied = len(results)
	current, _, err = provider.GetVersions(ctx)
	if err != nil {
		return fmt.Errorf("get %s migration versions after migration: %w", database, err)
	}
	return nil
}
