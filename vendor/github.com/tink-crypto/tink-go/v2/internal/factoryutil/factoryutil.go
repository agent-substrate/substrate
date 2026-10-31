// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package factoryutil provides utility functions for Tink factories.
package factoryutil

import (
	"fmt"
	"iter"
	"reflect"
	"strings"

	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/internalregistry"
	"github.com/tink-crypto/tink-go/v2/internal/monitoringutil"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	"github.com/tink-crypto/tink-go/v2/internal/registryconfig/legacyprimitive"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"github.com/tink-crypto/tink-go/v2/monitoring"
)

func monitoringKeyStatusFromKeysetStatus(status keyset.KeyStatus) monitoring.KeyStatus {
	switch status {
	case keyset.Enabled:
		return monitoring.Enabled
	case keyset.Disabled:
		return monitoring.Disabled
	case keyset.Destroyed:
		return monitoring.Destroyed
	default:
		return monitoring.DoNotUse
	}
}

func stripTypeURLPrefix(s string) string {
	return strings.TrimPrefix(s, "type.googleapis.com/google.crypto.")
}

// EnabledUnmonitoredEntries returns an iterator over the enabled entries in a
// [keyset.Handle].
func EnabledUnmonitoredEntries(kh *keyset.Handle) iter.Seq[*keyset.Entry] {
	if kh == nil {
		return func(yield func(*keyset.Entry) bool) { /*do nothing*/ }
	}
	return func(yield func(*keyset.Entry) bool) {
		for i := 0; i < kh.Len(); i++ {
			entry, err := kh.Entry(i)
			if err != nil {
				panic(err) // Should never happen.
			}
			if entry.KeyStatus() != keyset.Enabled {
				continue
			}
			// Make sure this access doesn't get logged as key export.
			entry = entry.ToUnmonitoredEntry(internalapi.Token{})
			if !yield(entry) {
				return
			}
		}
	}
}

// keysetInfo returns the monitoring info for the given keyset handle.
func keysetInfo(kh *keyset.Handle) (*monitoring.KeysetInfo, error) {
	annotations := kh.Annotations(internalapi.Token{})
	if len(annotations) == 0 {
		// No annotations, so no monitoring info.
		return nil, nil
	}

	primaryKeyID := uint32(0)
	var entries []*monitoring.Entry
	for entry := range EnabledUnmonitoredEntries(kh) {
		if entry.IsPrimary() {
			primaryKeyID = entry.KeyID()
		}
		protoSerialization, err := protoserialization.SerializeKey(entry.Key())
		if err != nil {
			return nil, err
		}
		entries = append(entries, &monitoring.Entry{
			Status:    monitoringKeyStatusFromKeysetStatus(entry.KeyStatus()),
			KeyID:     entry.KeyID(),
			KeyType:   stripTypeURLPrefix(protoSerialization.KeyData().GetTypeUrl()),
			KeyPrefix: protoSerialization.OutputPrefixType().String(),
		})
	}
	return &monitoring.KeysetInfo{
		Annotations:  annotations,
		PrimaryKeyID: primaryKeyID,
		Entries:      entries,
	}, nil
}

// LoggerFactory is a factory for creating [monitoring.Loggers] for a
// keyset.
type LoggerFactory struct {
	client monitoring.Client
	info   *monitoring.KeysetInfo
}

// NewLoggerFactory creates a new [LoggerFactory] for the given [keyset.Handle].
func NewLoggerFactory(kh *keyset.Handle) (*LoggerFactory, error) {
	info, err := keysetInfo(kh)
	if err != nil {
		return nil, err
	}
	return &LoggerFactory{
		client: internalregistry.GetMonitoringClient(),
		info:   info,
	}, nil
}

// CreateFor creates a new [monitoring.Logger] for the given primitive and API
// function.
func (lb *LoggerFactory) CreateFor(primitive, apiFunction string) (monitoring.Logger, error) {
	if lb.info == nil || lb.client == nil {
		return &monitoringutil.DoNothingLogger{}, nil
	}
	return lb.client.NewLogger(&monitoring.Context{
		Primitive:   primitive,
		APIFunction: apiFunction,
		KeysetInfo:  lb.info,
	})
}

// PrimitiveFromKey returns the primitive of type T from the given key and config.
//
// On success, it also returns whether the primitive is a legacy "non-full"
// primitive.
//
// k and config must not be nil.
func PrimitiveFromKey[T any](k key.Key, config keyset.Config) (T, bool, error) {
	primitive, err := config.PrimitiveFromKey(k, internalapi.Token{})
	if err != nil {
		var zero T
		return zero, false, err
	}
	isLegacy := false
	if legacyPrimitive, ok := primitive.(legacyprimitive.LegacyPrimitive); ok {
		primitive = legacyPrimitive.Primitive()
		isLegacy = true
	}
	result, ok := primitive.(T)
	if !ok {
		var zero T
		return zero, isLegacy, fmt.Errorf("factoryutil: primitive is not a %v", reflect.TypeFor[T]())
	}
	return result, isLegacy, nil
}

type withOutputPrefix interface {
	OutputPrefix() []byte
}

// OutputPrefix returns the output prefix of the given key.
//
// It leverages the fact that almost all [key.Key] implementations in Tink have
// an OutputPrefix() method.
//
// Returns and error if the key does not have an OutputPrefix() method.
func OutputPrefix(k key.Key) ([]byte, error) {
	v, ok := k.(withOutputPrefix)
	if !ok {
		return nil, fmt.Errorf("factoryutil: key does not have an OutputPrefix() method")
	}
	return v.OutputPrefix(), nil
}
