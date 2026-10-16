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

package apiconfig

import (
	"fmt"
	"os"
	"path/filepath"

	"go.ate.dev/substrate/pkg/proto/ateconfigpb"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

func LoadConfig(configFile string) (*ateconfigpb.APIConfig, error) {
	configBytes, err := os.ReadFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("while reading config file: %w", err)
	}

	config := &ateconfigpb.APIConfig{}
	switch ext := filepath.Ext(configFile); ext {
	case ".pb":
		if err := proto.Unmarshal(configBytes, config); err != nil {
			return nil, fmt.Errorf("while unmarshaling protobuf: %w", err)
		}
	case ".textproto", ".textpb":
		if err := prototext.Unmarshal(configBytes, config); err != nil {
			return nil, fmt.Errorf("while unmarshaling prototext: %w", err)
		}
	default:
		return nil, fmt.Errorf("unrecognized config extension %q", ext)
	}

	// Validate log level
	switch config.GetLogging().GetLevel() {
	case "debug", "info", "warn", "error":
		// Do nothing
	default:
		return nil, fmt.Errorf("logging.level: invalid value %q", config.GetLogging().GetLevel())
	}

	return config, nil
}
