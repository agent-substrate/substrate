package apiconfig

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateconfigpb"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

type Loader struct {
	configFile string

	// lock covers nextLoad and config
	lock       sync.Mutex
	nextLoad   time.Time
	config     *ateconfigpb.APIConfig
	configHash [32]byte
}

func NewLoader(configFile string) (*Loader, error) {
	l := &Loader{
		configFile: configFile,
	}
	l.lock.Lock()
	defer l.lock.Unlock()

	if err := l.refreshIfNecessary(); err != nil {
		return nil, fmt.Errorf("while loading config: %w", err)
	}

	return l, nil
}

// Precondition: l.lock() is locked
func (l *Loader) refreshIfNecessary() error {
	if l.config != nil && time.Now().Before(l.nextLoad) {
		return nil
	}

	configBytes, err := os.ReadFile(l.configFile)
	if err != nil {
		return fmt.Errorf("while reading config file: %w", err)
	}

	config := &ateconfigpb.APIConfig{}
	switch ext := filepath.Ext(l.configFile); ext {
	case "pb":
		if err := proto.Unmarshal(configBytes, config); err != nil {
			return fmt.Errorf("while unmarshaling protobuf: %w", err)
		}
	case "textproto", "textpb":
		if err := prototext.Unmarshal(configBytes, config); err != nil {
			return fmt.Errorf("while unmarshaling prototext: %w", err)
		}
	default:
		return fmt.Errorf("unrecognized config extension %q", ext)
	}

	// TODO(unifiedconfig): Apply validation.

	l.config = config
	l.configHash = sha256.Sum256(configBytes)
	l.nextLoad = time.Now().Add(time.Minute)
	return nil
}

// Config returns the currently-active server config, and a hash of the config.
//
// Subsystems that consume the config should call the Config() each time they
// need to read a value.  If they need to build local memoized structures (like
// making a hashtable out of a list), they should save the hash at which they
// built the memoized structure and treat a changed hash as the trigger to
// recompute.
//
// The returned config object MUST be treated as read-only.
func (l *Loader) Config(ctx context.Context) (*ateconfigpb.APIConfig, [32]byte) {
	l.lock.Lock()
	defer l.lock.Unlock()

	if err := l.refreshIfNecessary(); err != nil {
		slog.ErrorContext(ctx, "Failed to refresh config, continuing with stale config: %w", err)
	}

	return l.config, l.configHash
}
