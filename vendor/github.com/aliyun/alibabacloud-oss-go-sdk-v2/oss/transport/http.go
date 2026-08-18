package transport

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"time"
)

// Defaults for the Transport
var (
	DefaultConnectTimeout        = 5 * time.Second
	DefaultReadWriteTimeout      = 10 * time.Second
	DefaultIdleConnectionTimeout = 50 * time.Second
	DefaultExpectContinueTimeout = 1 * time.Second
	DefaultKeepAliveTimeout      = 30 * time.Second

	DefaultMaxConnections      = 100
	DefaultMaxIdleConnsPerHost = 10

	// Default to TLS 1.2 for all HTTPS requests.
	DefaultTLSMinVersion uint16 = tls.VersionTLS12
)

var DefaultConfig = Config{
	ConnectTimeout:        &DefaultConnectTimeout,
	ReadWriteTimeout:      &DefaultReadWriteTimeout,
	IdleConnectionTimeout: &DefaultIdleConnectionTimeout,
	KeepAliveTimeout:      &DefaultKeepAliveTimeout,
}

type Config struct {
	ConnectTimeout        *time.Duration
	ReadWriteTimeout      *time.Duration
	IdleConnectionTimeout *time.Duration
	KeepAliveTimeout      *time.Duration
	EnabledRedirect       *bool
	BindAddr              net.IP

	PostRead  []func(n int, err error)
	PostWrite []func(n int, err error)
}

func newTransportCustom(cfg *Config, fns ...func(*http.Transport)) http.RoundTripper {
	tr := &http.Transport{
		DialContext:           newDialer(cfg).DialContext,
		TLSHandshakeTimeout:   *cfg.ConnectTimeout,
		IdleConnTimeout:       *cfg.IdleConnectionTimeout,
		MaxConnsPerHost:       DefaultMaxConnections,
		MaxIdleConnsPerHost:   DefaultMaxIdleConnsPerHost,
		ExpectContinueTimeout: DefaultExpectContinueTimeout,
		TLSClientConfig: &tls.Config{
			MinVersion: DefaultTLSMinVersion,
		},
	}

	for _, fn := range fns {
		fn(tr)
	}

	return tr
}

func (c *Config) mergeIn(cfgs ...*Config) {
	for _, other := range cfgs {
		mergeInConfig(c, other)
	}
}

func (c *Config) copy(cfgs ...*Config) *Config {
	dst := &Config{}
	dst.mergeIn(c)

	for _, cfg := range cfgs {
		dst.mergeIn(cfg)
	}

	return dst
}

func mergeInConfig(dst *Config, other *Config) {
	if other == nil {
		return
	}

	if other.ConnectTimeout != nil {
		dst.ConnectTimeout = other.ConnectTimeout
	}

	if other.ReadWriteTimeout != nil {
		dst.ReadWriteTimeout = other.ReadWriteTimeout
	}

	if other.IdleConnectionTimeout != nil {
		dst.IdleConnectionTimeout = other.IdleConnectionTimeout
	}

	if other.KeepAliveTimeout != nil {
		dst.KeepAliveTimeout = other.KeepAliveTimeout
	}

	if other.EnabledRedirect != nil {
		dst.EnabledRedirect = other.EnabledRedirect
	}

	if other.BindAddr != nil {
		dst.BindAddr = other.BindAddr
	}

	if other.PostRead != nil {
		dst.PostRead = make([]func(n int, err error), len(other.PostRead))
		copy(dst.PostRead, other.PostRead)
	}

	if other.PostWrite != nil {
		dst.PostWrite = make([]func(n int, err error), len(other.PostWrite))
		copy(dst.PostWrite, other.PostWrite)
	}
}

func InsecureSkipVerify(enabled bool) func(*http.Transport) {
	return func(t *http.Transport) {
		if t.TLSClientConfig != nil {
			t.TLSClientConfig.InsecureSkipVerify = enabled
		} else {
			t.TLSClientConfig = &tls.Config{
				InsecureSkipVerify: enabled,
			}
		}
	}
}

func MaxConnections(value int) func(*http.Transport) {
	return func(t *http.Transport) {
		t.MaxConnsPerHost = value
	}
}

func ExpectContinueTimeout(value time.Duration) func(*http.Transport) {
	return func(t *http.Transport) {
		t.ExpectContinueTimeout = value
	}
}

func TLSMinVersion(value int) func(*http.Transport) {
	return func(t *http.Transport) {
		if t.TLSClientConfig != nil {
			t.TLSClientConfig.MinVersion = uint16(value)
		} else {
			t.TLSClientConfig = &tls.Config{
				MinVersion: uint16(value),
			}
		}
	}
}

func HttpProxy(fixedURL *url.URL) func(*http.Transport) {
	return func(t *http.Transport) {
		t.Proxy = http.ProxyURL(fixedURL)
	}
}

func ProxyFromEnvironment() func(*http.Transport) {
	return func(t *http.Transport) {
		t.Proxy = http.ProxyFromEnvironment
	}
}

func NewHttpClient(cfg *Config, fns ...func(*http.Transport)) *http.Client {
	cfg = DefaultConfig.copy(cfg)
	client := &http.Client{
		Transport: newTransportCustom(cfg, fns...),
		//Disalbe Redirect
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	if cfg.EnabledRedirect != nil && *cfg.EnabledRedirect {
		client.CheckRedirect = nil
	}

	return client
}
