package transport

import (
	"context"
	"net"
	"time"
)

// Dialer
type Dialer struct {
	net.Dialer
	// Read/Write timeout
	timeout   time.Duration
	postRead  []func(n int, err error)
	postWrite []func(n int, err error)
}

func newDialer(cfg *Config) *Dialer {
	dialer := &Dialer{
		Dialer: net.Dialer{
			Timeout:   *cfg.ConnectTimeout,
			KeepAlive: *cfg.KeepAliveTimeout,
		},
		timeout:   *cfg.ReadWriteTimeout,
		postRead:  cfg.PostRead,
		postWrite: cfg.PostWrite,
	}

	if cfg.BindAddr != nil {
		dialer.Dialer.LocalAddr = &net.TCPAddr{IP: cfg.BindAddr}
	}

	return dialer
}

func (d *Dialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

func (d *Dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	c, err := d.Dialer.DialContext(ctx, network, address)
	if err != nil {
		return c, err
	}

	timeout := d.timeout
	if u, ok := ctx.Value("OpReadWriteTimeout").(*time.Duration); ok {
		timeout = *u
	}

	t := &timeoutConn{
		Conn:    c,
		timeout: timeout,
		dialer:  d,
	}
	return t, t.nudgeDeadline()
}

// A net.Conn with Read/Write timeout and rate limiting,
type timeoutConn struct {
	net.Conn
	timeout time.Duration
	dialer  *Dialer
}

func (c *timeoutConn) nudgeDeadline() error {
	if c.timeout > 0 {
		return c.SetDeadline(time.Now().Add(c.timeout))
	}
	return nil
}

func (c *timeoutConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	for _, fn := range c.dialer.postRead {
		fn(n, err)
	}
	if err == nil && n > 0 && c.timeout > 0 {
		err = c.nudgeDeadline()
	}
	return n, err
}

func (c *timeoutConn) Write(b []byte) (n int, err error) {
	n, err = c.Conn.Write(b)
	for _, fn := range c.dialer.postWrite {
		fn(n, err)
	}
	if err == nil && n > 0 && c.timeout > 0 {
		err = c.nudgeDeadline()
	}
	return n, err
}
