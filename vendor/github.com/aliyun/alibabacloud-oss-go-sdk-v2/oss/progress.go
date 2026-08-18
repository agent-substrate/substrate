package oss

import "io"

type ProgressFunc func(increment, transferred, total int64)

type progressTracker struct {
	pr       ProgressFunc
	written  int64
	lwritten int64 // last written
	total    int64
}

// NewProgress NewRequestProgress creates a tracker with progress reporting
func NewProgress(pr ProgressFunc, total int64) io.Writer {
	return &progressTracker{
		pr:       pr,
		written:  0,
		lwritten: 0,
		total:    total,
	}
}

func (p *progressTracker) Write(b []byte) (n int, err error) {
	n = len(b)
	p.written += int64(n)

	// Invokes the user's callback method to report progress
	if p.written > p.lwritten {
		p.pr(int64(n), p.written, p.total)
	}

	return
}

func (p *progressTracker) Reset() {
	p.lwritten = p.written
	p.written = 0
}

var _ RequestBodyTracker = (*progressTracker)(nil)
