package oss

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

type LimitedReadCloser struct {
	*io.LimitedReader
	io.Closer
}

func NewLimitedReadCloser(rc io.ReadCloser, limit int64) io.ReadCloser {
	if limit < 0 {
		return rc
	}
	return &LimitedReadCloser{
		LimitedReader: &io.LimitedReader{R: rc, N: limit},
		Closer:        rc,
	}
}

func ReadSeekNopCloser(r io.Reader) ReadSeekerNopClose {
	return ReadSeekerNopClose{r}
}

type ReadSeekerNopClose struct {
	r io.Reader
}

func (r ReadSeekerNopClose) Read(p []byte) (int, error) {
	switch t := r.r.(type) {
	case io.Reader:
		return t.Read(p)
	}
	return 0, nil
}

func (r ReadSeekerNopClose) Seek(offset int64, whence int) (int64, error) {
	switch t := r.r.(type) {
	case io.Seeker:
		return t.Seek(offset, whence)
	}
	return int64(0), nil
}

func (r ReadSeekerNopClose) Close() error {
	return nil
}

func (r ReadSeekerNopClose) IsSeeker() bool {
	_, ok := r.r.(io.Seeker)
	return ok
}

func (r ReadSeekerNopClose) HasLen() (int, bool) {
	type lenner interface {
		Len() int
	}

	if lr, ok := r.r.(lenner); ok {
		return lr.Len(), true
	}

	return 0, false
}

func (r ReadSeekerNopClose) GetLen() (int64, error) {
	if l, ok := r.HasLen(); ok {
		return int64(l), nil
	}

	if s, ok := r.r.(io.Seeker); ok {
		return seekerLen(s)
	}

	return -1, nil
}

func seekerLen(s io.Seeker) (int64, error) {
	curOffset, err := s.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	endOffset, err := s.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	_, err = s.Seek(curOffset, io.SeekStart)
	if err != nil {
		return 0, err
	}

	return endOffset - curOffset, nil
}

func isReaderSeekable(r io.Reader) bool {
	switch v := r.(type) {
	case ReadSeekerNopClose:
		return v.IsSeeker()
	case *ReadSeekerNopClose:
		return v.IsSeeker()
	case io.ReadSeeker:
		return true
	default:
		return false
	}
}

func GetReaderLen(r io.Reader) int64 {
	type lenner interface {
		Len() int
	}

	if lr, ok := r.(lenner); ok {
		return int64(lr.Len())
	}

	if s, ok := r.(io.Seeker); ok {
		if l, err := seekerLen(s); err == nil {
			return l
		}
	}

	return -1
}

type buffer struct {
	buf    []byte
	err    error
	offset int
}

func (b *buffer) isEmpty() bool {
	if b == nil {
		return true
	}
	if len(b.buf)-b.offset <= 0 {
		return true
	}
	return false
}

func (b *buffer) read(rd io.Reader) error {
	var n int
	n, b.err = readFill(rd, b.buf)
	b.buf = b.buf[0:n]
	b.offset = 0
	return b.err
}

func (b *buffer) buffer() []byte {
	return b.buf[b.offset:]
}

func (b *buffer) increment(n int) {
	b.offset += n
}

const (
	AsyncReadeBufferSize = 1024 * 1024
	softStartInitial     = 512 * 1024
)

type ReaderRangeGetOutput struct {
	Body          io.ReadCloser
	ContentLength int64
	ContentRange  *string
	ETag          *string
	LastModified  *time.Time
}

type ReaderRangeGetFn func(context.Context, HTTPRange) (output *ReaderRangeGetOutput, err error)

type AsyncRangeReader struct {
	in      io.ReadCloser // Input reader
	ready   chan *buffer  // Buffers ready to be handed to the reader
	token   chan struct{} // Tokens which allow a buffer to be taken
	exit    chan struct{} // Closes when finished
	buffers int           // Number of buffers
	err     error         // If an error has occurred it is here
	cur     *buffer       // Current buffer being served
	exited  chan struct{} // Channel is closed been the async reader shuts down
	size    int           // size of buffer to use
	closed  bool          // whether we have closed the underlying stream
	mu      sync.Mutex    // lock for Read/WriteTo/Abandon/Close

	//Range Getter
	rangeGet  ReaderRangeGetFn
	httpRange HTTPRange

	// For reader
	offset  int64
	gotsize int64

	oriHttpRange HTTPRange

	context context.Context
	cancel  context.CancelFunc

	// Origin file pattern
	etag    string
	modTime string
}

// NewAsyncRangeReader returns a reader that will asynchronously read from
// the Reader returued by getter from the given offset into a number of buffers each of size AsyncReadeBufferSize
// The input can be read from the returned reader.
// When done use Close to release the buffers and close the supplied input.
// The etag is used to identify the content of the object. If not set, the first ETag returned value will be used instead.
func NewAsyncRangeReader(ctx context.Context,
	rangeGet ReaderRangeGetFn, httpRange *HTTPRange, etag string, buffers int) (*AsyncRangeReader, error) {

	if buffers <= 0 {
		return nil, errors.New("number of buffers too small")
	}
	if rangeGet == nil {
		return nil, errors.New("nil reader supplied")
	}

	context, cancel := context.WithCancel(ctx)

	range_ := HTTPRange{}
	if httpRange != nil {
		range_ = *httpRange
	}

	a := &AsyncRangeReader{
		rangeGet:     rangeGet,
		context:      context,
		cancel:       cancel,
		httpRange:    range_,
		oriHttpRange: range_,
		offset:       range_.Offset,
		gotsize:      0,
		etag:         etag,
		buffers:      buffers,
	}

	//fmt.Printf("NewAsyncRangeReader, range: %s, etag:%s, buffer count:%v\n", ToString(a.httpRange.FormatHTTPRange()), a.etag, a.buffers)

	a.init(buffers)
	return a, nil
}

func (a *AsyncRangeReader) init(buffers int) {
	a.ready = make(chan *buffer, buffers)
	a.token = make(chan struct{}, buffers)
	a.exit = make(chan struct{})
	a.exited = make(chan struct{})
	a.buffers = buffers
	a.cur = nil
	a.size = softStartInitial

	// Create tokens
	for i := 0; i < buffers; i++ {
		a.token <- struct{}{}
	}

	// Start async reader
	go func() {
		// Ensure that when we exit this is signalled.
		defer close(a.exited)
		defer close(a.ready)
		for {
			select {
			case <-a.token:
				b := a.getBuffer()
				if a.size < AsyncReadeBufferSize {
					b.buf = b.buf[:a.size]
					a.size <<= 1
				}

				if a.httpRange.Count > 0 && a.gotsize > a.httpRange.Count {
					b.buf = b.buf[0:0]
					b.err = io.EOF
					//fmt.Printf("a.gotsize > a.httpRange.Count, err:%v\n", b.err)
					a.ready <- b
					return
				}

				if a.in == nil {
					httpRangeRemains := a.httpRange
					if a.httpRange.Count > 0 {
						gotNum := a.httpRange.Offset - a.oriHttpRange.Offset
						if gotNum > 0 && a.httpRange.Count > gotNum {
							httpRangeRemains.Count = a.httpRange.Count - gotNum
						}
					}
					output, err := a.rangeGet(a.context, httpRangeRemains)
					if err == nil {
						etag := ToString(output.ETag)
						if a.etag == "" {
							a.etag = etag
						}
						if etag != a.etag {
							err = fmt.Errorf("Source file is changed, expect etag:%s ,got etag:%s", a.etag, etag)
						}

						// Partial Response check
						var off int64
						if output.ContentRange == nil {
							off = 0
						} else {
							off, _, _, _ = ParseContentRange(*output.ContentRange)
						}
						if off != httpRangeRemains.Offset {
							err = fmt.Errorf("Range get fail, expect offset:%v, got offset:%v", httpRangeRemains.Offset, off)
						}
					}
					if err != nil {
						b.buf = b.buf[0:0]
						b.err = err
						if output != nil && output.Body != nil {
							output.Body.Close()
						}
						//fmt.Printf("call getFunc fail, err:%v\n", err)
						a.ready <- b
						return
					}
					body := output.Body
					if httpRangeRemains.Count > 0 {
						body = NewLimitedReadCloser(output.Body, httpRangeRemains.Count)
					}
					a.in = body
					//fmt.Printf("call getFunc done, range:%s\n", ToString(httpRangeRemains.FormatHTTPRange()))
				}

				// ignore err from read
				err := b.read(a.in)
				a.httpRange.Offset += int64(len(b.buf))
				a.gotsize += int64(len(b.buf))
				if err != io.EOF {
					b.err = nil
				}
				//fmt.Printf("read into buffer, size:%v, next begin:%v, err:%v\n", len(b.buf), a.httpRange.Offset, err)
				a.ready <- b
				if err != nil {
					a.in.Close()
					a.in = nil
					if err == io.EOF {
						return
					}
				}
			case <-a.exit:
				return
			}
		}
	}()
}

func (a *AsyncRangeReader) fill() (err error) {
	if a.cur.isEmpty() {
		if a.cur != nil {
			a.putBuffer(a.cur)
			a.token <- struct{}{}
			a.cur = nil
		}
		b, ok := <-a.ready
		if !ok {
			// Return an error to show fill failed
			if a.err == nil {
				return errors.New("stream abandoned")
			}
			return a.err
		}
		a.cur = b
	}
	return nil
}

// Read will return the next available data.
func (a *AsyncRangeReader) Read(p []byte) (n int, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	defer func() {
		a.offset += int64(n)
	}()

	// Swap buffer and maybe return error
	err = a.fill()
	if err != nil {
		return 0, err
	}

	// Copy what we can
	n = copy(p, a.cur.buffer())
	a.cur.increment(n)

	// If at end of buffer, return any error, if present
	if a.cur.isEmpty() {
		a.err = a.cur.err
		return n, a.err
	}
	return n, nil
}

func (a *AsyncRangeReader) Offset() int64 {
	return a.offset
}

func (a *AsyncRangeReader) Close() (err error) {
	a.abandon()
	if a.closed {
		return nil
	}
	a.closed = true

	if a.in != nil {
		err = a.in.Close()
	}
	return
}

func (a *AsyncRangeReader) abandon() {
	a.stop()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cur != nil {
		a.putBuffer(a.cur)
		a.cur = nil
	}
	for b := range a.ready {
		a.putBuffer(b)
	}
}

func (a *AsyncRangeReader) stop() {
	select {
	case <-a.exit:
		return
	default:
	}
	a.cancel()
	close(a.exit)
	<-a.exited
}

// bufferPool is a global pool of buffers
var bufferPool *sync.Pool
var bufferPoolOnce sync.Once

// TODO use pool
func (a *AsyncRangeReader) putBuffer(b *buffer) {
	b.buf = b.buf[0:cap(b.buf)]
	bufferPool.Put(b.buf)
}

func (a *AsyncRangeReader) getBuffer() *buffer {
	bufferPoolOnce.Do(func() {
		// Initialise the buffer pool when used
		bufferPool = &sync.Pool{
			New: func() any {
				//fmt.Printf("make([]byte, BufferSize)\n")
				return make([]byte, AsyncReadeBufferSize)
			},
		}
	})
	return &buffer{
		buf: bufferPool.Get().([]byte),
	}
}

func readFill(r io.Reader, buf []byte) (n int, err error) {
	var nn int
	for n < len(buf) && err == nil {
		nn, err = r.Read(buf[n:])
		n += nn
	}
	return n, err
}

// MultiBytesReader A Reader implements the io.Reader, io.Seeker interfaces by reading from multi byte slice.
type MultiBytesReader struct {
	s    [][]byte
	i    int64 // current reading index
	size int64
	rbuf int
	rp   int
}

// Len returns the number of bytes of the unread portion of the slice.
func (r *MultiBytesReader) Len() int {
	if r.i >= r.size {
		return 0
	}
	return int(r.size - r.i)
}

// Size returns the original length of the underlying byte slice.
func (r *MultiBytesReader) Size() int64 { return r.size }

// Read implements the io.Reader interface.
func (r *MultiBytesReader) Read(b []byte) (n int, err error) {
	if r.i >= r.size {
		return 0, io.EOF
	}

	var nn int
	for n < len(b) && err == nil {
		nn, err = r.read(b[n:])
		n += nn
	}

	if err == io.EOF {
		err = nil
	}

	return n, err
}

func (r *MultiBytesReader) read(b []byte) (n int, err error) {
	if r.i >= r.size {
		return 0, io.EOF
	}

	//if r.rp == cap(r.s[r.rbuf]) {
	if r.rp == len(r.s[r.rbuf]) {
		r.rbuf++
		r.rp = 0
	}

	if r.rbuf == len(r.s) {
		err = io.EOF
		return
	} else if r.rbuf > len(r.s) {
		return 0, fmt.Errorf("read overflow, rbuf:%d, buf len%d", r.rbuf, len(r.s))
	}

	n = copy(b, r.s[r.rbuf][r.rp:])
	r.rp += n
	r.i += int64(n)

	return
}

// Seek implements the io.Seeker interface.
func (r *MultiBytesReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.i + offset
	case io.SeekEnd:
		abs = r.size + offset
	default:
		return 0, errors.New("MultiSliceReader.Seek: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("MultiSliceReader.Seek: negative position")
	}
	r.i = abs
	r.updateRp()
	return abs, nil
}

// Reset resets the Reader to be reading from b.
func (r *MultiBytesReader) Reset(b [][]byte) {
	n := MultiBytesReader{
		s: b,
		i: 0,
	}
	n.size = int64(r.calcSize(n.s))
	n.updateRp()
	*r = n
}

func (r *MultiBytesReader) calcSize(b [][]byte) int {
	size := 0
	for i := 0; i < len(b); i++ {
		size += len(r.s[i])
	}
	return size
}

func (r *MultiBytesReader) updateRp() {
	remains := r.i
	rbuf := 0
	for remains > 0 && rbuf < len(r.s) {
		slen := int64(len(r.s[rbuf]))
		if remains < slen {
			break
		}
		rbuf++
		remains -= slen
	}
	r.rbuf = rbuf
	r.rp = int(remains)
}

// NewReader returns a new Reader reading from b.
func NewMultiBytesReader(b [][]byte) *MultiBytesReader {
	r := &MultiBytesReader{
		s: b,
		i: 0,
	}
	r.size = int64(r.calcSize(r.s))
	r.updateRp()
	return r
}

type RangeReader struct {
	in     io.ReadCloser // Input reader
	closed bool          // whether we have closed the underlying stream

	//Range Getter
	rangeGet  ReaderRangeGetFn
	httpRange HTTPRange

	// For reader
	offset int64

	oriHttpRange HTTPRange

	context context.Context

	// Origin file pattern
	etag      string
	modTime   *time.Time
	totalSize int64
}

// NewRangeReader returns a reader that will read from the Reader returued by getter from the given offset.
// The etag is used to identify the content of the object. If not set, the first ETag returned value will be used instead.
func NewRangeReader(ctx context.Context, rangeGet ReaderRangeGetFn, httpRange *HTTPRange, etag string) (*RangeReader, error) {
	if rangeGet == nil {
		return nil, errors.New("nil reader supplied")
	}

	range_ := HTTPRange{}
	if httpRange != nil {
		range_ = *httpRange
	}

	a := &RangeReader{
		rangeGet:     rangeGet,
		context:      ctx,
		httpRange:    range_,
		oriHttpRange: range_,
		offset:       range_.Offset,
		etag:         etag,
	}

	//fmt.Printf("NewRangeReader, range: %s, etag:%s\n", ToString(a.httpRange.FormatHTTPRange()), a.etag)

	return a, nil
}

// Read will return the next available data.
func (r *RangeReader) Read(p []byte) (n int, err error) {
	defer func() {
		r.offset += int64(n)
		r.httpRange.Offset += int64(n)
	}()
	n, err = r.read(p)
	return
}

func (r *RangeReader) read(p []byte) (int, error) {
	if r.closed {
		return 0, fmt.Errorf("RangeReader is closed")
	}

	// open stream
	if r.in == nil {
		httpRangeRemains := r.httpRange
		if r.httpRange.Count > 0 {
			gotNum := r.httpRange.Offset - r.oriHttpRange.Offset
			if gotNum > 0 && r.httpRange.Count > gotNum {
				httpRangeRemains.Count = r.httpRange.Count - gotNum
			}
		}
		output, err := r.rangeGet(r.context, httpRangeRemains)
		if err == nil {
			etag := ToString(output.ETag)
			if r.etag == "" {
				r.etag = etag
				r.modTime = output.LastModified
			}
			if etag != r.etag {
				err = fmt.Errorf("Source file is changed, expect etag:%s ,got etag:%s", r.etag, etag)
			}

			// Partial Response check
			var off int64
			if output.ContentRange == nil {
				off = 0
				r.totalSize = output.ContentLength
			} else {
				off, _, r.totalSize, _ = ParseContentRange(*output.ContentRange)
			}
			if off != httpRangeRemains.Offset {
				err = fmt.Errorf("Range get fail, expect offset:%v, got offset:%v", httpRangeRemains.Offset, off)
			}
		}
		if err != nil {
			if output != nil && output.Body != nil {
				output.Body.Close()
			}
			return 0, err
		}
		body := output.Body
		if httpRangeRemains.Count > 0 {
			body = NewLimitedReadCloser(output.Body, httpRangeRemains.Count)
		}
		r.in = body
	}

	// read from stream
	// ignore error when reading from stream
	n, err := r.in.Read(p)
	if err != nil && err != io.EOF {
		r.in.Close()
		r.in = nil
		err = nil
	}

	return n, err
}

func (r *RangeReader) Offset() int64 {
	return r.offset
}

func (r *RangeReader) Close() (err error) {
	if r.closed {
		return nil
	}
	r.closed = true

	if r.in != nil {
		err = r.in.Close()
	}
	return
}

// TeeReadNopCloser returns a Reader that writes to w what it reads from r.
// All reads from r performed through it are matched with
// corresponding writes to w. There is no internal buffering -
// the write must complete before the read completes.
// Any error encountered while writing is reported as a read error.
func TeeReadNopCloser(reader io.Reader, writers ...io.Writer) io.ReadCloser {
	return &teeReadNopCloser{
		reader:  reader,
		writers: writers,
		mark:    -1,
	}
}

type teeReadNopCloser struct {
	reader  io.Reader
	writers []io.Writer
	mark    int64
}

func (t *teeReadNopCloser) Read(p []byte) (n int, err error) {
	n, err = t.reader.Read(p)
	if n > 0 {
		for _, w := range t.writers {
			if nn, err := w.Write(p[:n]); err != nil {
				return nn, err
			}
		}
	}
	return
}

func (t *teeReadNopCloser) Seek(offset int64, whence int) (int64, error) {
	switch t := t.reader.(type) {
	case io.Seeker:
		return t.Seek(offset, whence)
	}
	return int64(0), nil
}

func (t *teeReadNopCloser) Close() error {
	return nil
}

// IsSeekable tests if this reader supports Seek method.
func (t *teeReadNopCloser) IsSeekable() bool {
	_, ok := t.reader.(io.Seeker)
	return ok
}

// MarkSupported tests if this reader supports the Mark and Reset methods.
func (t *teeReadNopCloser) MarkSupported() bool {
	return t.IsSeekable()
}

// Mark marks the current position in this reader. A subsequent call to
// the Reset method repositions this reader at the last marked position
// so that subsequent reads re-read the same bytes.
func (t *teeReadNopCloser) Mark() {
	if s, ok := t.reader.(io.Seeker); ok {
		if pos, err := s.Seek(0, io.SeekCurrent); err == nil {
			t.mark = pos
		}
	}
}

// Reset repositions this stream to the position at the time
// the Mark method was last called on this reader.
func (t *teeReadNopCloser) Reset() error {
	if !t.MarkSupported() {
		return fmt.Errorf("Mark/Reset not supported")
	}

	if t.mark < 0 {
		return fmt.Errorf("Mark is not called yet")
	}

	// seek to the last marked position
	if s, ok := t.reader.(io.Seeker); ok {
		if _, err := s.Seek(t.mark, io.SeekStart); err != nil {
			return err
		}
	}

	// reset writer
	type reseter interface {
		Reset()
	}

	for _, w := range t.writers {
		if rw, ok := w.(reseter); ok {
			rw.Reset()
		}
	}

	return nil
}

type DiscardReadCloser struct {
	RC      io.ReadCloser
	Discard int
}

func (drc *DiscardReadCloser) Read(b []byte) (int, error) {
	n, err := drc.RC.Read(b)
	if drc.Discard == 0 || n <= 0 {
		return n, err
	}

	if n <= drc.Discard {
		drc.Discard -= n
		return 0, err
	}

	realLen := n - drc.Discard
	copy(b[0:realLen], b[drc.Discard:n])
	drc.Discard = 0
	return realLen, err
}

func (drc *DiscardReadCloser) Close() error {
	closer, ok := drc.RC.(io.ReadCloser)
	if ok {
		return closer.Close()
	}
	return nil
}
