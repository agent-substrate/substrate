package oss

import (
	"bytes"
	"context"
	"encoding"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func init() {
	for i := 0; i < len(noEscape); i++ {
		noEscape[i] = (i >= 'A' && i <= 'Z') ||
			(i >= 'a' && i <= 'z') ||
			(i >= '0' && i <= '9') ||
			i == '-' ||
			i == '.' ||
			i == '_' ||
			i == '~'
	}

	defaultUserAgent = fmt.Sprintf("%s/%s (%s/%s/%s;%s)", SdkName, Version(), runtime.GOOS,
		"-", runtime.GOARCH, runtime.Version())
}

var defaultUserAgent string
var noEscape [256]bool

func sleepWithContext(ctx context.Context, dur time.Duration) error {
	t := time.NewTimer(dur)
	defer t.Stop()

	select {
	case <-t.C:
		break
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

// getNowSec returns Unix time, the number of seconds elapsed since January 1, 1970 UTC.
// gets the current time in Unix time, in seconds.
func getNowSec() int64 {
	return time.Now().Unix()
}

// getNowGMT gets the current time in GMT format.
func getNowGMT() string {
	return time.Now().UTC().Format(http.TimeFormat)
}

func escapePath(path string, encodeSep bool) string {
	var buf bytes.Buffer
	for i := 0; i < len(path); i++ {
		c := path[i]
		if noEscape[c] || (c == '/' && !encodeSep) {
			buf.WriteByte(c)
		} else {
			fmt.Fprintf(&buf, "%%%02X", c)
		}
	}
	return buf.String()
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Pointer:
		return v.IsNil()
	}
	return false
}

func setTimeReflectValue(dst reflect.Value, value time.Time) (err error) {
	dst0 := dst
	if dst.Kind() == reflect.Pointer {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		dst = dst.Elem()
	}
	if dst.CanAddr() {
		pv := dst.Addr()
		if pv.CanInterface() {
			if val, ok := pv.Interface().(encoding.TextUnmarshaler); ok {
				return val.UnmarshalText([]byte(value.Format(time.RFC3339)))
			}
		}
	}
	return errors.New("cannot unmarshal into " + dst0.Type().String())
}

func setReflectValue(dst reflect.Value, data string) (err error) {
	dst0 := dst
	src := []byte(data)

	if dst.Kind() == reflect.Pointer {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		dst = dst.Elem()
	}

	switch dst.Kind() {
	case reflect.Invalid:
	default:
		return errors.New("cannot unmarshal into " + dst0.Type().String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if len(src) == 0 {
			dst.SetInt(0)
			return nil
		}
		itmp, err := strconv.ParseInt(strings.TrimSpace(string(src)), 10, dst.Type().Bits())
		if err != nil {
			return err
		}
		dst.SetInt(itmp)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if len(src) == 0 {
			dst.SetUint(0)
			return nil
		}
		utmp, err := strconv.ParseUint(strings.TrimSpace(string(src)), 10, dst.Type().Bits())
		if err != nil {
			return err
		}
		dst.SetUint(utmp)
	case reflect.Bool:
		if len(src) == 0 {
			dst.SetBool(false)
			return nil
		}
		value, err := strconv.ParseBool(strings.TrimSpace(string(src)))
		if err != nil {
			return err
		}
		dst.SetBool(value)
	case reflect.String:
		dst.SetString(string(src))
	}
	return nil
}

func setMapStringReflectValue(dst reflect.Value, key any, data any) (err error) {
	dst0 := dst

	if dst.Kind() == reflect.Pointer {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		dst = dst.Elem()
	}

	switch dst.Kind() {
	case reflect.Invalid:
	default:
		return errors.New("cannot unmarshal into " + dst0.Type().String())
	case reflect.Map:
		if dst.IsNil() {
			dst.Set(reflect.MakeMap(dst.Type()))
		}
		mapValue := reflect.ValueOf(data)
		mapKey := reflect.ValueOf(key)
		dst.SetMapIndex(mapKey, mapValue)
	}
	return nil
}

func isContextError(ctx context.Context, perr *error) bool {
	if ctxErr := ctx.Err(); ctxErr != nil {
		if *perr == nil {
			*perr = ctxErr
		}
		return true
	}
	return false
}

func copySeekableBody(dst io.Writer, src io.ReadSeeker) (int64, error) {
	curPos, err := src.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	n, err := io.Copy(dst, src)
	if err != nil {
		return n, err
	}

	_, err = src.Seek(curPos, io.SeekStart)
	if err != nil {
		return n, err
	}

	return n, nil
}

func ParseOffsetAndSizeFromHeaders(headers http.Header) (offset, size int64) {
	return parseOffsetAndSizeFromHeaders(headers)
}

func parseOffsetAndSizeFromHeaders(headers http.Header) (offset, size int64) {
	size = -1
	var contentLength = headers.Get("Content-Length")
	if len(contentLength) != 0 {
		var err error
		if size, err = strconv.ParseInt(contentLength, 10, 64); err != nil {
			return 0, -1
		}
	}

	var contentRange = headers.Get("Content-Range")
	if len(contentRange) == 0 {
		return 0, size
	}

	if !strings.HasPrefix(contentRange, "bytes ") {
		return 0, -1
	}

	// start offset
	dash := strings.IndexRune(contentRange, '-')
	if dash < 0 {
		return 0, -1
	}
	ret, err := strconv.ParseInt(contentRange[6:dash], 10, 64)
	if err != nil {
		return 0, -1
	}
	offset = ret

	// total size
	slash := strings.IndexRune(contentRange, '/')
	if slash < 0 {
		return 0, -1
	}
	tsize := contentRange[slash+1:]
	if tsize != "*" {
		ret, err = strconv.ParseInt(contentRange[slash+1:], 10, 64)
		if err != nil {
			return 0, -1
		}
		size = ret
	}

	return offset, size
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

// ParseRange parses a ContentRange from a ContentRange: header.
// It only accepts bytes 22-33/42 and bytes 22-33/* format.
func ParseContentRange(s string) (from int64, to int64, total int64, err error) {
	if !strings.HasPrefix(s, "bytes ") {
		return from, to, total, errors.New("invalid content range")
	}

	slash := strings.IndexRune(s, '/')
	if slash < 0 {
		return from, to, total, errors.New("invalid content range")
	}

	dash := strings.IndexRune(s, '-')
	if dash < 0 {
		return from, to, total, errors.New("invalid content range")
	}

	if slash < dash {
		return from, to, total, errors.New("invalid content range")
	}

	// from
	ret, err := strconv.ParseInt(s[6:dash], 10, 64)
	if err != nil {
		return from, to, total, errors.New("invalid content range")
	}
	from = ret

	// to
	ret, err = strconv.ParseInt(s[dash+1:slash], 10, 64)
	if err != nil {
		return from, to, total, errors.New("invalid content range")
	}
	to = ret

	// total
	last := s[slash+1:]
	if last == "*" {
		total = -1
	} else {
		ret, err = strconv.ParseInt(s[slash+1:], 10, 64)
		if err != nil {
			return from, to, total, errors.New("invalid content range")
		}
		total = ret
	}

	return from, to, total, nil
}

// ParseRange parses a HTTPRange from a Range: header.
// It only accepts single ranges.
func ParseRange(s string) (r *HTTPRange, err error) {
	const preamble = "bytes="
	if !strings.HasPrefix(s, preamble) {
		return nil, errors.New("range: header invalid: doesn't start with " + preamble)
	}
	s = s[len(preamble):]
	if strings.ContainsRune(s, ',') {
		return nil, errors.New("range: header invalid: contains multiple ranges which isn't supported")
	}
	dash := strings.IndexRune(s, '-')
	if dash < 0 {
		return nil, errors.New("range: header invalid: contains no '-'")
	}
	start, end := strings.TrimSpace(s[:dash]), strings.TrimSpace(s[dash+1:])
	o := HTTPRange{Offset: 0, Count: 0}
	if start != "" {
		o.Offset, err = strconv.ParseInt(start, 10, 64)
		if err != nil || o.Offset < 0 {
			return nil, errors.New("range: header invalid: bad start")
		}
	}
	if end != "" {
		e, err := strconv.ParseInt(end, 10, 64)
		if err != nil || e < 0 {
			return nil, errors.New("range: header invalid: bad end")
		}
		o.Count = e - o.Offset + 1
	}
	return &o, nil
}

// FileExists returns whether the given file exists or not
func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return (info != nil && !info.IsDir())
}

// DirExists returns whether the given directory exists or not
func DirExists(dir string) bool {
	info, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return false
	}
	return (info != nil && info.IsDir())
}

// EmptyFile changes the size of the named file to zero.
func EmptyFile(filename string) bool {
	err := os.Truncate(filename, 0)
	return err == nil
}

// Exposed to external modules
func IsEmptyValue(v reflect.Value) bool {
	return isEmptyValue(v)
}

func EscapePath(path string, encodeSep bool) string {
	return escapePath(path, encodeSep)
}

func extractStatusCode(status string) string {
	parts := strings.SplitN(status, " ", 2)
	if len(parts) > 1 {
		return parts[1]
	}
	return ""
}
