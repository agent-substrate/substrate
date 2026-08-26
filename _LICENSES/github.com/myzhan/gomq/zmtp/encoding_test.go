package zmtp

import (
	"bytes"
	"testing"
)

func TestToNullPaddedString(t *testing.T) {
	b := make([]byte, 5)
	err := toNullPaddedString("hello", b)
	if err != nil {
		t.Error(err)
	}

	if want, got := 0, bytes.Compare([]byte("hello"), b); want != got {
		t.Errorf("want %v, got %v", want, got)
	}

	b = make([]byte, 1)
	err = toNullPaddedString("hello", b)
	if err == nil {
		t.Errorf("should have error and do not")
	}
}

func TestFromNullPaddedString(t *testing.T) {
	s := fromNullPaddedString([]byte("hello"))

	if want, got := "hello", s; want != got {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestToByteBool(t *testing.T) {
	b := toByteBool(true)

	if want, got := byte(1), b; want != got {
		t.Errorf("want %v, got %v", want, got)
	}

	b = toByteBool(false)

	if want, got := byte(0), b; want != got {
		t.Errorf("want %v, got %v", want, got)
	}
}

func TestFromByteBool(t *testing.T) {
	b, err := fromByteBool(byte(0))
	if err != nil {
		t.Error(err)
	}
	if want, got := false, b; want != got {
		t.Errorf("want %v, got %v", want, got)
	}

	b, err = fromByteBool(byte(1))
	if err != nil {
		t.Error(err)
	}
	if want, got := true, b; want != got {
		t.Errorf("want %v, got %v", want, got)
	}

	b, err = fromByteBool(byte('a'))
	if err == nil {
		t.Errorf("should have error and do not")
	}

}
