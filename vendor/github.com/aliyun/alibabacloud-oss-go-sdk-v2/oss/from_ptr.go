package oss

import "time"

// ToBool returns bool value if the pointer is not nil.
// Returns a bool zero value if the pointer is nil.
func ToBool(p *bool) (v bool) {
	if p == nil {
		return v
	}

	return *p
}

// ToInt returns int value if the pointer is not nil.
// Returns a int zero value if the pointer is nil.
func ToInt(p *int) (v int) {
	if p == nil {
		return v
	}

	return *p
}

// ToInt64 returns int value if the pointer is not nil.
// Returns a int64 zero value if the pointer is nil.
func ToInt64(p *int64) (v int64) {
	if p == nil {
		return v
	}

	return *p
}

// ToString returns bool value if the pointer is not nil.
// Returns a string zero value if the pointer is nil.
func ToString(p *string) (v string) {
	if p == nil {
		return v
	}

	return *p
}

// ToTime returns time.Time value if the pointer is not nil.
// Returns a time.Time  zero value if the pointer is nil.
func ToTime(p *time.Time) (v time.Time) {
	if p == nil {
		return v
	}

	return *p
}

// ToDuration returns time.Duration value if the pointer is not nil.
// Returns a time.Duration  zero value if the pointer is nil.
func ToDuration(p *time.Duration) (v time.Duration) {
	if p == nil {
		return v
	}

	return *p
}
