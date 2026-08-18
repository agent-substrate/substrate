package oss

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type ServiceError struct {
	XMLName   xml.Name `xml:"Error" json:"-"`
	Code      string   `xml:"Code" json:"Code"`
	Message   string   `xml:"Message" json:"Message"`
	RequestID string   `xml:"RequestId" json:"RequestId"`
	EC        string   `xml:"EC" json:"EC"`

	StatusCode    int
	Snapshot      []byte
	Timestamp     time.Time
	RequestTarget string
	Headers       http.Header
}

func (e *ServiceError) Error() string {
	return fmt.Sprintf(
		`Error returned by Service. 
Http Status Code: %d. 
Error Code: %s. 
Request Id: %s. 
Message: %s.
EC: %s.
Timestamp: %s.
Request Endpoint: %s.`,
		e.StatusCode, e.Code, e.RequestID, e.Message, e.EC, e.Timestamp, e.RequestTarget)
}

func (e *ServiceError) HttpStatusCode() int {
	return e.StatusCode
}

func (e *ServiceError) ErrorCode() string {
	return e.Code
}

type ClientError struct {
	Code    string
	Message string
	Err     error
}

func (e *ClientError) Unwrap() error { return e.Err }

func (e *ClientError) Error() string {
	return fmt.Sprintf("client error: %v, %v", e.Message, e.Err)
}

type OperationError struct {
	name string
	err  error
}

func (e *OperationError) Operation() string { return e.name }

func (e *OperationError) Unwrap() error { return e.err }

func (e *OperationError) Error() string {
	return fmt.Sprintf("operation error %s: %v", e.name, e.err)
}

type DeserializationError struct {
	Err      error
	Snapshot []byte
}

func (e *DeserializationError) Error() string {
	const msg = "deserialization failed"
	if e.Err == nil {
		return msg
	}
	return fmt.Sprintf("%s, %v", msg, e.Err)
}

func (e *DeserializationError) Unwrap() error { return e.Err }

type SerializationError struct {
	Err error
}

func (e *SerializationError) Error() string {
	const msg = "serialization failed"
	if e.Err == nil {
		return msg
	}
	return fmt.Sprintf("%s: %v", msg, e.Err)
}

func (e *SerializationError) Unwrap() error { return e.Err }

type CanceledError struct {
	Err error
}

func (*CanceledError) CanceledError() bool { return true }

func (e *CanceledError) Unwrap() error {
	return e.Err
}

func (e *CanceledError) Error() string {
	return fmt.Sprintf("canceled, %v", e.Err)
}

type InvalidParamError interface {
	error
	Field() string
	SetContext(string)
}

type invalidParamError struct {
	context string
	field   string
	reason  string
}

func (e invalidParamError) Error() string {
	return fmt.Sprintf("%s, %s.", e.reason, e.Field())
}

func (e invalidParamError) Field() string {
	sb := &strings.Builder{}
	sb.WriteString(e.context)
	if sb.Len() > 0 {
		sb.WriteRune('.')
	}
	sb.WriteString(e.field)
	return sb.String()
}

func (e *invalidParamError) SetContext(ctx string) {
	e.context = ctx
}

func NewErrParamRequired(field string) InvalidParamError {
	return &invalidParamError{
		field:  field,
		reason: fmt.Sprintf("missing required field"),
	}
}

func NewErrParamInvalid(field string) InvalidParamError {
	return &invalidParamError{
		field:  field,
		reason: fmt.Sprintf("invalid field"),
	}
}

func NewErrParamNull(field string) InvalidParamError {
	return &invalidParamError{
		field:  field,
		reason: fmt.Sprintf("null field"),
	}
}

func NewErrParamTypeNotSupport(field string) InvalidParamError {
	return &invalidParamError{
		field:  field,
		reason: fmt.Sprintf("type not support"),
	}
}
