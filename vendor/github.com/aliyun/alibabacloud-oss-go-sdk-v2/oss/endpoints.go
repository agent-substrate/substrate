package oss

import (
	"fmt"
	"regexp"
)

type EndpointType int

const (
	// Access OSS over the public network, oss-[region].aliyuncs.com
	EndpointPublic EndpointType = iota

	// Access OSS over the internal network, oss-[region]-internal.aliyuncs.com
	EndpointInternal

	// Access OSS over the global acceleration endpoint, oss-accelerate.aliyuncs.com
	EndpointAccelerate

	// Access OSS over the acceleration endpoint outside the Chinese mainland, oss-accelerate-overseas.aliyuncs.com
	EndpointAccelerateOverseas

	// Access OSS over the dual stack endpoint that support both IPv4 and IPv6, [region].oss.aliyuncs.com
	EndpointDualStack
)

var schemeRE = regexp.MustCompile("^([^:]+)://")

func addEndpointScheme(endpoint string, disableSSL bool) string {
	if endpoint != "" && !schemeRE.MatchString(endpoint) {
		scheme := DefaultEndpointScheme
		if disableSSL {
			scheme = "http"
		}
		endpoint = fmt.Sprintf("%s://%s", scheme, endpoint)
	}
	return endpoint
}

func endpointFromRegion(region string, disableSSL bool, etype EndpointType) string {
	scheme := DefaultEndpointScheme
	if disableSSL {
		scheme = "http"
	}

	var endpoint string
	switch etype {
	case EndpointInternal:
		endpoint = fmt.Sprintf("oss-%s-internal.aliyuncs.com", region)
	case EndpointDualStack:
		endpoint = fmt.Sprintf("%s.oss.aliyuncs.com", region)
	case EndpointAccelerate:
		endpoint = "oss-accelerate.aliyuncs.com"
	case EndpointAccelerateOverseas:
		endpoint = "oss-accelerate-overseas.aliyuncs.com"
	default:
		endpoint = fmt.Sprintf("oss-%s.aliyuncs.com", region)
	}
	endpoint = fmt.Sprintf("%s://%s", scheme, endpoint)

	return endpoint
}

// A interface for an endpoint provider.
type EndpointProvider interface {
	BuildURL(input *OperationInput) string
}
