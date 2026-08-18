package oss

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/arn"
)

func isValidRegion(region string) bool {
	for _, v := range region {
		if !(('a' <= v && v <= 'z') || ('0' <= v && v <= '9') || v == '-') {
			return false
		}
	}
	return region != ""
}

func isValidEndpoint(endpoint *url.URL) bool {
	return (endpoint != nil)
}

func isValidBucketName(bucketName *string) bool {
	if bucketName == nil {
		return false
	}

	nameLen := len(*bucketName)
	if nameLen < 3 || nameLen > 63 {
		return false
	}

	if (*bucketName)[0] == '-' || (*bucketName)[nameLen-1] == '-' {
		return false
	}

	for _, v := range *bucketName {
		if !(('a' <= v && v <= 'z') || ('0' <= v && v <= '9') || v == '-') {
			return false
		}
	}
	return true
}

func isValidObjectName(objectName *string) bool {
	if objectName == nil || len(*objectName) == 0 {
		return false
	}
	return true
}

func isValidRange(r *string) bool {
	if _, err := ParseRange(*r); err != nil {
		return false
	}
	return true
}

var supportedMethod = map[string]struct{}{
	"GET":     {},
	"PUT":     {},
	"HEAD":    {},
	"POST":    {},
	"DELETE":  {},
	"OPTIONS": {},
}

func isValidMethod(method string) bool {
	if _, ok := supportedMethod[method]; ok {
		return true
	}
	return false
}

var supportedCopyDirective = map[string]struct{}{
	"COPY":    {},
	"REPLACE": {},
}

func isValidCopyDirective(value string) bool {
	upper := strings.ToUpper(value)
	if _, ok := supportedCopyDirective[upper]; ok {
		return true
	}
	return false
}

// Exposed to external modules
func IsValidRegion(region string) bool {
	return isValidRegion(region)
}

func IsValidBucketName(bucketName *string) bool {
	return isValidBucketName(bucketName)
}

func IsValidMethod(method string) bool {
	return isValidMethod(method)
}

func AssertValidateArnBucket(bucket string) error {
	parsedArn, err := arn.ParseArn(bucket)
	if err != nil {
		return err
	}

	// must have account id
	if parsedArn.AccountId() == nil || *parsedArn.AccountId() == "" {
		return errors.New("OperationInput.bucket does not contain account id")
	}

	// must have bucket resource
	resource := parsedArn.Resource()
	resourceType := ""
	if resource.ResourceType() != nil {
		resourceType = *resource.ResourceType()
	}

	qualifier := ""
	if resource.Qualifier() != nil {
		qualifier = *resource.Qualifier()
	}

	if resourceType != "bucket" ||
		strings.TrimSpace(resource.Resource()) == "" ||
		qualifier != "" {
		return fmt.Errorf("operationInput.bucket is not bucket arn, got %s", bucket)
	}

	// check bucket value
	if !isValidBucketName(Ptr(resource.Resource())) {
		return fmt.Errorf("bucket resource is invalid, got %s", bucket)
	}

	return nil
}
