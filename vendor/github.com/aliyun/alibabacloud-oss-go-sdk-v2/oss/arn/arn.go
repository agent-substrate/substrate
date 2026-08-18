package arn

import (
	"errors"
	"fmt"
	"strings"
)

// Arn represents an Alibaba Cloud Resource Name (ARN).
type Arn struct {
	service     string
	region      *string
	accountId   *string
	resource    string
	arnResource *ArnResource
}

// ArnBuilder is used to build an Arn instance.
type ArnBuilder struct {
	service   string
	region    *string
	accountId *string
	resource  string
}

// Service returns the service namespace that identifies the product.
func (a *Arn) Service() string {
	return a.service
}

// Region returns the Region that the resource resides in.
// Returns nil if region is empty.
func (a *Arn) Region() *string {
	return a.region
}

// AccountId returns the ID of the account that owns the resource.
// Returns nil if accountId is empty.
func (a *Arn) AccountId() *string {
	return a.accountId
}

// Resource returns the ArnResource.
func (a *Arn) Resource() *ArnResource {
	return a.arnResource
}

// ResourceAsString returns the resource as string.
func (a *Arn) ResourceAsString() string {
	return a.resource
}

// ArnBuilderNew Builder creates a new ArnBuilder.
func ArnBuilderNew() *ArnBuilder {
	return &ArnBuilder{}
}

// Service sets the service for the builder.
func (b *ArnBuilder) Service(service string) *ArnBuilder {
	b.service = service
	return b
}

// Region sets the region for the builder.
func (b *ArnBuilder) Region(region *string) *ArnBuilder {
	b.region = region
	return b
}

// AccountId sets the accountId for the builder.
func (b *ArnBuilder) AccountId(accountId *string) *ArnBuilder {
	b.accountId = accountId
	return b
}

// Resource sets the resource for the builder.
func (b *ArnBuilder) Resource(resource string) *ArnBuilder {
	b.resource = resource
	return b
}

// Build creates an Arn from the builder.
func (b *ArnBuilder) Build() (*Arn, error) {
	if strings.TrimSpace(b.service) == "" {
		return nil, errors.New("service must not be blank")
	}
	if strings.TrimSpace(b.resource) == "" {
		return nil, errors.New("resource must not be blank")
	}

	arnResource, err := ArnResourceFromString(b.resource)
	if err != nil {
		return nil, err
	}

	return &Arn{
		service:     b.service,
		region:      b.region,
		accountId:   b.accountId,
		resource:    b.resource,
		arnResource: arnResource,
	}, nil
}

// TryParseArn attempts to parse an ARN string.
// Returns (nil, nil) if parsing fails without throwing an error.
func TryParseArn(arn string) (*Arn, error) {
	return parseArn(arn, false)
}

// ParseArn parses an ARN string.
// Returns an error if parsing fails.
func ParseArn(arn string) (*Arn, error) {
	result, err := parseArn(arn, true)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New("ARN parsing failed")
	}
	return result, nil
}

func parseArn(arn string, throwOnError bool) (*Arn, error) {
	if arn == "" {
		return nil, nil
	}

	arnColonIndex := strings.Index(arn, ":")
	if arnColonIndex < 0 || arn[:arnColonIndex] != "acs" {
		if throwOnError {
			return nil, errors.New("malformed ARN - doesn't start with 'acs:'")
		}
		return nil, nil
	}

	serviceColonIndex := strings.Index(arn[arnColonIndex+1:], ":")
	if serviceColonIndex < 0 {
		if throwOnError {
			return nil, errors.New("malformed ARN - no service specified")
		}
		return nil, nil
	}
	serviceColonIndex += arnColonIndex + 1
	service := arn[arnColonIndex+1 : serviceColonIndex]

	regionColonIndex := strings.Index(arn[serviceColonIndex+1:], ":")
	if regionColonIndex < 0 {
		if throwOnError {
			return nil, errors.New("malformed ARN - no region specified")
		}
		return nil, nil
	}
	regionColonIndex += serviceColonIndex + 1
	region := arn[serviceColonIndex+1 : regionColonIndex]

	accountColonIndex := strings.Index(arn[regionColonIndex+1:], ":")
	if accountColonIndex < 0 {
		if throwOnError {
			return nil, errors.New("malformed ARN - no account specified")
		}
		return nil, nil
	}
	accountColonIndex += regionColonIndex + 1
	accountId := arn[regionColonIndex+1 : accountColonIndex]

	resource := arn[accountColonIndex+1:]
	if resource == "" {
		if throwOnError {
			return nil, errors.New("malformed ARN - no resource specified")
		}
		return nil, nil
	}

	var regionPtr *string
	if region != "" {
		regionPtr = &region
	}

	var accountIdPtr *string
	if accountId != "" {
		accountIdPtr = &accountId
	}

	builder := ArnBuilderNew().
		Service(service).
		Region(regionPtr).
		AccountId(accountIdPtr).
		Resource(resource)

	return builder.Build()
}

// String returns the string representation of the ARN.
func (a *Arn) String() string {
	region := ""
	if a.region != nil {
		region = *a.region
	}

	accountId := ""
	if a.accountId != nil {
		accountId = *a.accountId
	}

	return fmt.Sprintf("acs:%s:%s:%s:%s", a.service, region, accountId, a.resource)
}

// Equals checks if two Arn instances are equal.
func (a *Arn) Equals(other *Arn) bool {
	if a == other {
		return true
	}
	if other == nil {
		return false
	}

	if a.service != other.service {
		return false
	}

	if (a.region == nil && other.region != nil) || (a.region != nil && other.region == nil) {
		return false
	}
	if a.region != nil && other.region != nil && *a.region != *other.region {
		return false
	}

	if (a.accountId == nil && other.accountId != nil) || (a.accountId != nil && other.accountId == nil) {
		return false
	}
	if a.accountId != nil && other.accountId != nil && *a.accountId != *other.accountId {
		return false
	}

	if a.resource != other.resource {
		return false
	}

	if (a.arnResource == nil && other.arnResource != nil) || (a.arnResource != nil && other.arnResource == nil) {
		return false
	}
	if a.arnResource != nil && other.arnResource != nil && !a.arnResource.Equals(other.arnResource) {
		return false
	}

	return true
}

// HashCode returns the hash code for the ARN.
func (a *Arn) HashCode() int {
	result := hashString("acs")
	result = 31*result + hashString(a.service)
	if a.region != nil {
		result = 31*result + hashString(*a.region)
	}
	if a.accountId != nil {
		result = 31*result + hashString(*a.accountId)
	}
	result = 31*result + hashString(a.resource)
	return result
}

// ToBuilder creates a builder from the current Arn.
func (a *Arn) ToBuilder() *ArnBuilder {
	return ArnBuilderNew().
		Service(a.service).
		Region(a.region).
		AccountId(a.accountId).
		Resource(a.resource)
}

func hashString(s string) int {
	hash := 0
	for _, c := range s {
		hash = 31*hash + int(c)
	}
	return hash
}
