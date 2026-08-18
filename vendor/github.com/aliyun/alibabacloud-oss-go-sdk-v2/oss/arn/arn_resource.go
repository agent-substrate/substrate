package arn

import (
	"errors"
	"fmt"
	"strings"
)

// ArnResource represents the resource portion of an ARN.
type ArnResource struct {
	resourceType *string
	resource     string
	qualifier    *string
}

// ArnResourceBuilder is used to build an ArnResource instance.
type ArnResourceBuilder struct {
	resourceType *string
	resource     string
	qualifier    *string
}

// ResourceType returns the optional resource type.
func (a *ArnResource) ResourceType() *string {
	return a.resourceType
}

// Resource returns the entire resource as a string.
func (a *ArnResource) Resource() string {
	return a.resource
}

// Qualifier returns the optional resource qualifier.
func (a *ArnResource) Qualifier() *string {
	return a.qualifier
}

// ArnResourceBuilderNew creates a new ArnResourceBuilder.
func ArnResourceBuilderNew() *ArnResourceBuilder {
	return &ArnResourceBuilder{}
}

// ResourceType sets the resourceType for the builder.
func (b *ArnResourceBuilder) ResourceType(resourceType *string) *ArnResourceBuilder {
	b.resourceType = resourceType
	return b
}

// Resource sets the resource for the builder.
func (b *ArnResourceBuilder) Resource(resource string) *ArnResourceBuilder {
	b.resource = resource
	return b
}

// Qualifier sets the qualifier for the builder.
func (b *ArnResourceBuilder) Qualifier(qualifier *string) *ArnResourceBuilder {
	b.qualifier = qualifier
	return b
}

// Build creates an ArnResource from the builder.
func (b *ArnResourceBuilder) Build() (*ArnResource, error) {
	if strings.TrimSpace(b.resource) == "" {
		return nil, errors.New("resource must not be blank")
	}

	return &ArnResource{
		resourceType: b.resourceType,
		resource:     b.resource,
		qualifier:    b.qualifier,
	}, nil
}

// ArnResourceFromString parses an ArnResource from a string.
func ArnResourceFromString(resource string) (*ArnResource, error) {
	splitter := findFirstOccurrence(resource, ':', '/')
	if splitter == 0 {
		return ArnResourceBuilderNew().Resource(resource).Build()
	}

	resourceTypeColonIndex := strings.IndexRune(resource, splitter)
	if resourceTypeColonIndex < 0 {
		return ArnResourceBuilderNew().Resource(resource).Build()
	}

	resourceType := resource[:resourceTypeColonIndex]
	builder := ArnResourceBuilderNew().ResourceType(&resourceType)

	resourceColonIndex := strings.IndexRune(resource[resourceTypeColonIndex:], splitter)
	if resourceColonIndex < 0 {
		builder.Resource(resource[resourceTypeColonIndex+1:])
	} else {
		resourceColonIndex += resourceTypeColonIndex
		qualifierColonIndex := strings.IndexRune(resource[resourceColonIndex+1:], splitter)
		if qualifierColonIndex < 0 {
			builder.Resource(resource[resourceTypeColonIndex+1:])
		} else {
			qualifierColonIndex += resourceColonIndex + 1
			res := resource[resourceTypeColonIndex+1 : qualifierColonIndex]
			builder.Resource(res)
			qual := resource[qualifierColonIndex+1:]
			builder.Qualifier(&qual)
		}
	}

	return builder.Build()
}

// findFirstOccurrence finds the first occurrence of either rune in the string.
// Returns 0 if neither is found.
func findFirstOccurrence(s string, runes ...rune) rune {
	firstPos := -1
	var firstRune rune

	for _, r := range runes {
		pos := strings.IndexRune(s, r)
		if pos >= 0 && (firstPos < 0 || pos < firstPos) {
			firstPos = pos
			firstRune = r
		}
	}

	if firstPos < 0 {
		return 0
	}
	return firstRune
}

// String returns the string representation of the ArnResource.
func (a *ArnResource) String() string {
	var result strings.Builder

	if a.resourceType != nil {
		result.WriteString(*a.resourceType)
	}
	result.WriteString(":")

	result.WriteString(a.resource)
	result.WriteString(":")

	if a.qualifier != nil {
		result.WriteString(*a.qualifier)
	}

	return result.String()
}

// Equals checks if two ArnResource instances are equal.
func (a *ArnResource) Equals(other *ArnResource) bool {
	if a == other {
		return true
	}
	if other == nil {
		return false
	}

	if (a.resourceType == nil && other.resourceType != nil) || (a.resourceType != nil && other.resourceType == nil) {
		return false
	}
	if a.resourceType != nil && other.resourceType != nil && *a.resourceType != *other.resourceType {
		return false
	}

	if a.resource != other.resource {
		return false
	}

	if (a.qualifier == nil && other.qualifier != nil) || (a.qualifier != nil && other.qualifier == nil) {
		return false
	}
	if a.qualifier != nil && other.qualifier != nil && *a.qualifier != *other.qualifier {
		return false
	}

	return true
}

// HashCode returns the hash code for the ArnResource.
func (a *ArnResource) HashCode() int {
	result := 0
	if a.resourceType != nil {
		result = hashString(*a.resourceType)
	}
	result = 31*result + hashString(a.resource)
	if a.qualifier != nil {
		result = 31*result + hashString(*a.qualifier)
	}
	return result
}

// ToBuilder creates a builder from the current ArnResource.
func (a *ArnResource) ToBuilder() *ArnResourceBuilder {
	return ArnResourceBuilderNew().
		Resource(a.resource).
		ResourceType(a.resourceType).
		Qualifier(a.qualifier)
}

// EnsureBucketResource ensures this is a valid bucket resource.
// Returns an error if not a valid bucket resource.
func (a *ArnResource) EnsureBucketResource() error {
	if a.resourceType == nil || *a.resourceType != "bucket" ||
		strings.TrimSpace(a.resource) == "" ||
		a.qualifier != nil {
		return fmt.Errorf("%s is not bucket resource", a.String())
	}
	return nil
}
