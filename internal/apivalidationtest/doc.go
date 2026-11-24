// Copyright 2026 The Agent Substrate Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package apivalidationtest tests the formats in .validation-extensions.yaml.
// Child applies each format as a field tag, and Parent applies each as a
// subfield tag on ChildWithoutTags.
//
// Kubernetes codegen tools required this to be in doc.go, no other name will
// work.
//
// +k8s:validation-gen=*
// +k8s:validation-gen-scheme-registry=nil
package apivalidationtest

type Child struct {
	// +k8s:optional
	// +k8s:format=ate-singleton-name
	SingletonName string `json:"singletonName"`

	// +k8s:optional
	// +k8s:format=ate-http-probe-path
	HTTPProbePath string `json:"httpProbePath"`

	// +k8s:optional
	// +k8s:format=ate-env-var-name
	EnvVarName string `json:"envVarName"`

	// +k8s:optional
	// +k8s:format=ate-volume-type-name
	VolumeTypeName string `json:"volumeTypeName"`

	// +k8s:optional
	// +k8s:format=ate-volume-storage-id
	VolumeStorageID string `json:"volumeStorageID"`

	// +k8s:optional
	// +k8s:format=ate-http-header-name
	HTTPHeaderName string `json:"httpHeaderName"`

	// +k8s:optional
	// +k8s:format=ate-http-header-value
	HTTPHeaderValue string `json:"httpHeaderValue"`
}

type ChildWithoutTags struct {
	SingletonName   string `json:"singletonName"`
	HTTPProbePath   string `json:"httpProbePath"`
	EnvVarName      string `json:"envVarName"`
	VolumeTypeName  string `json:"volumeTypeName"`
	VolumeStorageID string `json:"volumeStorageID"`
	HTTPHeaderName  string `json:"httpHeaderName"`
	HTTPHeaderValue string `json:"httpHeaderValue"`
}

type Parent struct {
	// +k8s:subfield(singletonName)=+k8s:format=ate-singleton-name
	// +k8s:subfield(httpProbePath)=+k8s:format=ate-http-probe-path
	// +k8s:subfield(envVarName)=+k8s:format=ate-env-var-name
	// +k8s:subfield(volumeTypeName)=+k8s:format=ate-volume-type-name
	// +k8s:subfield(volumeStorageID)=+k8s:format=ate-volume-storage-id
	// +k8s:subfield(httpHeaderName)=+k8s:format=ate-http-header-name
	// +k8s:subfield(httpHeaderValue)=+k8s:format=ate-http-header-value
	Child ChildWithoutTags `json:"child"`
}
