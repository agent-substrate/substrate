package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type StyleList struct {
	// The list of styles.
	Styles []StyleInfo `xml:"Style"`
}

type StyleInfo struct {
	// The time when the style was created.
	CreateTime *string `xml:"CreateTime"`

	// The time when the style was last modified.
	LastModifyTime *string `xml:"LastModifyTime"`

	// The category of this style。  Invalid value：image、document、video。
	Category *string `xml:"Category"`

	// The style name.
	Name *string `xml:"Name"`

	// The content of the style.
	Content *string `xml:"Content"`
}

type StyleContent struct {
	// The content of the style.
	Content *string `xml:"Content"`
}

type PutStyleRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the image style.
	StyleName *string `input:"query,styleName,required"`

	// The category of the style.
	Category *string `input:"query,category"`

	// The container that stores the content information about the image style.
	Style *StyleContent `input:"body,Style,xml,required"`

	RequestCommon
}

type PutStyleResult struct {
	ResultCommon
}

// PutStyle Adds an image style to a bucket. An image style contains one or more image processing parameters.
func (c *Client) PutStyle(ctx context.Context, request *PutStyleRequest, optFns ...func(*Options)) (*PutStyleResult, error) {
	var err error
	if request == nil {
		request = &PutStyleRequest{}
	}
	input := &OperationInput{
		OpName: "PutStyle",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"style": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"style", "styleName"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutStyleResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type ListStyleRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type ListStyleResult struct {

	// The container that was used to query the information about image styles.
	StyleList *StyleList `output:"body,StyleList,xml"`

	ResultCommon
}

// ListStyle Queries all image styles that are created for a bucket.
func (c *Client) ListStyle(ctx context.Context, request *ListStyleRequest, optFns ...func(*Options)) (*ListStyleResult, error) {
	var err error
	if request == nil {
		request = &ListStyleRequest{}
	}
	input := &OperationInput{
		OpName: "ListStyle",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"style": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"style"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &ListStyleResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetStyleRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the image style.
	StyleName *string `input:"query,styleName,required"`

	RequestCommon
}

type GetStyleResult struct {
	// The container that stores the information about the image style.
	Style *StyleInfo `output:"body,Style,xml"`

	ResultCommon
}

// GetStyle Queries the information about an image style of a bucket.
func (c *Client) GetStyle(ctx context.Context, request *GetStyleRequest, optFns ...func(*Options)) (*GetStyleResult, error) {
	var err error
	if request == nil {
		request = &GetStyleRequest{}
	}
	input := &OperationInput{
		OpName: "GetStyle",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"style": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"style", "styleName"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetStyleResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type DeleteStyleRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the image style.
	StyleName *string `input:"query,styleName,required"`

	RequestCommon
}

type DeleteStyleResult struct {
	ResultCommon
}

// DeleteStyle Deletes an image style from a bucket.
func (c *Client) DeleteStyle(ctx context.Context, request *DeleteStyleRequest, optFns ...func(*Options)) (*DeleteStyleResult, error) {
	var err error
	if request == nil {
		request = &DeleteStyleRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteStyle",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"style": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"style", "styleName"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteStyleResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
