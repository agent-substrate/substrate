package oss

import (
	"context"
)

type RegionInfo struct {
	// The region ID.
	Region *string `xml:"Region"`

	// The public endpoint of the region.
	InternetEndpoint *string `xml:"InternetEndpoint"`

	// The internal endpoint of the region.
	InternalEndpoint *string `xml:"InternalEndpoint"`

	// The acceleration endpoint of the region. The value is always oss-accelerate.aliyuncs.com.
	AccelerateEndpoint *string `xml:"AccelerateEndpoint"`
}

type RegionInfoList struct {
	// The information about the regions.
	RegionInfos []RegionInfo `xml:"RegionInfo"`
}

type DescribeRegionsRequest struct {
	// The region ID of the request.
	Regions *string `input:"query,regions"`

	RequestCommon
}

type DescribeRegionsResult struct {
	// The information about the regions.
	RegionInfoList *RegionInfoList `output:"body,RegionInfoList,xml"`

	ResultCommon
}

// DescribeRegions Queries the endpoints of all supported regions or the endpoints of a specific region.
func (c *Client) DescribeRegions(ctx context.Context, request *DescribeRegionsRequest, optFns ...func(*Options)) (*DescribeRegionsResult, error) {
	var err error
	if request == nil {
		request = &DescribeRegionsRequest{}
	}
	input := &OperationInput{
		OpName: "DescribeRegions",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"regions": "",
		},
	}

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DescribeRegionsResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
