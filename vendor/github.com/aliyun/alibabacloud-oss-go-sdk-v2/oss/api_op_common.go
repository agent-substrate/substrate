package oss

import (
	"context"
)

func (c *Client) InvokeOperation(ctx context.Context, input *OperationInput, optFns ...func(*Options)) (*OperationOutput, error) {
	if err := validateInput(input); err != nil {
		return nil, err
	}
	return c.invokeOperation(ctx, input, optFns)
}
