// Copyright 2026 Google LLC
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

package ec2macautoscaler

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
)

type autoScalingAPI interface {
	DescribeAutoScalingGroups(context.Context, *autoscaling.DescribeAutoScalingGroupsInput, ...func(*autoscaling.Options)) (*autoscaling.DescribeAutoScalingGroupsOutput, error)
	SetDesiredCapacity(context.Context, *autoscaling.SetDesiredCapacityInput, ...func(*autoscaling.Options)) (*autoscaling.SetDesiredCapacityOutput, error)
}

type ASGScaler struct {
	client autoScalingAPI
	name   string
}

func NewASGScaler(client autoScalingAPI, name string) (*ASGScaler, error) {
	if client == nil || name == "" {
		return nil, fmt.Errorf("auto scaling client and group name are required")
	}
	return &ASGScaler{client: client, name: name}, nil
}

func (s *ASGScaler) Desired(ctx context.Context) (int32, int32, error) {
	out, err := s.client.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{s.name},
	})
	if err != nil {
		return 0, 0, err
	}
	if len(out.AutoScalingGroups) != 1 {
		return 0, 0, fmt.Errorf("auto scaling group %q not found", s.name)
	}
	group := out.AutoScalingGroups[0]
	return aws.ToInt32(group.DesiredCapacity), aws.ToInt32(group.MaxSize), nil
}

func (s *ASGScaler) SetDesired(ctx context.Context, desired int32) error {
	_, err := s.client.SetDesiredCapacity(ctx, &autoscaling.SetDesiredCapacityInput{
		AutoScalingGroupName: aws.String(s.name),
		DesiredCapacity:      aws.Int32(desired),
		HonorCooldown:        aws.Bool(true),
	})
	return err
}
