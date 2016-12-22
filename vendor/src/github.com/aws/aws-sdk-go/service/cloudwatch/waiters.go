// THIS FILE IS AUTOMATICALLY GENERATED. DO NOT EDIT.

package cloudwatch

import (
	"github.com/aws/aws-sdk-go/private/waiter"
)

// WaitUntilAlarmExists uses the CloudWatch API operation
// DescribeAlarms to wait for a condition to be met before returning.
// If the condition is not meet within the max attempt window an error will
// be returned.
func (c *CloudWatch) WaitUntilAlarmExists(input *DescribeAlarmsInput) error {
	waiterCfg := waiter.Config{
		Operation:   "DescribeAlarms",
		Delay:       5,
		MaxAttempts: 40,
		Acceptors: []waiter.WaitAcceptor{
			{
				State:    "success",
				Matcher:  "path",
				Argument: "length(MetricAlarms[]) > `0`",
				Expected: true,
			},
		},
	}

	w := waiter.Waiter{
		Client: c,
		Input:  input,
		Config: waiterCfg,
	}
	return w.Wait()
}
