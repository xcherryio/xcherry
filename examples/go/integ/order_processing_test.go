// Copyright (c) 2022-2026 Super Durable, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package integ

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/examples/go/products/order-processing"
	"github.com/superdurable/dex/examples/go/registry"
	"github.com/superdurable/dex/sdk-go/dex"
)

func TestOrderProcessingHappyPath(t *testing.T) {
	skipWhenServerZD(t, "failure policies")
	ctx := integrationContext(t)
	flowID := newFlowID(t, "order-processing")
	input := orderprocessing.OrderRequest{
		OrderID:    flowID,
		Email:      "buyer@example.com",
		CustomerID: "customer-1",
		Amount:     42,
	}
	_, err := integClient.StartFlow(
		ctx,
		registry.OrderProcessing,
		flowID,
		input,
		dex.StartFlowOptions{},
	)
	require.NoError(t, err)
	require.NoError(t, integClient.WaitForStepCompletion(
		ctx,
		flowID,
		dex.StepExecutionID{StepType: orderprocessing.ChargeStepType},
		dex.WaitForStepCompletionOptions{},
	))
	var approved string
	require.NoError(t, integClient.InvokeRPC(
		ctx,
		flowID,
		registry.OrderProcessing.Approve,
		"",
		&approved,
	))
	require.Equal(t, "ok", approved)
	result := waitForFlow(t, flowID)
	require.Equal(t, dex.FlowCompleted, result.Status)
	var output string
	require.NoError(t, result.Completions[0].Output.Decode(&output))
	require.Equal(t, "shipped:"+flowID, output)
}

func TestOrderProcessingReminderThenShip(t *testing.T) {
	skipWhenServerZD(t, "failure policies")
	ctx := integrationContext(t)
	flowID := newFlowID(t, "order-processing-reminder")
	input := orderprocessing.OrderRequest{
		OrderID:    flowID,
		Email:      "buyer@example.com",
		CustomerID: "customer-1",
		Amount:     42,
	}
	_, err := integClient.StartFlow(
		ctx,
		registry.OrderProcessing,
		flowID,
		input,
		dex.StartFlowOptions{},
	)
	require.NoError(t, err)
	require.NoError(t, integClient.WaitForStepCompletion(
		ctx,
		flowID,
		dex.StepExecutionID{StepType: orderprocessing.ChargeStepType},
		dex.WaitForStepCompletionOptions{},
	))
	require.Eventually(t, func() bool {
		return integClient.SkipTimer(
			integrationContext(t),
			flowID,
			dex.StepExecutionID{StepType: orderprocessing.ShipStepType},
			dex.TimerID{Index: new(int32)},
		) == nil
	}, 15*time.Second, 50*time.Millisecond)
	require.NoError(t, integClient.WaitForStepCompletion(
		ctx,
		flowID,
		dex.StepExecutionID{StepType: orderprocessing.ShipStepType},
		dex.WaitForStepCompletionOptions{},
	))
	var approved string
	require.NoError(t, integClient.InvokeRPC(
		ctx,
		flowID,
		registry.OrderProcessing.Approve,
		"",
		&approved,
	))
	require.Equal(t, "ok", approved)
	result := waitForFlow(t, flowID)
	require.Equal(t, dex.FlowCompleted, result.Status)
	var output string
	require.NoError(t, result.Completions[0].Output.Decode(&output))
	require.Equal(t, "shipped:"+flowID, output)
}

func TestOrderProcessingShipFailureRefunds(t *testing.T) {
	skipWhenServerZD(t, "failure policies")
	ctx := integrationContext(t)
	flowID := newFlowID(t, "order-processing-refund")
	input := orderprocessing.OrderRequest{
		OrderID:            flowID,
		Email:              "buyer@example.com",
		CustomerID:         "customer-1",
		Amount:             42,
		TestFailAtShipping: true,
	}
	_, err := integClient.StartFlow(
		ctx,
		registry.OrderProcessing,
		flowID,
		input,
		dex.StartFlowOptions{},
	)
	require.NoError(t, err)
	require.NoError(t, integClient.WaitForStepCompletion(
		ctx,
		flowID,
		dex.StepExecutionID{StepType: orderprocessing.ChargeStepType},
		dex.WaitForStepCompletionOptions{},
	))
	var approved string
	require.NoError(t, integClient.InvokeRPC(
		ctx,
		flowID,
		registry.OrderProcessing.Approve,
		"",
		&approved,
	))
	require.Equal(t, "ok", approved)
	result := waitForFlow(t, flowID)
	require.Equal(t, dex.FlowCompleted, result.Status)
	var output string
	require.NoError(t, result.Completions[0].Output.Decode(&output))
	require.Equal(t, "refunded:"+flowID, output)
}
