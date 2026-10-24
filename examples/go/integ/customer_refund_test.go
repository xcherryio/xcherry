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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/examples/go/products/customer-refund/agentic"
	refundmodel "github.com/superdurable/dex/examples/go/products/customer-refund/model"
	"github.com/superdurable/dex/examples/go/registry"
	"github.com/superdurable/dex/sdk-go/dex"
)

func TestDeterministicCustomerRefundOutcomes(t *testing.T) {
	tests := []struct {
		name               string
		caseID             string
		orderAgeDays       int64
		wantRecommendation string
		wantBillingOutcome any
	}{
		{name: "eligible refund", orderAgeDays: 12, wantRecommendation: "refund", wantBillingOutcome: "confirmed"},
		{name: "expired denial", orderAgeDays: 31, wantRecommendation: "deny-outside-window", wantBillingOutcome: nil},
		{name: "missing order", caseID: "no-such-order", orderAgeDays: 4, wantRecommendation: "manual-order-follow-up", wantBillingOutcome: nil},
		{name: "declined provider", caseID: "declined", orderAgeDays: 4, wantRecommendation: "refund", wantBillingOutcome: "declined"},
		{name: "unknown provider", caseID: "unproven", orderAgeDays: 4, wantRecommendation: "refund", wantBillingOutcome: "unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := integrationContext(t)
			flowID := newFlowID(t, "customer-refund")
			caseID := test.caseID
			if caseID == "" {
				caseID = flowID
			}
			_, err := integClient.StartFlow(ctx, registry.CustomerRefund, flowID, refundmodel.RefundCase{
				CaseID: caseID, Customer: "customer", CustomerNote: "refund", AmountCents: 4200,
				OrderAgeDays: test.orderAgeDays,
			}, dex.StartFlowOptions{})
			require.NoError(t, err)
			require.Equal(t, dex.FlowCompleted, waitForFlow(t, flowID).Status)

			var display map[string]any
			require.NoError(t, integClient.InvokeRPC(
				ctx, flowID, registry.CustomerRefund.GetDexDisplay, nil, &display,
			))
			require.Equal(t, test.wantRecommendation, display["recommended-action"])
			require.Equal(t, test.wantBillingOutcome, display["billing-outcome"])
		})
	}
}

func TestAgenticCustomerRefundAutomaticAndUnknownOutcomes(t *testing.T) {
	skipWhenServerZD(t, "failure policies")
	tests := []struct {
		name               string
		caseID             string
		wantBillingOutcome string
		// An unknown billing effect gives up at NonConvergenceStep, so it never drafts a message.
		messagesCustomer bool
	}{
		{name: "automatic refund", wantBillingOutcome: "confirmed", messagesCustomer: true},
		{name: "unknown effect", caseID: "unproven", wantBillingOutcome: "unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := integrationContext(t)
			flowID := newFlowID(t, "agentic-refund")
			caseID := test.caseID
			if caseID == "" {
				caseID = flowID
			}
			_, err := integClient.StartFlow(ctx, registry.AgenticRefund, flowID, refundmodel.RefundCase{
				CaseID: caseID, Customer: "customer", CustomerNote: "refund", AmountCents: 4200,
				OrderAgeDays: 8,
			}, dex.StartFlowOptions{})
			require.NoError(t, err)
			if test.messagesCustomer {
				confirmAgenticMessage(t, ctx, flowID, "")
			}
			require.Equal(t, dex.FlowCompleted, waitForFlow(t, flowID).Status)
			var display map[string]any
			require.NoError(t, integClient.InvokeRPC(
				ctx, flowID, registry.AgenticRefund.GetDexDisplay, nil, &display,
			))
			require.Equal(t, test.wantBillingOutcome, display["billing-outcome"])
		})
	}
}

func TestAgenticCustomerRefundApproveAndRejectActions(t *testing.T) {
	skipWhenServerZD(t, "failure policies")
	t.Run("approve without input", func(t *testing.T) {
		ctx := integrationContext(t)
		flowID := newFlowID(t, "agentic-approve")
		startAgenticEscalation(t, ctx, flowID, flowID, 1_400_000)
		approvalGateKey := waitForAgenticGate(t, ctx, flowID)

		require.NoError(t, integClient.InvokeRPC(
			ctx, flowID, registry.AgenticRefund.ApproveRefund, nil, nil,
		))
		confirmAgenticMessage(t, ctx, flowID, approvalGateKey)
		require.Equal(t, dex.FlowCompleted, waitForFlow(t, flowID).Status)
	})

	t.Run("reject validates gate snapshot", func(t *testing.T) {
		ctx := integrationContext(t)
		flowID := newFlowID(t, "agentic-reject")
		startAgenticEscalation(t, ctx, flowID, flowID, 1_400_000)
		gateRequestKey := waitForAgenticGate(t, ctx, flowID)

		err := integClient.InvokeRPC(ctx, flowID, registry.AgenticRefund.RejectRefund, agentic.RejectRefundInput{
			Reason: "duplicate request", GateRequestKey: "stale-gate",
		}, nil)
		require.ErrorContains(t, err, "approval gate changed")
		require.NoError(t, integClient.InvokeRPC(
			ctx, flowID, registry.AgenticRefund.RejectRefund,
			agentic.RejectRefundInput{Reason: "duplicate request", GateRequestKey: gateRequestKey}, nil,
		))
		confirmAgenticMessage(t, ctx, flowID, gateRequestKey)
		require.Equal(t, dex.FlowCompleted, waitForFlow(t, flowID).Status)
	})
}

func TestAgenticCustomerRefundEscalatesUnavailableEvidence(t *testing.T) {
	skipWhenServerZD(t, "failure policies")
	ctx := integrationContext(t)
	flowID := newFlowID(t, "agentic-evidence")
	startAgenticEscalation(t, ctx, flowID, "agent-uncertain", 4200)
	approvalGateKey := waitForAgenticGate(t, ctx, flowID)
	require.NoError(t, integClient.InvokeRPC(
		ctx, flowID, registry.AgenticRefund.ApproveRefund, nil, nil,
	))
	confirmAgenticMessage(t, ctx, flowID, approvalGateKey)
	require.Equal(t, dex.FlowCompleted, waitForFlow(t, flowID).Status)
}

func TestAgenticCustomerRefundSearchAttributes(t *testing.T) {
	skipWhenServerZD(t, "failure policies")
	ctx := integrationContext(t)
	flowID := newFlowID(t, "agentic-refund-search")
	_, err := integClient.StartFlow(ctx, registry.AgenticRefund, flowID, refundmodel.RefundCase{
		CaseID:        "search-case",
		Customer:      "customer",
		CustomerEmail: "synthetic-search@example.com",
		CustomerNote:  "review",
		AmountCents:   1_400_000,
		OrderAgeDays:  8,
	}, dex.StartFlowOptions{})
	require.NoError(t, err)
	waitForAgenticGate(t, ctx, flowID)
	for _, query := range []string{
		"FlowType = 'AgenticCustomerRefundFlow' AND keyword1 = 'synthetic-search@example.com'",
		"FlowType = 'AgenticCustomerRefundFlow' AND keyword2 = 'awaiting-manager-rule'",
		"FlowType = 'AgenticCustomerRefundFlow' AND double1 = 14000",
	} {
		query := query
		var searchErr error
		require.Eventually(t, func() bool {
			page, err := integClient.SearchFlows(ctx, query, 20, "")
			searchErr = err
			if err != nil {
				return false
			}
			for _, entry := range page.Flows {
				if entry.FlowID == flowID {
					return true
				}
			}
			return false
		}, 20*time.Second, 200*time.Millisecond, "SearchFlows failed for %q: %v", query, searchErr)
	}
}

func startAgenticEscalation(
	t *testing.T,
	ctx context.Context,
	flowID string,
	caseID string,
	amountCents int64,
) {
	t.Helper()
	_, err := integClient.StartFlow(ctx, registry.AgenticRefund, flowID, refundmodel.RefundCase{
		CaseID: caseID, Customer: "customer", CustomerNote: "review", AmountCents: amountCents,
		OrderAgeDays: 8,
	}, dex.StartFlowOptions{})
	require.NoError(t, err)
}

// Answer the customer-message gate, which every path that writes to the customer stops at.
//
// Both gates publish through the same gate-request-key and the drafting Step mints a fresh one, so
// the message gate is the first key that is not the approval key just answered. Pass "" when the run
// never opened an approval gate.
func confirmAgenticMessage(t *testing.T, ctx context.Context, flowID string, answeredKey string) {
	t.Helper()
	gateRequestKey := ""
	var invokeErr error
	require.Eventually(t, func() bool {
		var display map[string]any
		invokeErr = integClient.InvokeRPC(ctx, flowID, registry.AgenticRefund.GetDexDisplay, nil, &display)
		if invokeErr != nil {
			return false
		}
		key, _ := display["gate-request-key"].(string)
		if key == "" || key == answeredKey {
			return false
		}
		gateRequestKey = key
		return true
	}, 30*time.Second, 200*time.Millisecond, "GetDexDisplay failed: %v", invokeErr)

	require.NoError(t, integClient.InvokeRPC(
		ctx, flowID, registry.AgenticRefund.ConfirmCustomerMessage,
		agentic.ConfirmCustomerMessageInput{GateRequestKey: gateRequestKey}, nil,
	))
}

func waitForAgenticGate(t *testing.T, ctx context.Context, flowID string) string {
	t.Helper()
	gateRequestKey := ""
	var invokeErr error
	require.Eventually(t, func() bool {
		var display map[string]any
		invokeErr = integClient.InvokeRPC(ctx, flowID, registry.AgenticRefund.GetDexDisplay, nil, &display)
		if invokeErr != nil {
			return false
		}
		gateRequestKey, _ = display["gate-request-key"].(string)
		return gateRequestKey != ""
	}, 30*time.Second, 200*time.Millisecond, "GetDexDisplay failed: %v", invokeErr)
	return gateRequestKey
}
