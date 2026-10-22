// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package pulldispatch

import "github.com/superdurable/dex/gen/dexpb"

func Routing(target *dexpb.WorkerTarget, stickyAppWorkerID string, servedByAppWorkerID string) *dexpb.PullRouting {
	if !target.GetIsPullMode() {
		return nil
	}
	routing := &dexpb.PullRouting{StickyAppWorkerId: stickyAppWorkerID, ServedByAppWorkerId: servedByAppWorkerID}
	if servedByAppWorkerID != "" {
		routing.Source = dexpb.PullRoutingSource_PULL_ROUTING_SOURCE_FLOW_TYPE_QUEUE
		if stickyAppWorkerID != "" && servedByAppWorkerID == stickyAppWorkerID {
			routing.Source = dexpb.PullRoutingSource_PULL_ROUTING_SOURCE_STICKY_QUEUE
		}
	}
	return routing
}
