// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package pulldispatch

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type testDispatchStream struct {
	grpc.ServerStreamingClient[dexpb.DispatchTaskResponse]
	frames chan *dexpb.DispatchTaskResponse
}

type firstFrameResult struct {
	frame *dexpb.DispatchTaskResponse
	err   error
}

func (s *testDispatchStream) Recv() (*dexpb.DispatchTaskResponse, error) {
	return <-s.frames, nil
}

func TestReceiveFirstFrameHeartbeatsOnlyWhileUnassigned(t *testing.T) {
	stream := &testDispatchStream{frames: make(chan *dexpb.DispatchTaskResponse)}
	heartbeats := make(chan struct{}, 4)
	result := make(chan firstFrameResult, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		frame, err := ReceiveFirstFrame(ctx, stream, 5*time.Second, func() { heartbeats <- struct{}{} }, nil)
		result <- firstFrameResult{frame: frame, err: err}
	}()
	stream.frames <- &dexpb.DispatchTaskResponse{Output: &dexpb.DispatchTaskResponse_Assigned{Assigned: &emptypb.Empty{}}}
	select {
	case <-heartbeats:
		t.Fatal("assigned Worker received a synthetic heartbeat")
	case <-time.After(1200 * time.Millisecond):
	}
	stream.frames <- &dexpb.DispatchTaskResponse{Output: &dexpb.DispatchTaskResponse_Unassigned{Unassigned: &emptypb.Empty{}}}
	select {
	case <-heartbeats:
	case <-ctx.Done():
		t.Fatal("unassigned task received no synthetic heartbeat")
	}
	stream.frames <- &dexpb.DispatchTaskResponse{Output: &dexpb.DispatchTaskResponse_Assigned{Assigned: &emptypb.Empty{}}}
	select {
	case <-heartbeats:
		t.Fatal("reassigned Worker received a synthetic heartbeat")
	case <-time.After(1200 * time.Millisecond):
	}
	final := &dexpb.DispatchTaskResponse{Output: &dexpb.DispatchTaskResponse_Execute{Execute: &dexpb.InvokeExecuteMethodOutput{}}}
	stream.frames <- final
	select {
	case received := <-result:
		require.NoError(t, received.err)
		require.Same(t, final, received.frame)
	case <-ctx.Done():
		t.Fatal("final frame was not returned")
	}
}
