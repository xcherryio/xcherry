// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

// Package tenantadmin serves TenantAdminService: tenant tombstones and bounded, paced purges.
package tenantadmin

import (
	"context"
	"fmt"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	uclient "github.com/superdurable/dex/service/client"
	"github.com/superdurable/dex/service/common/attributestore"
	"github.com/superdurable/dex/service/common/blobstore"
	serviceerrors "github.com/superdurable/dex/service/common/errors"
	"github.com/superdurable/dex/service/common/streamstore"
	"github.com/superdurable/dex/service/tenancy"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	purgeExecutionBatchSize   = 100
	purgeBlobObjectBatchSize  = 1000
	executionStatusRunning    = "Running"
	purgeTerminationReason    = "tenant purge"
	purgeExecutionDescription = "terminate or delete tenant executions"
)

// Service implements dexpb.TenantAdminServiceServer for operator principals.
type Service struct {
	dexpb.UnimplementedTenantAdminServiceServer

	cfg               *config.TenancyConfig
	client            uclient.UnifiedClient
	blobStore         blobstore.BlobStore
	streamStore       *streamstore.Store
	attributeStore    *attributestore.Manager
	tombstones        *tenancy.TombstoneRegistry
	executionLimiter  *rate.Limiter
	confirmationDelay time.Duration
}

func NewService(
	cfg *config.TenancyConfig,
	client uclient.UnifiedClient,
	blobStore blobstore.BlobStore,
	streamStore *streamstore.Store,
	attributeStore *attributestore.Manager,
	tombstones *tenancy.TombstoneRegistry,
) *Service {
	if cfg == nil || client == nil || blobStore == nil || streamStore == nil || attributeStore == nil || tombstones == nil {
		panic("tenant admin service dependencies must not be nil")
	}
	return &Service{
		cfg:               cfg,
		client:            client,
		blobStore:         blobStore,
		streamStore:       streamStore,
		attributeStore:    attributeStore,
		tombstones:        tombstones,
		executionLimiter:  rate.NewLimiter(rate.Limit(cfg.Limits.EffectivePurgeExecutionsPerSecond()), 1),
		confirmationDelay: cfg.EffectivePurgeConfirmationDelay(),
	}
}

func (s *Service) TombstoneTenant(
	ctx context.Context,
	req *dexpb.TombstoneTenantRequest,
) (*dexpb.TombstoneTenantResponse, error) {
	tenantID, err := validTenantID(req.GetTenantId())
	if err != nil {
		return nil, err
	}
	record, err := s.tombstones.Update(ctx, func(record *tenancy.TombstoneRecord, now time.Time) error {
		if _, exists := record.Tenants[tenantID]; !exists {
			record.Tenants[tenantID] = &tenancy.TenantTombstone{TombstonedAt: now}
		}
		return nil
	})
	if err != nil {
		return nil, serviceerrors.Internal(err.Error()).ToGRPCError()
	}
	return &dexpb.TombstoneTenantResponse{State: s.state(tenantID, record)}, nil
}

func (s *Service) DescribeTenant(
	ctx context.Context,
	req *dexpb.DescribeTenantRequest,
) (*dexpb.DescribeTenantResponse, error) {
	tenantID, err := validTenantID(req.GetTenantId())
	if err != nil {
		return nil, err
	}
	record, err := s.tombstones.Read(ctx)
	if err != nil {
		return nil, serviceerrors.Internal(err.Error()).ToGRPCError()
	}
	return &dexpb.DescribeTenantResponse{State: s.state(tenantID, record)}, nil
}

// PurgeTenant deletes one bounded batch of a tombstoned tenant's data: open executions, closed executions, blob
// objects, then Stream keys and Attribute Store rows. It reports done only after two empty passes at least the
// confirmation delay apart, and is idempotent.
func (s *Service) PurgeTenant(
	ctx context.Context,
	req *dexpb.PurgeTenantRequest,
) (*dexpb.PurgeTenantResponse, error) {
	tenantID, err := validTenantID(req.GetTenantId())
	if err != nil {
		return nil, err
	}
	record, err := s.tombstones.Read(ctx)
	if err != nil {
		return nil, serviceerrors.Internal(err.Error()).ToGRPCError()
	}
	tombstone, isTombstoned := record.Tenants[tenantID]
	if !isTombstoned {
		return nil, serviceerrors.NewErrorAndStatus(
			codes.FailedPrecondition,
			dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
			"tombstone the tenant before purging it",
		).ToGRPCError()
	}
	if tombstone.PurgeDoneAt != nil {
		return &dexpb.PurgeTenantResponse{State: s.state(tenantID, record), Done: true}, nil
	}
	response, err := s.purgeBatch(ctx, tenantID)
	if err != nil {
		return nil, serviceerrors.Internal(err.Error()).ToGRPCError()
	}
	isEmpty := response.GetTerminatedExecutions() == 0 && response.GetDeletedExecutions() == 0 &&
		response.GetDeletedBlobObjects() == 0
	record, err = s.tombstones.Update(ctx, func(record *tenancy.TombstoneRecord, now time.Time) error {
		current, exists := record.Tenants[tenantID]
		if !exists {
			return fmt.Errorf("tenant %q lost its tombstone during the purge", tenantID)
		}
		switch {
		case current.PurgeDoneAt != nil:
		case !isEmpty:
			current.FirstEmptyAt = nil
		case current.FirstEmptyAt == nil:
			current.FirstEmptyAt = &now
		case now.Sub(*current.FirstEmptyAt) >= s.confirmationDelay:
			current.PurgeDoneAt = &now
		}
		return nil
	})
	if err != nil {
		return nil, serviceerrors.Internal(err.Error()).ToGRPCError()
	}
	response.State = s.state(tenantID, record)
	response.Done = record.Tenants[tenantID] != nil && record.Tenants[tenantID].PurgeDoneAt != nil
	return response, nil
}

func (s *Service) purgeBatch(ctx context.Context, tenantID string) (*dexpb.PurgeTenantResponse, error) {
	response := &dexpb.PurgeTenantResponse{}
	tenantClause := fmt.Sprintf("%s = %q", tenancy.SearchAttributeDexTenantID, tenantID)
	openExecutions, err := s.listExecutions(ctx, tenantClause+fmt.Sprintf(" AND ExecutionStatus = %q", executionStatusRunning))
	if err != nil {
		return nil, err
	}
	for _, execution := range openExecutions {
		if err := s.paceExecution(ctx); err != nil {
			return nil, err
		}
		err := s.client.TerminateWorkflow(ctx, execution.GetFlowId(), execution.GetRunId(), purgeTerminationReason)
		if err != nil && !s.client.IsNotFoundError(err) {
			return nil, fmt.Errorf("%s: %w", purgeExecutionDescription, err)
		}
		response.TerminatedExecutions++
	}
	closedExecutions, err := s.listExecutions(ctx, tenantClause+fmt.Sprintf(" AND ExecutionStatus != %q", executionStatusRunning))
	if err != nil {
		return nil, err
	}
	for _, execution := range closedExecutions {
		if err := s.paceExecution(ctx); err != nil {
			return nil, err
		}
		err := s.client.DeleteWorkflowExecution(ctx, execution.GetFlowId(), execution.GetRunId())
		if err != nil && !s.client.IsNotFoundError(err) {
			return nil, fmt.Errorf("%s: %w", purgeExecutionDescription, err)
		}
		response.DeletedExecutions++
	}
	deletedObjects, err := s.blobStore.DeleteTenantObjects(ctx, tenantID, purgeBlobObjectBatchSize)
	response.DeletedBlobObjects = int32(deletedObjects)
	if err != nil {
		return nil, err
	}
	if deletedObjects < purgeBlobObjectBatchSize {
		if err := s.streamStore.DeleteTenant(ctx, tenantID); err != nil {
			return nil, err
		}
		if err := s.attributeStore.DeleteTenantRows(ctx, tenantID); err != nil {
			return nil, err
		}
	}
	return response, nil
}

func (s *Service) listExecutions(ctx context.Context, query string) ([]*dexpb.SearchFlowsResponseEntry, error) {
	response, err := s.client.ListWorkflow(ctx, &uclient.ListWorkflowExecutionsRequest{
		PageSize: purgeExecutionBatchSize,
		Query:    query,
	})
	if err != nil {
		return nil, fmt.Errorf("list tenant executions: %w", err)
	}
	return response.Executions, nil
}

func (s *Service) paceExecution(ctx context.Context) error {
	if err := s.executionLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("pace tenant purge: %w", err)
	}
	return nil
}

func (s *Service) state(tenantID string, record *tenancy.TombstoneRecord) *dexpb.TenantAdminState {
	state := &dexpb.TenantAdminState{TenantId: tenantID}
	tombstone, isTombstoned := record.Tenants[tenantID]
	if !isTombstoned {
		return state
	}
	state.IsTombstoned = true
	state.TombstonedTime = timestamppb.New(tombstone.TombstonedAt)
	if tombstone.PurgeDoneAt != nil {
		state.PurgeDoneTime = timestamppb.New(*tombstone.PurgeDoneAt)
	}
	if expiry, isDone := s.tombstones.TombstoneExpiry(tombstone); isDone {
		state.TombstoneExpiryTime = timestamppb.New(expiry)
	}
	return state
}

func validTenantID(tenantID string) (string, error) {
	if !config.TenantIDPattern.MatchString(tenantID) {
		return "", serviceerrors.InvalidArgument(
			dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
			fmt.Sprintf("tenant ID must match %s", config.TenantIDPattern),
		).ToGRPCError()
	}
	return tenantID, nil
}
