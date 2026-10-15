// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package indexsync

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	serviceerrors "github.com/superdurable/dex/service/common/errors"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/tag"
	"go.uber.org/yarpc/yarpcerrors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	initialPollInterval = 200 * time.Millisecond
	maximumPollInterval = time.Second
)

// Client manages backend attribute indexes.
type Client interface {
	ListAttributeIndexes(context.Context) (map[string]dexpb.IndexType, error)
	AddAttributeIndexes(context.Context, map[string]dexpb.IndexType) error
	NormalizeAttributeIndexType(dexpb.IndexType) dexpb.IndexType
}

// Synchronizer makes requested indexes visible in the workflow backend.
type Synchronizer struct {
	cfg    *config.Interpreter
	client Client
	logger log.Logger
	// Cadence mutates shared search-attribute state unsafely during concurrent registrations.
	syncSlot chan struct{}
}

// New creates an attribute index synchronizer.
func New(interpreterCfg *config.Interpreter, client Client, logger log.Logger) *Synchronizer {
	if interpreterCfg == nil {
		panic("interpreter config must not be nil")
	}
	if client == nil {
		panic("attribute index client must not be nil")
	}
	if logger == nil {
		panic("attribute index logger must not be nil")
	}
	synchronizer := &Synchronizer{
		cfg:      interpreterCfg,
		client:   client,
		logger:   logger,
		syncSlot: make(chan struct{}, 1),
	}
	synchronizer.syncSlot <- struct{}{}
	return synchronizer
}

// Sync creates missing indexes and waits until the backend reports them.
func (s *Synchronizer) Sync(ctx context.Context, requested map[string]dexpb.IndexType) error {
	if err := validateRequestedIndexes(requested); err != nil {
		return err
	}
	if len(requested) == 0 {
		return nil
	}
	if s.cfg.AttributeIndexesManagedExternally {
		return nil
	}
	syncCtx, cancel := context.WithTimeout(ctx, s.cfg.EffectiveAttributeIndexSyncTimeout())
	defer cancel()
	select {
	case <-syncCtx.Done():
		return backendError(syncCtx.Err())
	case <-s.syncSlot:
	}
	defer func() { s.syncSlot <- struct{}{} }()

	existing, err := s.listUntilAvailable(syncCtx)
	if err != nil {
		return err
	}
	missing, err := s.missingIndexes(requested, existing)
	if err != nil || len(missing) == 0 {
		return err
	}
	if addErr := s.addAttributeIndexes(syncCtx, missing); addErr != nil {
		existing, listErr := s.client.ListAttributeIndexes(syncCtx)
		if listErr == nil {
			remaining, compareErr := s.missingIndexes(requested, existing)
			if compareErr != nil || len(remaining) == 0 {
				return compareErr
			}
		}
		if !isRetryable(addErr) && !isConcurrentRegistration(addErr) {
			return backendError(addErr)
		}
		return s.retryRegistration(syncCtx, requested)
	}

	return s.waitUntilVisible(syncCtx, requested)
}

func (s *Synchronizer) listUntilAvailable(ctx context.Context) (map[string]dexpb.IndexType, error) {
	interval := initialPollInterval
	for {
		existing, err := s.client.ListAttributeIndexes(ctx)
		if err == nil {
			s.logger.Info("Attribute index list poll succeeded", tag.IndexCount(len(existing)))
			return existing, nil
		}
		if !isRetryable(err) {
			return nil, backendError(err)
		}
		s.logger.Info("Attribute index list poll will retry", tag.Error(err), tag.Interval(interval))
		if err := waitForPoll(ctx, interval); err != nil {
			return nil, syncDeadlineError(err)
		}
		interval = nextPollInterval(interval)
	}
}

func (s *Synchronizer) retryRegistration(
	ctx context.Context,
	requested map[string]dexpb.IndexType,
) error {
	interval := initialPollInterval
	for {
		if err := waitForPoll(ctx, interval); err != nil {
			return syncDeadlineError(err)
		}
		existing, err := s.client.ListAttributeIndexes(ctx)
		if err != nil {
			if !isRetryable(err) {
				return backendError(err)
			}
			s.logger.Info(
				"Attribute index registration poll will retry",
				tag.Error(err),
				tag.Interval(nextPollInterval(interval)),
			)
			interval = nextPollInterval(interval)
			continue
		}
		missing, err := s.missingIndexes(requested, existing)
		if err != nil || len(missing) == 0 {
			if err == nil {
				s.logger.Info("Attribute indexes became visible during registration retry", tag.Requested(requested))
			}
			return err
		}
		s.logger.Info(
			"Attribute indexes remain unregistered",
			tag.Missing(missing),
			tag.Interval(nextPollInterval(interval)),
		)
		addErr := s.addAttributeIndexes(ctx, missing)
		if addErr == nil {
			return s.waitUntilVisible(ctx, requested)
		}
		if !isRetryable(addErr) && !isConcurrentRegistration(addErr) {
			return backendError(addErr)
		}
		interval = nextPollInterval(interval)
	}
}

func (s *Synchronizer) waitUntilVisible(
	ctx context.Context,
	requested map[string]dexpb.IndexType,
) error {
	interval := initialPollInterval
	for {
		if err := waitForPoll(ctx, interval); err != nil {
			return syncDeadlineError(err)
		}
		existing, err := s.client.ListAttributeIndexes(ctx)
		if err == nil {
			missing, compareErr := s.missingIndexes(requested, existing)
			if compareErr != nil || len(missing) == 0 {
				if compareErr == nil {
					s.logger.Info("Attribute indexes are visible", tag.Requested(requested))
				}
				return compareErr
			}
			s.logger.Info(
				"Attribute indexes are not visible yet",
				tag.Missing(missing),
				tag.Interval(nextPollInterval(interval)),
			)
			addErr := s.addAttributeIndexes(ctx, missing)
			if addErr != nil && !isRetryable(addErr) && !isConcurrentRegistration(addErr) {
				return backendError(addErr)
			}
		} else if !isRetryable(err) {
			return backendError(err)
		} else {
			s.logger.Info(
				"Attribute index visibility poll will retry",
				tag.Error(err),
				tag.Interval(nextPollInterval(interval)),
			)
		}
		interval = nextPollInterval(interval)
	}
}

func (s *Synchronizer) addAttributeIndexes(
	ctx context.Context,
	indexes map[string]dexpb.IndexType,
) error {
	startedAt := time.Now()
	s.logger.Info("Adding attribute indexes", tag.Indexes(indexes))
	err := s.client.AddAttributeIndexes(ctx, indexes)
	elapsed := time.Since(startedAt)
	if err != nil {
		s.logger.Info("Add attribute indexes failed", tag.Error(err), tag.Elapsed(elapsed), tag.Indexes(indexes))
		return err
	}
	s.logger.Info("Add attribute indexes succeeded", tag.Elapsed(elapsed), tag.Indexes(indexes))
	return nil
}

func (s *Synchronizer) missingIndexes(
	requested map[string]dexpb.IndexType,
	existing map[string]dexpb.IndexType,
) (map[string]dexpb.IndexType, error) {
	missing := make(map[string]dexpb.IndexType)
	for name, requestedType := range requested {
		existingType, found := existing[name]
		if !found {
			missing[name] = requestedType
			continue
		}
		expectedType := s.client.NormalizeAttributeIndexType(requestedType)
		if existingType != expectedType {
			return nil, serviceerrors.NewErrorAndStatus(
				codes.FailedPrecondition,
				dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
				fmt.Sprintf("attribute index %q has type %s; requested %s", name, existingType, expectedType),
			).ToGRPCError()
		}
	}
	return missing, nil
}

func validateRequestedIndexes(requested map[string]dexpb.IndexType) error {
	for name, indexType := range requested {
		if strings.TrimSpace(name) == "" {
			return serviceerrors.InvalidArgument(
				dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
				"attribute index name must not be empty",
			).ToGRPCError()
		}
		if indexType <= dexpb.IndexType_INDEX_TYPE_UNSPECIFIED ||
			indexType > dexpb.IndexType_INDEX_TYPE_DATETIME {
			return serviceerrors.InvalidArgument(
				dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
				fmt.Sprintf("attribute index %q has invalid type %s", name, indexType),
			).ToGRPCError()
		}
	}
	return nil
}

func waitForPoll(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextPollInterval(interval time.Duration) time.Duration {
	interval *= 2
	if interval > maximumPollInterval {
		return maximumPollInterval
	}
	return interval
}

func isRetryable(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	grpcCode := status.Code(err)
	if grpcCode != codes.Unknown {
		return grpcCode == codes.Unavailable || grpcCode == codes.DeadlineExceeded ||
			grpcCode == codes.ResourceExhausted || grpcCode == codes.Aborted
	}
	if !yarpcerrors.IsStatus(err) {
		return true
	}
	yarpcCode := yarpcerrors.ErrorCode(err)
	return yarpcCode == yarpcerrors.CodeUnknown || yarpcCode == yarpcerrors.CodeUnavailable ||
		yarpcCode == yarpcerrors.CodeDeadlineExceeded || yarpcCode == yarpcerrors.CodeResourceExhausted ||
		yarpcCode == yarpcerrors.CodeAborted
}

func isConcurrentRegistration(err error) bool {
	return status.Code(err) == codes.AlreadyExists ||
		(yarpcerrors.IsStatus(err) && yarpcerrors.ErrorCode(err) == yarpcerrors.CodeAlreadyExists)
}

func backendError(err error) error {
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return syncDeadlineError(err)
	}
	grpcCode := status.Code(err)
	if grpcCode == codes.Unknown && yarpcerrors.IsStatus(err) {
		grpcCode = grpcCodeFromYARPC(yarpcerrors.ErrorCode(err))
	}
	if grpcCode == codes.OK {
		grpcCode = codes.Internal
	}
	return serviceerrors.NewErrorAndStatus(
		grpcCode,
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
		err.Error(),
	).ToGRPCError()
}

func grpcCodeFromYARPC(code yarpcerrors.Code) codes.Code {
	switch code {
	case yarpcerrors.CodeCancelled:
		return codes.Canceled
	case yarpcerrors.CodeInvalidArgument:
		return codes.InvalidArgument
	case yarpcerrors.CodeDeadlineExceeded:
		return codes.DeadlineExceeded
	case yarpcerrors.CodeAlreadyExists:
		return codes.AlreadyExists
	case yarpcerrors.CodePermissionDenied:
		return codes.PermissionDenied
	case yarpcerrors.CodeResourceExhausted:
		return codes.ResourceExhausted
	case yarpcerrors.CodeFailedPrecondition:
		return codes.FailedPrecondition
	case yarpcerrors.CodeAborted:
		return codes.Aborted
	case yarpcerrors.CodeUnavailable:
		return codes.Unavailable
	case yarpcerrors.CodeUnauthenticated:
		return codes.Unauthenticated
	default:
		return codes.Unknown
	}
}

func syncDeadlineError(err error) error {
	return serviceerrors.NewErrorAndStatus(
		codes.DeadlineExceeded,
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
		fmt.Sprintf("attribute index synchronization timed out: %v", err),
	).ToGRPCError()
}
