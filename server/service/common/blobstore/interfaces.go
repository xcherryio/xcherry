// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package blobstore

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	StepEventInputMethodWaitFor = "wait_for"
	StepEventInputMethodExecute = "execute"
)

var (
	blobReferenceDateEpoch   = time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	blobReferenceMaximumDate = time.Date(2099, time.December, 31, 0, 0, 0, 0, time.UTC)
)

type WorkflowPath struct {
	// FlowID is the stored Flow ID.
	FlowID string
	RunID  string
}

func ParseWorkflowPath(workflowPath string) (WorkflowPath, error) {
	parts := strings.Split(workflowPath, "$")
	if len(parts) != 2 && len(parts) != 3 {
		return WorkflowPath{}, fmt.Errorf("invalid workflow path: %s", workflowPath)
	}
	if _, err := parseBlobStorageDate(parts[0]); err != nil {
		return WorkflowPath{}, fmt.Errorf("invalid workflow path date: %w", err)
	}
	flowID, err := decodeFlowIDPathPart(parts[1])
	if err != nil {
		return WorkflowPath{}, fmt.Errorf("decode flow ID: %w", err)
	}
	parsed := WorkflowPath{FlowID: flowID}
	if len(parts) == 3 {
		if parsed.RunID, err = decodeOpaquePathPart(parts[2]); err != nil {
			return WorkflowPath{}, fmt.Errorf("decode run ID: %w", err)
		}
	}
	return parsed, nil
}

// workflowDirectory returns <yymmdd>$<escaped Flow ID>.
func workflowDirectory(storageDate string, flowID string) string {
	return storageDate + "$" + encodeFlowIDPathPart(flowID)
}

// StepEventInputPath returns the path of a Step event input.
func StepEventInputPath(
	runStarted time.Time,
	flowID string,
	runID string,
	stepExecutionID string,
	method string,
) string {
	return stepEventInputPath(runStarted, flowID, runID, stepExecutionID, method)
}

func stepEventInputPath(
	runStarted time.Time,
	flowID string,
	runID string,
	stepExecutionID string,
	method string,
) string {
	workflowPath := workflowDirectory(formatBlobStorageDate(runStarted), flowID) + "$" +
		encodeOpaquePathPart(runID)
	return strings.Join([]string{workflowPath, encodeOpaquePathPart(stepExecutionID), method + ".pb"}, "/")
}

// ValueObjectPath returns the path of a value object.
func ValueObjectPath(flowID string, locator string) (string, error) {
	return valueObjectPath(flowID, locator)
}

func valueObjectPath(flowID string, locator string) (string, error) {
	if flowID == "" {
		return "", fmt.Errorf("Blob locator requires a Flow ID")
	}
	parts := strings.Split(locator, "/")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid Blob locator %q", locator)
	}
	writeDate, err := parseBlobReferenceDate(parts[0])
	if err != nil {
		return "", fmt.Errorf("invalid Blob locator date: %w", err)
	}
	objectID := parts[1]
	if objectID == "" {
		return "", fmt.Errorf("invalid empty Blob object ID")
	}
	for _, character := range objectID {
		if (character < '0' || character > '9') && (character < 'a' || character > 'z') {
			return "", fmt.Errorf("invalid Blob object ID %q", objectID)
		}
	}
	return workflowDirectory(formatBlobStorageDate(writeDate), flowID) + "/" + objectID, nil
}

func encodeFlowIDPathPart(flowID string) string {
	var encoded strings.Builder
	for _, character := range []byte(flowID) {
		if isReadableFlowIDPathByte(character) {
			encoded.WriteByte(character)
			continue
		}
		encoded.WriteByte('%')
		encoded.WriteByte("0123456789ABCDEF"[character>>4])
		encoded.WriteByte("0123456789ABCDEF"[character&0x0f])
	}
	return encoded.String()
}

func decodeFlowIDPathPart(encodedFlowID string) (string, error) {
	if encodedFlowID == "" {
		return "", fmt.Errorf("empty Flow ID path part")
	}
	decoded := make([]byte, 0, len(encodedFlowID))
	for index := 0; index < len(encodedFlowID); {
		character := encodedFlowID[index]
		if isReadableFlowIDPathByte(character) {
			decoded = append(decoded, character)
			index++
			continue
		}
		if character != '%' || index+2 >= len(encodedFlowID) {
			return "", fmt.Errorf("invalid Flow ID path part %q", encodedFlowID)
		}
		high, isHighHex := uppercaseHexValue(encodedFlowID[index+1])
		low, isLowHex := uppercaseHexValue(encodedFlowID[index+2])
		if !isHighHex || !isLowHex {
			return "", fmt.Errorf("invalid Flow ID path part %q", encodedFlowID)
		}
		decoded = append(decoded, high<<4|low)
		index += 3
	}
	if !utf8.Valid(decoded) {
		return "", fmt.Errorf("Flow ID path part is not UTF-8")
	}
	flowID := string(decoded)
	if encodeFlowIDPathPart(flowID) != encodedFlowID {
		return "", fmt.Errorf("non-canonical Flow ID path part %q", encodedFlowID)
	}
	return flowID, nil
}

func isReadableFlowIDPathByte(character byte) bool {
	return character >= '0' && character <= '9' ||
		character >= 'A' && character <= 'Z' ||
		character >= 'a' && character <= 'z' ||
		character == '-' || character == '_'
}

func uppercaseHexValue(character byte) (byte, bool) {
	switch {
	case character >= '0' && character <= '9':
		return character - '0', true
	case character >= 'A' && character <= 'F':
		return character - 'A' + 10, true
	default:
		return 0, false
	}
}

func encodeOpaquePathPart(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func decodeOpaquePathPart(value string) (string, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}

func formatBlobReferenceDate(timestamp time.Time) (string, error) {
	utcTimestamp := timestamp.UTC()
	writeDate := time.Date(
		utcTimestamp.Year(), utcTimestamp.Month(), utcTimestamp.Day(), 0, 0, 0, 0, time.UTC,
	)
	if writeDate.Before(blobReferenceDateEpoch) || writeDate.After(blobReferenceMaximumDate) {
		return "", fmt.Errorf("Blob reference date %s is outside 2026-09-01 through 2099-12-31", writeDate.Format(time.DateOnly))
	}
	dayOffset := int64(writeDate.Sub(blobReferenceDateEpoch) / (24 * time.Hour))
	return strconv.FormatInt(dayOffset, 36), nil
}

func parseBlobReferenceDate(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, fmt.Errorf("empty Blob reference date")
	}
	if len(value) > 1 && value[0] == '0' {
		return time.Time{}, fmt.Errorf("Blob reference date %q has a leading zero", value)
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'z') {
			return time.Time{}, fmt.Errorf("invalid Blob reference date %q", value)
		}
	}
	dayOffset, err := strconv.ParseInt(value, 36, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse Blob reference date %q: %w", value, err)
	}
	maximumDayOffset := int64(blobReferenceMaximumDate.Sub(blobReferenceDateEpoch) / (24 * time.Hour))
	if dayOffset > maximumDayOffset {
		return time.Time{}, fmt.Errorf("Blob reference date %q exceeds 2099-12-31", value)
	}
	return blobReferenceDateEpoch.AddDate(0, 0, int(dayOffset)), nil
}

func formatBlobStorageDate(timestamp time.Time) string {
	return timestamp.UTC().Format("060102")
}

func parseBlobStorageDate(value string) (time.Time, error) {
	return time.Parse("20060102", "20"+value)
}

type BlobStore interface {
	Close() error
	// WriteObject stores data under the Flow-owned path and returns its compact locator.
	WriteObject(ctx context.Context, flowID, invocationID string, data []byte) (storeID, locator string, err error)
	// ReadObject resolves a compact locator under its owning Flow.
	ReadObject(ctx context.Context, storeID, flowID, locator string) ([]byte, error)
	WriteStepEventInput(
		ctx context.Context,
		runStarted time.Time,
		flowID string,
		runID string,
		stepExecutionID string,
		method string,
		data []byte,
	) error
	ReadStepEventInput(
		ctx context.Context,
		runStarted time.Time,
		flowID string,
		runID string,
		stepExecutionID string,
		method string,
	) ([]byte, bool, error)
	// DeleteWorkflowObjects will delete all the objects of the workflowId
	// workflowPath is yymmdd$escapedFlowId, where yymmdd is needed to compose the path,
	DeleteWorkflowObjects(ctx context.Context, storeId, workflowPath string) error
	// ListWorkflowPaths will list the workflowPaths ( yymmdd$escapedFlowId ) as CommonPrefixes from S3
	// It uses of delimiter "/" before the object ID to get all the CommonPrefixes
	ListWorkflowPaths(ctx context.Context, input ListObjectPathsInput) (*ListObjectPathsOutput, error)
	// CountWorkflowObjectsForTesting is for testing ONLY.
	// count the number of S3 objects for this workflowId
	// Limitation:
	//  1. It doesn't count across two days(so expect test to fail if you happen to run the test across day boundary :)
	//  2. Only count less than 1000 objects(because it only make one API call to S3 which return at most 1000 objects)
	CountWorkflowObjectsForTesting(ctx context.Context, flowID string) (int64, error)
}

type ListObjectPathsInput struct {
	StoreId           string
	ContinuationToken *string
}

type ListObjectPathsOutput struct {
	ContinuationToken *string
	// WorkflowPaths are relative to the namespace.
	WorkflowPaths []string
}
