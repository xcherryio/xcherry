// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package service

import (
	"fmt"
	"strings"
	"time"

	"github.com/superdurable/dex/gen/dexpb"
)

// flowIDReservedCharacters are rejected from application-created Flow IDs and Step types.
//
// "/" separates object-key path segments. Reserving it keeps every Flow's blobs in one directory
// and preserves local and remote blob listing and cleanup boundaries.
//
// "$" separates the UTC date prefix from the escaped Flow ID in `<yymmdd>$<escapedFlowID>/<blobID>`.
// Reserving it keeps date-scoped listing and cleanup prefixes unambiguous.
//
// ":" identifies server-generated Flow namespaces such as `SubFlow:`. Reserving it prevents an
// application from pre-creating an internal Flow ID and interfering with SubFlow reuse resolution.
// Server-generated Flows bypass ValidateFlowID.
//
// Step types follow the same rules because Dex embeds a parent Step execution ID in each generated
// SubFlow ID. Allowing any reserved character there would reintroduce the same ambiguity indirectly.
var flowIDReservedCharacters = []string{"/", "$", ":"}

// ValidateFlowID rejects characters reserved for Dex identities and blob paths.
func ValidateFlowID(flowID string) error {
	return validateIdentifierCharacters("flow ID", flowID)
}

// ValidateStepType rejects characters reserved for generated Flow identities and blob paths.
func ValidateStepType(stepType string) error {
	return validateIdentifierCharacters("step type", stepType)
}

// ValidateStepOptions validates reachable Step options against the heartbeat minimum.
func ValidateStepOptions(options *dexpb.StepOptions, minimumHeartbeatTimeout time.Duration) error {
	for current := options; current != nil; current = current.GetExecuteFailureProceedStepOptions() {
		if err := ValidateStepType(current.GetExecuteFailureProceedStepType()); err != nil {
			return err
		}
		heartbeatTimeout := current.GetHeartbeatTimeoutSeconds()
		if heartbeatTimeout < 0 || heartbeatTimeout > 0 &&
			time.Duration(heartbeatTimeout)*time.Second < minimumHeartbeatTimeout {
			return fmt.Errorf("heartbeat timeout must be zero or at least %s", minimumHeartbeatTimeout)
		}
		if _, err := ValidateAndSortStateSelections(
			current.GetWaitForLoadAttributeMapInstances(),
			current.GetWaitForLoadChannelNames(),
			current.GetWaitForLoadChannelMapInstances(),
		); err != nil {
			return fmt.Errorf("WaitFor %w", err)
		}
		if _, err := ValidateAndSortStateSelections(
			current.GetExecuteLoadAttributeMapInstances(),
			current.GetExecuteLoadChannelNames(),
			current.GetExecuteLoadChannelMapInstances(),
		); err != nil {
			return fmt.Errorf("Execute %w", err)
		}
	}
	return nil
}

func validateIdentifierCharacters(identifierName string, value string) error {
	for _, reservedCharacter := range flowIDReservedCharacters {
		if strings.Contains(value, reservedCharacter) {
			return fmt.Errorf("%s contains reserved character %q", identifierName, reservedCharacter)
		}
	}
	return nil
}
