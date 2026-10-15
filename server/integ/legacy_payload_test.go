// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package integ

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestV190TenantPayloadsRemainDecodable(t *testing.T) {
	// Fixtures were encoded with protos/dex.proto from server/v1.9.0.
	fixtures := []struct {
		filename    string
		message     proto.Message
		knownField  protoreflect.FieldNumber
		tenantField protoreflect.FieldNumber
		claimsField protoreflect.FieldNumber
	}{
		{"interpreter-workflow-input.pb", &dexpb.InterpreterWorkflowInput{}, 1, 12, 13},
		{"invoke-wait-for-input.pb", &dexpb.InvokeWaitForMethodActivityInput{}, 1, 3, 4},
		{"invoke-execute-input.pb", &dexpb.InvokeExecuteMethodActivityInput{}, 1, 3, 4},
		{"invoke-worker-rpc-input.pb", &dexpb.InvokeWorkerRPCActivityInput{}, 2, 3, 4},
		{"start-sub-flow-input.pb", &dexpb.StartSubFlowActivityInput{}, 3, 4, 5},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.filename, func(t *testing.T) {
			serializedPayload, err := os.ReadFile(filepath.Join("testdata", "legacy-tenancy", fixture.filename))
			require.NoError(t, err)
			require.NoError(t, proto.Unmarshal(serializedPayload, fixture.message))
			messageReflection := fixture.message.ProtoReflect()
			require.True(t, messageReflection.Has(messageReflection.Descriptor().Fields().ByNumber(fixture.knownField)))
			require.Nil(t, messageReflection.Descriptor().Fields().ByNumber(fixture.tenantField))
			require.Nil(t, messageReflection.Descriptor().Fields().ByNumber(fixture.claimsField))
			unknownFields := messageReflection.GetUnknown()
			foundFieldNumbers := map[protowire.Number]bool{}
			for len(unknownFields) > 0 {
				fieldNumber, _, consumed := protowire.ConsumeField(unknownFields)
				require.Greater(t, consumed, 0)
				foundFieldNumbers[fieldNumber] = true
				unknownFields = unknownFields[consumed:]
			}
			require.True(t, foundFieldNumbers[protowire.Number(fixture.tenantField)])
			require.True(t, foundFieldNumbers[protowire.Number(fixture.claimsField)])
		})
	}
}
