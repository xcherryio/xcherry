// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package config

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

func rejectRemovedTenancyConfig(document *yaml.Node) error {
	if len(document.Content) == 0 {
		return nil
	}
	root := document.Content[0]
	if mappingValueForKey(root, "tenancy") != nil {
		return removedTenancyConfigError("tenancy")
	}
	if blobStore := mappingValueForKey(root, "blobStore"); blobStore != nil {
		if mappingValueForKey(blobStore, "tenantCleanupConfirmationDelay") != nil {
			return removedTenancyConfigError("blobStore.tenantCleanupConfirmationDelay")
		}
	}
	if attributeStore := mappingValueForKey(root, "attributeStore"); attributeStore != nil {
		if stores := mappingValueForKey(attributeStore, "stores"); stores != nil && stores.Kind == yaml.MappingNode {
			for index := 0; index+1 < len(stores.Content); index += 2 {
				if mappingValueForKey(stores.Content[index+1], "tenantColumn") != nil {
					return removedTenancyConfigError("attributeStore.stores." + stores.Content[index].Value + ".tenantColumn")
				}
			}
		}
	}
	return nil
}

func mappingValueForKey(mapping *yaml.Node, key string) *yaml.Node {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return nil
	}
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			return mapping.Content[index+1]
		}
	}
	return nil
}

func removedTenancyConfigError(path string) error {
	return fmt.Errorf("%s: multi-tenancy was removed; move tenancy.authenticators to auth.authenticators, tenancy.internal to internal, and tenancy.wake to worker.wake; drop subjectPattern and claims, and keep only sha256 entries in static token files", path)
}
