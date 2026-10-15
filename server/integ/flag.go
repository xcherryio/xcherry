// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package integ

import (
	"flag"
)

var repeatIntegTest = flag.Int("repeat", 1, "the number of repeat")

var repeatInterval = flag.Int("intervalMs", 0, "the interval between test in milliseconds")

var cadenceIntegTest = flag.Bool("cadence", true, "run integ test against cadence")

var temporalIntegTest = flag.Bool("temporal", true, "run integ test against temporal")

var testSearchIntegTest = flag.Bool("search", true, "run search integ test against temporal/Cadence")

var searchWaitTimeIntegTest = flag.Int("searchWaitMs", 2000, "the amount of time to wait for ElasticSearch being able to search in milliseconds")

var temporalHostPort = flag.String("temporalHostPort", "", "temporal host port")

var dexServerAddress = flag.String("dexServerAddress", "", "existing Dex gRPC address")

var dependencyWaitSeconds = flag.Int("dependencyWaitSeconds", 60, "the number of seconds waiting for dependencies to be up(Cadence/Temporal)")

var disableStickyCache = flag.Bool("disableStickyCache", false, "disable Temporal/Cadence sticky execution")

func init() {
	flag.StringVar(&testNamespace, "temporalNamespace", "default", "Temporal namespace for integration tests")
}
