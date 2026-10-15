// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package interpreter

import (
	"fmt"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/service/auth"
	"github.com/superdurable/dex/service/common/log/loggerimpl"
	"github.com/superdurable/dex/service/common/workerclient"
	"github.com/superdurable/dex/service/tenancy"
)

// NewInternalServiceClient builds the continue-as-new dump client.
func NewInternalServiceClient(cfg *config.Config) *workerclient.InternalServiceClient {
	if cfg == nil {
		panic("NewInternalServiceClient requires non-nil config sections")
	}
	internalService, err := workerclient.NewInternalServiceClient(
		cfg.GetInternalServiceTargetWithDefault(),
		cfg,
		newInternalToken(cfg),
	)
	if err != nil {
		panic(fmt.Sprintf("create internal service client: %v", err))
	}
	return internalService
}

// NewDispatchGate builds the gate that admits tenant dispatches; it admits everything in single-tenant mode.
func NewDispatchGate(cfg *config.Config) *tenancy.DispatchGate {
	var internalToken *tenancy.InternalTokenFile
	if cfg.Tenancy.Wake.URL != "" {
		if cfg.Tenancy.IsMultiTenant() {
			internalToken = newInternalToken(cfg).(*tenancy.InternalTokenFile)
		}
	}
	zapLogger, err := cfg.Log.NewZapLogger()
	if err != nil {
		panic(fmt.Sprintf("create dispatch gate logger: %v", err))
	}
	gate, err := tenancy.NewDispatchGateWithWake(&cfg.Tenancy, &cfg.Worker.Wake, internalToken, newInternalToken(cfg), loggerimpl.NewLogger(zapLogger))
	if err != nil {
		panic(fmt.Sprintf("create dispatch gate: %v", err))
	}
	return gate
}

// newInternalToken returns nil when neither internal authentication nor a legacy tenant token is configured.
func newInternalToken(cfg *config.Config) interface{ Token() string } {
	if !cfg.Tenancy.IsMultiTenant() && cfg.Internal.TokenFile == "" {
		return nil
	}
	zapLogger, err := cfg.Log.NewZapLogger()
	if err != nil {
		panic(fmt.Sprintf("create internal token logger: %v", err))
	}
	if !cfg.Tenancy.IsMultiTenant() {
		internalToken, err := auth.NewInternalTokenFile(&cfg.Internal, loggerimpl.NewLogger(zapLogger))
		if err != nil {
			panic(fmt.Sprintf("read internal.tokenFile: %v", err))
		}
		return internalToken
	}
	internalToken, err := tenancy.NewInternalTokenFile(cfg.Tenancy.Internal.TokenFile, loggerimpl.NewLogger(zapLogger))
	if err != nil {
		panic(fmt.Sprintf("read tenancy.internal.tokenFile: %v", err))
	}
	return internalToken
}
