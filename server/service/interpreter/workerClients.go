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
		internalServiceTarget(cfg),
		cfg,
		newInternalToken(cfg),
	)
	if err != nil {
		panic(fmt.Sprintf("create internal service client: %v", err))
	}
	return internalService
}

// newInternalToken returns nil in single-tenant mode, where InternalService needs no token.
func newInternalToken(cfg *config.Config) *tenancy.InternalTokenFile {
	if !cfg.Tenancy.IsMultiTenant() {
		return nil
	}
	zapLogger, err := cfg.Log.NewZapLogger()
	if err != nil {
		panic(fmt.Sprintf("create internal token logger: %v", err))
	}
	internalToken, err := tenancy.NewInternalTokenFile(cfg.Tenancy.Internal.TokenFile, loggerimpl.NewLogger(zapLogger))
	if err != nil {
		panic(fmt.Sprintf("read tenancy.internal.tokenFile: %v", err))
	}
	return internalToken
}

func internalServiceTarget(cfg *config.Config) string {
	if cfg.Interpreter.InterpreterActivityConfig.InternalServiceTarget != "" {
		return cfg.Interpreter.InterpreterActivityConfig.InternalServiceTarget
	}
	if cfg.Tenancy.IsMultiTenant() {
		return cfg.Tenancy.Internal.Listen
	}
	port := cfg.Api.Port
	if port == 0 {
		port = config.DefaultApiPort
	}
	return fmt.Sprintf("localhost:%d", port)
}
