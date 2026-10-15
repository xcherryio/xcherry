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
	"github.com/superdurable/dex/service/wake"
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

// NewWakeNotifier builds the Worker wake notifier.
func NewWakeNotifier(cfg *config.Config) *wake.Notifier {
	if cfg == nil {
		panic("NewWakeNotifier requires non-nil config sections")
	}
	zapLogger, err := cfg.Log.NewZapLogger()
	if err != nil {
		panic(fmt.Sprintf("create dispatch gate logger: %v", err))
	}
	return wake.NewNotifier(&cfg.Worker.Wake, newInternalToken(cfg), loggerimpl.NewLogger(zapLogger))
}

// newInternalToken returns nil when internal authentication is not configured.
func newInternalToken(cfg *config.Config) interface{ Token() string } {
	if cfg.Internal.TokenFile == "" {
		return nil
	}
	zapLogger, err := cfg.Log.NewZapLogger()
	if err != nil {
		panic(fmt.Sprintf("create internal token logger: %v", err))
	}
	internalToken, err := auth.NewInternalTokenFile(&cfg.Internal, loggerimpl.NewLogger(zapLogger))
	if err != nil {
		panic(fmt.Sprintf("read internal.tokenFile: %v", err))
	}
	return internalToken
}
