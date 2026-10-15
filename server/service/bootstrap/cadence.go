// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package bootstrap

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/superdurable/dex/service"
	uclient "github.com/superdurable/dex/service/client"
	cadenceapi "github.com/superdurable/dex/service/client/cadence"
	"github.com/superdurable/dex/service/common/blobstore"
	dexconverter "github.com/superdurable/dex/service/common/converter"
	"github.com/superdurable/dex/service/interpreter/cadence"
	adminv1 "github.com/uber/cadence-idl/go/proto/admin/v1"
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	"go.temporal.io/sdk/client"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	cadenceclient "go.uber.org/cadence/client"
	"go.uber.org/cadence/compatibility"
	"go.uber.org/cadence/encoded"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/grpc"
)

const DefaultCadenceDomain = "default"
const DefaultCadenceHostPort = "127.0.0.1:7833"

const cadenceFrontendService = "cadence-frontend"
const cadenceClientName = "cadence-client"

func (r *Runtime) createCadenceServices() (
	uclient.UnifiedClient,
	blobstore.BlobStore,
	interpreterWorker,
	error,
) {
	hostPort := r.cfg.Interpreter.Cadence.HostPort
	if hostPort == "" {
		hostPort = DefaultCadenceHostPort
	}
	domain := r.cfg.Interpreter.Cadence.Domain
	if domain == "" {
		domain = DefaultCadenceDomain
	}
	serviceClient, adminClient, closeServiceClient, err := BuildCadenceServiceClient(hostPort)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("connect to Cadence: %w", err)
	}
	var closeOnce sync.Once
	closeClient := func() {
		closeOnce.Do(closeServiceClient)
	}
	dataConverter := dexconverter.NewCadenceDataConverter()
	cadenceClient, err := BuildCadenceClient(serviceClient, domain, dataConverter)
	if err != nil {
		closeClient()
		return nil, nil, nil, fmt.Errorf("create Cadence client: %w", err)
	}
	unifiedClient := cadenceapi.NewCadenceClient(
		domain,
		cadenceClient,
		serviceClient,
		adminClient,
		r.cfg.Interpreter.Cadence.AdminSecurityToken,
		dataConverter,
		closeClient,
		r.cfg.Api.QueryWorkflowFailedRetryPolicy,
	)
	store, err := r.createBlobStore(context.Background(), domain, client.MetricsNopHandler)
	if err != nil {
		unifiedClient.Close()
		return nil, nil, nil, err
	}
	if !r.options.Services.Interpreter {
		return unifiedClient, store, nil, nil
	}
	worker := cadence.NewInterpreterWorker(
		r.cfg,
		serviceClient,
		domain,
		service.TaskQueue,
		closeClient,
		dataConverter,
		unifiedClient,
		store,
		r.attributeStore,
		r.workerPool,
		r.streamStore,
		client.MetricsNopHandler,
		r.pullDispatcher,
	)
	return unifiedClient, store, worker, nil
}

func BuildCadenceClient(
	serviceClient workflowserviceclient.Interface,
	domain string,
	dataConverter encoded.DataConverter,
) (cadenceclient.Client, error) {
	return cadenceclient.NewClient(
		serviceClient,
		domain,
		&cadenceclient.Options{
			DataConverter: dataConverter,
			FeatureFlags: cadenceclient.FeatureFlags{
				WorkflowExecutionAlreadyCompletedErrorEnabled: true,
			},
		},
	), nil
}

func BuildCadenceServiceClient(
	hostPort string,
) (workflowserviceclient.Interface, adminv1.AdminAPIYARPCClient, func(), error) {
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: cadenceClientName,
		Outbounds: yarpc.Outbounds{
			cadenceFrontendService: {Unary: grpc.NewTransport().NewSingleOutbound(hostPort)},
		},
	})
	if err := dispatcher.Start(); err != nil {
		return nil, nil, nil, fmt.Errorf("start Cadence transport: %w", err)
	}
	clientConfig := dispatcher.ClientConfig(cadenceFrontendService)
	return compatibility.NewThrift2ProtoAdapter(
			apiv1.NewDomainAPIYARPCClient(clientConfig),
			apiv1.NewWorkflowAPIYARPCClient(clientConfig),
			apiv1.NewWorkerAPIYARPCClient(clientConfig),
			apiv1.NewVisibilityAPIYARPCClient(clientConfig),
		), adminv1.NewAdminAPIYARPCClient(clientConfig), func() {
			if err := dispatcher.Stop(); err != nil {
				log.Printf("stop Cadence transport: %v", err)
			}
		}, nil
}
