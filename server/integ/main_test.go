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
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/bootstrap"
	"github.com/superdurable/dex/service/common/ptr"
	"go.temporal.io/sdk/client"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
)

func TestMain(m *testing.M) {
	if os.Getenv("DEX_INTEG_FAKE_BINARY") == "1" {
		os.Exit(runFakeBinaryDex())
	}
	flag.Parse()
	var err error

	if len(os.Args) > 0 {
		lastArg := os.Args[len(os.Args)-1]
		// check if lastArg is the regex  pattern of ^\QTest.*Temporal.*\E$
		matched, err := regexp.MatchString(`Test.*Temporal.*`, lastArg)
		if err == nil && matched {
			*temporalIntegTest = true
			*cadenceIntegTest = false
		} else {
			matched, err := regexp.MatchString(`Test.*Cadence.*`, lastArg)
			if err == nil && matched {
				*temporalIntegTest = false
				*cadenceIntegTest = true
			}
		}
	}

	fmt.Println("*temporalIntegTest, *cadenceIntegTest", *temporalIntegTest, *cadenceIntegTest)
	if *integWorkerMode != "" && *integWorkerMode != "push" && *integWorkerMode != "pull" {
		log.Fatal("integ-worker-mode must be push or pull")
	}
	if *dexServerBinary != "" {
		if *dexServerEngine != "zd" || *dexServerAddress != "" {
			log.Fatal("binary mode requires -dex-server-engine=zd and no dexServerAddress")
		}
		*integWorkerMode = "pull"
		*cadenceIntegTest = false
		code := m.Run()
		os.Exit(code)
	}
	if *dexServerAddress != "" && (!*temporalIntegTest || *cadenceIntegTest) {
		log.Fatal("dexServerAddress requires Temporal-only integration tests")
	}

	if *temporalIntegTest {
		var temporalClient client.Client
		for i := 0; i < *dependencyWaitSeconds; i++ {
			temporalClient, err = client.Dial(client.Options{
				HostPort:  *temporalHostPort,
				Namespace: testNamespace,
			})
			if err != nil {
				fmt.Println("wait for Temporal to be up...last err: ", err)
				time.Sleep(time.Second)

			} else {
				break
			}
		}
		if err != nil {
			log.Fatalf("unable to connect to Temporal %v", err)
		}
		fmt.Println("connected to Temporal namespace")
		if *dexServerAddress != "" {
			dependencyCtx, cancelDependency := context.WithTimeout(
				context.Background(),
				time.Duration(*dependencyWaitSeconds)*time.Second,
			)
			err = prepareExternalDex(dependencyCtx)
			cancelDependency()
			if err != nil {
				temporalClient.Close()
				log.Fatalf("external Dex test setup failed: %v", err)
			}
			fmt.Println("connected to external Dex server", *dexServerAddress)
		}
		temporalClient.Close()
		if *dependencyWaitSeconds > 0 {
			time.Sleep(time.Second * 1) // see https://github.com/temporalio/temporal/issues/4160
		}
	}

	if *cadenceIntegTest {
		for i := 0; i < *dependencyWaitSeconds; i++ {
			_, _, _, err = bootstrap.BuildCadenceServiceClient(bootstrap.DefaultCadenceHostPort)
			if err != nil {
				fmt.Println("wait for Cadence to be up...last err: ", err)
				time.Sleep(time.Second)
			} else {
				break
			}
		}
		if err != nil {
			log.Fatalf("cannot connnect to Cadence service%v", err)
		}
		fmt.Println("connected to Cadence service")

		var closeFunc func()
		var serviceClient workflowserviceclient.Interface
		serviceClient, _, closeFunc, err = bootstrap.BuildCadenceServiceClient(bootstrap.DefaultCadenceHostPort)
		for i := 0; i < *dependencyWaitSeconds; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			_, err = serviceClient.DescribeDomain(ctx, &shared.DescribeDomainRequest{
				Name: ptr.Any(bootstrap.DefaultCadenceDomain),
			})
			cancel()
			if err != nil {
				fmt.Println("wait for Cadence domain to be ready...", err)
				time.Sleep(time.Second)
			} else {
				break
			}
		}
		if err != nil {
			log.Fatalf("Cadence service is not ready %v", err)
		}
		fmt.Println("Cadence service is now ready")
		closeFunc()
	}

	// disable caching for now as it makes it difficult to test memo
	//var integCadenceUclientCloseFunc, integTemporalUclientCloseFunc func()
	//if !(*cadenceIntegTest && *temporalIntegTest) {
	//	// hack for ci test to save the performance
	//	// only start connection once
	//	// TODO need to do this for outside of ci as well
	//	if *cadenceIntegTest {
	//		integCadenceUclientCached, integCadenceUclientCloseFunc = doStartDexServiceWithClient(service.BackendTypeCadence)
	//		defer func() {
	//			fmt.Println("shutdown cadence client and dex server")
	//			integCadenceUclientCloseFunc()
	//		}()
	//		fmt.Println("cached cadence client and dex server")
	//	} else {
	//		integTemporalUclientCached, integTemporalUclientCloseFunc = doStartDexServiceWithClient(service.BackendTypeTemporal)
	//		defer func() {
	//			fmt.Println("shutdown temporal client and dex server")
	//			integTemporalUclientCloseFunc()
	//		}()
	//		fmt.Println("cached temporal client and dex server")
	//	}
	//}

	code := m.Run()
	fmt.Println("finished running integ test with status code", code)
	os.Exit(code)
}

func newRequestID() string {
	return uuid.NewString()
}

func userActiveStepExecutions(
	state *dexpb.GetFlowStateResponse,
) []*dexpb.ActiveStepExecutionState {
	userExecutions := make([]*dexpb.ActiveStepExecutionState, 0, len(state.GetActiveStepExecutions()))
	for _, execution := range state.GetActiveStepExecutions() {
		if execution.GetStepType() != service.FlowTimeoutStepType {
			userExecutions = append(userExecutions, execution)
		}
	}
	return userExecutions
}
