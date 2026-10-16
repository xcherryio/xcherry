// Copyright (c) 2022-2026 Super Durable, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package pullworkers

import "github.com/superdurable/dex/sdk-go/dex"

var greetingMessage = dex.DefineAttribute[string]("pull-greeting-message")

type GreetingFlow struct {
	dex.FlowDefaults
}

func (*GreetingFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(greetStep{})}
}

func (flow *GreetingFlow) GetRPCs() []dex.RPCDef {
	return []dex.RPCDef{dex.DefineRPC(flow.GetGreeting, nil)}
}

func (*GreetingFlow) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{Attributes: []dex.AttributeDef{greetingMessage}}
}

func (*GreetingFlow) GetGreeting(ctx dex.Context, _ dex.None) (*dex.RPCResult[string], error) {
	message, err := greetingMessage.Get(ctx)
	if err != nil {
		return nil, err
	}
	return &dex.RPCResult[string]{Output: message}, nil
}

type greetStep struct {
	dex.StepDefaultsNoWaitFor[string]
}

func (greetStep) Execute(ctx dex.Context, name string) (*dex.StepDecision, error) {
	message := "Hello, " + name + "!"
	if err := greetingMessage.Set(ctx, message); err != nil {
		return nil, err
	}
	return dex.GracefulComplete(message), nil
}

var _ dex.Flow = (*GreetingFlow)(nil)
