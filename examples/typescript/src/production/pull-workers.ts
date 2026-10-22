/*
 * Copyright (c) 2022-2026 Super Durable, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  Attribute,
  Client,
  Registry,
  StepList,
  Worker,
  gracefulComplete,
  rpc,
  stringCodec,
  type BlobCache,
  type Context,
  type Flow,
  type PersistenceSchema,
  type RPCResult,
  type Step,
  type StepDecision,
} from "@superdurable/dex";

const greetingMessage = new Attribute("pull-greeting-message", stringCodec);

class GreetStep implements Step<string> {
  public readonly inputCodec = stringCodec;

  public getStepType(): string {
    return "GreetStep";
  }

  public execute(context: Context, name: string): StepDecision {
    const message = `Hello, ${name}!`;
    greetingMessage.set(context, message);
    return gracefulComplete(message);
  }
}

export class GreetingFlow implements Flow<string> {
  private readonly greetStep = new GreetStep();

  public getFlowType(): string {
    return "GreetingFlow";
  }

  public getSteps() {
    return StepList.startStep(this.greetStep);
  }

  public getPersistenceSchema(): PersistenceSchema {
    return { attributes: [greetingMessage] };
  }

  @rpc({ outputCodec: stringCodec })
  public getGreeting(context: Context): RPCResult<string> {
    return { output: greetingMessage.get(context) };
  }
}

export async function startPullGreeting(
  cache: BlobCache,
  serverAddress: string,
): Promise<{ flow: GreetingFlow; worker: Worker; client: Client }> {
  const flow = new GreetingFlow();
  const registry = new Registry([flow]);
  const worker = new Worker(registry, cache, {
    serverAddress,
    pull: {
      maxConcurrentTasks: 8,
      // Keep one slot for RPCs so Flows stay responsive.
      reservedRpcTasks: 1,
    },
  });
  try {
    await worker.start();
    await worker.waitPullRegistered({ timeoutMs: 30_000 });
  } catch (failure) {
    try {
      await worker.close();
    } catch (cleanupFailure) {
      throw new AggregateError([failure, cleanupFailure], "pull Worker startup and cleanup failed");
    }
    throw failure;
  }
  const client = new Client(registry, cache, {
    serverAddress,
    workerTarget: worker.workerTarget,
  });
  return { flow, worker, client };
}

export async function stopPullGreeting(worker: Worker, client: Client): Promise<void> {
  await client.close();
  await worker.close();
}

export function isPullGreetingReady(worker: Worker): boolean {
  return worker.pullRegistrationState().isReady;
}

export interface PullGreetingDiagnostics {
  readonly isReady: boolean;
  readonly reservedRpcDeclines: number;
}

export function getPullGreetingDiagnostics(worker: Worker): PullGreetingDiagnostics {
  return {
    isReady: worker.pullRegistrationState().isReady,
    reservedRpcDeclines: worker.pullReservedRpcDeclines,
  };
}
