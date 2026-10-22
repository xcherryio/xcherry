// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { renderToStaticMarkup } from 'react-dom/server';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it } from 'vitest';
import { PreferencesProvider } from '@/app/providers';
import type { FlowHistoryEvent } from '@/lib/types';
import {
  ASYNC_STEP_INPUT_SNAPSHOT_NOT_RECORDED,
  STEP_INPUT_SNAPSHOT_UNAVAILABLE,
} from '@/lib/unavailable';
import { eventTitle, eventTypeLabel, SemanticEventDetails } from './EventDetails';

function executeEvent(durability: number): FlowHistoryEvent {
  return {
    eventId: 12,
    eventTime: '2026-08-05T23:44:31.704507592Z',
    type: 'StepExecuteCompleted',
    payload: {
      input: {
        stepInput: { stringValue: 'charge' },
        conditionResults: { timerResults: [{ conditionId: 'timer-1' }] },
        attributes: [{ key: 'account', value: { stringValue: 'primary' } }],
        stepExecutionLocals: [{ key: 'attempted', value: { boolValue: true } }],
      },
      output: {
        stepDecision: { nextSteps: [{ stepType: 'complete' }] },
      },
      context: {
        stepExecutionId: 'charge-1',
        fromStepExecutionId: 'authorize-1',
        stepType: 'charge',
        durability,
        finalAttempt: 2,
        startedTime: '2026-08-05T23:44:31.700000000Z',
        duration: '0.002844876s',
        methodOptions: {
          timeoutSeconds: 10,
          retryPolicy: { maximumAttempts: 3 },
        },
      },
    },
  };
}

function waitForEvent(waitForCondition?: Record<string, unknown>): FlowHistoryEvent {
  const event = executeEvent(1);
  event.type = 'StepWaitForCompleted';
  event.payload.output = waitForCondition ? { waitForCondition } : {};
  return event;
}

function renderDetails(
  event: FlowHistoryEvent,
  history: FlowHistoryEvent[] = [event],
): string {
  return renderToStaticMarkup(
    <MemoryRouter>
      <PreferencesProvider>
        <SemanticEventDetails event={event} history={history} parentFlowId="parent" />
      </PreferencesProvider>
    </MemoryRouter>,
  );
}

function expectStrongLabelsInOrder(markup: string, labels: string[]): void {
  let previousPosition = -1;
  for (const label of labels) {
    const position = markup.indexOf(`>${label}</strong>`);
    expect(position).toBeGreaterThan(previousPosition);
    previousPosition = position;
  }
}

describe('selected step event details', () => {
  it('links a Time Travel fork to its previous run', () => {
    const fork: FlowHistoryEvent = {
      eventId: 9,
      eventTime: '2026-08-05T23:44:30Z',
      type: 'TimeTravelFork',
      payload: { previousRunId: 'previous-run-id' },
    };

    const markup = renderDetails(fork);

    expect(eventTitle(fork)).toBe('Time Travel fork');
    expect(eventTypeLabel(fork)).toBe('TimeTravelFork');
    expect(markup).toContain('Fork origin');
    expect(markup).toContain('/v1/flows/parent/previous-run-id');
    expect(markup).toContain('previous-run-id');
  });

  it.each([
    ['sync', 1],
    ['async', 2],
  ])('renders %s with the common input, output, and context structure', (_, durability) => {
    const markup = renderDetails(executeEvent(durability));
    const labels = ['Step input', 'Condition results', 'Attributes', 'Step locals', 'Output', 'Context'];
    const positions = labels.map((label) => markup.indexOf(label));

    expect(positions.every((position) => position >= 0)).toBe(true);
    expect(positions).toEqual([...positions].sort((left, right) => left - right));
    expect(markup).toContain('charge-1');
    expect(markup).toContain('authorize-1');
    expect(markup).toContain(`${durability === 1 ? 'sync' : 'async'}`);
    expect(markup).toContain('3ms');
    expect(markup).toContain('10s');
    expect(markup).not.toContain('previousAttempt');
  });

  it('renders Flow-wide and sibling cancellation selectors with next Steps', () => {
    const event = executeEvent(2);
    event.payload.output = {
      stepDecision: {
        nextSteps: [{ stepType: 'complete' }],
        cancelStepTypes: ['global-loser', 'shared-loser'],
        cancelSiblingStepTypes: ['sibling-loser'],
      },
    };
    const markup = renderDetails(event);

    expect(markup).toContain('complete');
    expect(markup).toContain('Cancel Step executions');
    expect(markup).toContain('Entire Flow');
    expect(markup).toContain('global-loser, shared-loser');
    expect(markup).toContain('Cancel sibling Step executions');
    expect(markup).toContain('Same parent execution');
    expect(markup).toContain('sibling-loser');
  });

  it('renders the latest sync failure in context', () => {
    const event = executeEvent(1);
    event.payload.context = {
      ...event.payload.context as Record<string, unknown>,
      lastFailureInfo: {
        attempt: 1,
        backendError: 'FLOW_ERROR_TYPE_WORKER_API_FAIL',
        message: 'legacy message',
        errorType: 'legacy error type',
        retryState: 'legacy retry state',
        stackTrace: 'legacy stack trace',
        details: {
          originalWorkerErrorType: 'UnavailableWorker',
          originalWorkerErrorDetail: 'try again',
          originalWorkerErrorStatus: 14,
          originalWorkerErrorStackTrace: 'java worker stack',
        },
      },
    };
    const markup = renderDetails(event);

    expect(markup).toContain('Last failure');
    expect(markup).toContain('Attempt');
    expect(markup).toContain('Error type');
    expect(markup).not.toContain('Backend error');
    expect(markup).toContain('Worker method failed');
    expect(markup).not.toContain('Retry state');
    expect(markup).not.toContain('Retry scheduled');
    expect(markup).not.toContain('sync retry failure');
    expect(markup).toContain('UnavailableWorker');
    expect(markup).toContain('try again');
    expect(markup).toContain('UNAVAILABLE (14)');
    expect(markup).toContain('java worker stack');
    expect(markup).toContain('<details class="failure-stack">');
    expect(markup).toContain('semantic-fields-stacked');
    expect(markup).toMatch(/semantic-fields-stacked[^>]*>.*Attempt.*Error type.*Worker error type/s);
    expect(markup).not.toContain('legacy message');
    expect(markup).not.toContain('legacy error type');
    expect(markup).not.toContain('legacy retry state');
    expect(markup).not.toContain('legacy stack trace');
    expect(markup).not.toContain('Previous attempts');
  });

  it('does not render the removed backend stack when the Worker does not provide one', () => {
    const event = executeEvent(1);
    event.payload.context = {
      ...event.payload.context as Record<string, unknown>,
      lastFailureInfo: {
        attempt: 1,
        backendError: 'FLOW_ERROR_TYPE_WORKER_API_FAIL',
        stackTrace: 'legacy backend activity stack',
        details: {
          originalWorkerErrorType: 'WorkerWithoutStackSupport',
          originalWorkerErrorStatus: 13,
        },
      },
    };
    const markup = renderDetails(event);

    expect(markup).not.toContain('legacy backend activity stack');
    expect(markup).toContain('INTERNAL (13)');
    expect(markup).not.toContain('<details class="failure-stack">');
  });

  it('renders the terminal failure attempt with the same failure component', () => {
    const event = executeEvent(1);
    event.type = 'StepExecuteFailed';
    event.payload.output = {
      failure: {
        attempt: 2,
        backendError: 'StartToClose',
      },
    };
    const markup = renderDetails(event);

    expect(markup).toContain('Failure');
    expect(markup).toContain('Error type');
    expect(markup).not.toContain('Backend error');
    expect(markup).toContain('StartToClose');
    expect(markup).toContain('2');
    expect(markup).not.toContain('Detail');
  });

  it('explains why a terminal async failure has no invocation input snapshot', () => {
    const event = executeEvent(2);
    event.type = 'StepExecuteFailed';
    event.payload.input = { unavailable: true };
    event.payload.output = { failure: { attempt: 1, backendError: 'Unavailable' } };
    const markup = renderDetails(event);

    expect(markup).toContain('Step method input snapshot unavailable');
    expect(markup).toContain(ASYNC_STEP_INPUT_SNAPSHOT_NOT_RECORDED);
    expect(markup).toContain('does not indicate an individual Value blob load failure');
    expect(markup).toContain('Step input, attributes, condition results, and step locals are absent');
    expect(markup).toContain('Follow its source arrow in Timeline');
    expect(markup).not.toContain('Step event input unavailable');
    expect(markup).not.toContain('Value blob unavailable');
  });

  it('distinguishes an unavailable input snapshot from an unavailable Value blob', () => {
    const event = executeEvent(2);
    event.payload.input = { unavailable: true };
    const markup = renderDetails(event);

    expect(markup).toContain('Step method input snapshot unavailable');
    expect(markup).toContain(STEP_INPUT_SNAPSHOT_UNAVAILABLE);
    expect(markup).toContain('blobStore.asyncStepInputSnapshotsEnabled');
    expect(markup).toContain('set blobStore.asyncStepInputSnapshotsEnabled to false');
    expect(markup).toContain('Blob Store may be disabled');
    expect(markup).toContain('full invocation input snapshot');
    expect(markup).toContain('does not indicate an individual Value blob load failure');
    expect(markup).not.toContain(ASYNC_STEP_INPUT_SNAPSHOT_NOT_RECORDED);
    expect(markup).not.toContain('Value blob unavailable');
  });

  it('derives Execute failure and locking options from the source step decision', () => {
    const event = executeEvent(1);
    const source: FlowHistoryEvent = {
      eventId: 8,
      eventTime: '2026-08-05T23:44:30Z',
      type: 'StepExecuteCompleted',
      payload: {
        context: { stepExecutionId: 'authorize-1', stepType: 'authorize' },
        output: {
          stepDecision: {
            nextSteps: [{
              stepType: 'charge',
              stepInput: { stringValue: 'charge' },
              fromStepExecutionIdInternalOnly: 'authorize-1',
              stepOptions: {
                executeFailurePolicy: 2,
                executeFailureProceedStepType: 'recover-charge',
                executeLockAttributeKeys: ['account', 'balance'],
                waitForFailurePolicy: 1,
                waitForLockAttributeKeys: ['should-not-render'],
              },
            }],
          },
        },
      },
    };

    const markup = renderDetails(event, [source, event]);

    expect(markup).toContain('Failure policy');
    expect(markup).toContain('Proceed to configured step');
    expect(markup).toContain('recover-charge');
    expect(markup).toContain('Locking attributes');
    expect(markup).toContain('account, balance');
    expect(markup).not.toContain('should-not-render');
  });

  it('derives initial WaitFor options from the flow start event', () => {
    const event: FlowHistoryEvent = {
      ...executeEvent(2),
      eventId: 10,
      type: 'StepWaitForCompleted',
      payload: {
        ...executeEvent(2).payload,
        context: {
          ...executeEvent(2).payload.context as Record<string, unknown>,
          fromStepExecutionId: '__start__',
        },
      },
    };
    const start: FlowHistoryEvent = {
      eventId: 1,
      eventTime: '2026-08-05T23:44:29Z',
      type: 'FlowStartedOrContinued',
      payload: {
        initialStart: {
          startStepType: 'charge',
          stepOptions: {
            waitForFailurePolicy: 2,
            waitForLockAttributeKeys: ['ready'],
            executeLockAttributeKeys: ['should-not-render'],
          },
        },
      },
    };

    const markup = renderDetails(event, [start, event]);

    expect(markup).toContain('Proceed');
    expect(markup).toContain('ready');
    expect(markup).not.toContain('should-not-render');
  });

  it('derives options from an exact continued step resume', () => {
    const event = executeEvent(2);
    event.payload.context = {
      ...event.payload.context as Record<string, unknown>,
      fromStepExecutionId: 'previous-run-step-1',
    };
    const continued: FlowHistoryEvent = {
      eventId: 1,
      eventTime: '2026-08-05T23:44:29Z',
      type: 'FlowStartedOrContinued',
      payload: {
        continuedStart: {
          stepsToResume: [{
            stepExecutionId: 'charge-1',
            step: {
              stepType: 'charge',
              stepOptions: {
                executeFailurePolicy: 1,
                executeLockAttributeKeys: ['continued-account'],
              },
            },
          }],
        },
      },
    };

    const markup = renderDetails(event, [continued, event]);

    expect(markup).toContain('Fail flow');
    expect(markup).toContain('continued-account');
  });

  it('derives options from a continued step movement', () => {
    const event = executeEvent(2);
    event.payload.context = {
      ...event.payload.context as Record<string, unknown>,
      fromStepExecutionId: 'previous-run-step-1',
    };
    const continued: FlowHistoryEvent = {
      eventId: 1,
      eventTime: '2026-08-05T23:44:29Z',
      type: 'FlowStartedOrContinued',
      payload: {
        continuedStart: {
          stepsToStart: [{
            stepType: 'charge',
            fromStepExecutionIdInternalOnly: 'previous-run-step-1',
            stepOptions: {
              executeFailurePolicy: 2,
              executeFailureProceedStepType: 'continued-recovery',
              executeLockAttributeKeys: ['continued-movement-account'],
            },
          }],
        },
      },
    };

    const markup = renderDetails(event, [continued, event]);

    expect(markup).toContain('Proceed to configured step');
    expect(markup).toContain('continued-recovery');
    expect(markup).toContain('continued-movement-account');
  });

  it('derives options from an RPC movement', () => {
    const event = executeEvent(2);
    event.payload.context = {
      ...event.payload.context as Record<string, unknown>,
      fromStepExecutionId: '__rpc/charge',
    };
    const rpc: FlowHistoryEvent = {
      eventId: 7,
      eventTime: '2026-08-05T23:44:30Z',
      type: 'RpcExecutionCompleted',
      payload: {
        rpcName: 'charge',
        stepDecision: {
          nextSteps: [{
            stepType: 'charge',
            stepOptions: {
              executeFailurePolicy: 1,
              executeLockAttributeKeys: ['rpc-account'],
            },
          }],
        },
      },
    };

    const markup = renderDetails(event, [rpc, event]);

    expect(markup).toContain('Fail flow');
    expect(markup).toContain('rpc-account');
  });

  it('describes one WaitFor condition without an allOf or anyOf rule', () => {
    const markup = renderDetails(waitForEvent({
      waitingConditionType: 'WAITING_CONDITION_TYPE_ANY_COMPLETED',
      channelConditions: [{ conditionId: 1, channelName: 'approval' }],
    }));

    expect(markup).toContain('Single condition');
    expect(markup).not.toContain('Any completed');
    expect(markup).not.toContain('All completed');
  });

  it('explains that an empty WaitFor condition skips waiting immediately', () => {
    const markup = renderDetails(waitForEvent());

    expect(markup).toContain('Empty condition — skips WaitFor immediately');
    expect(markup).not.toContain('All completed');
  });

  it('preserves the completion rule for multiple WaitFor conditions', () => {
    const markup = renderDetails(waitForEvent({
      waitingConditionType: 'WAITING_CONDITION_TYPE_ANY_COMPLETED',
      channelConditions: [{ conditionId: 1, channelName: 'approval' }],
      timerConditions: [{ conditionId: 2, durationSeconds: 30 }],
    }));

    expect(markup).toContain('Any completed');
    expect(markup).not.toContain('Single condition');
  });

  it('renders a keyboard-accessible SubFlow link card with reuse details', () => {
    const markup = renderDetails(waitForEvent({
      waitingConditionType: 'WAITING_CONDITION_TYPE_ALL_COMPLETED',
      subFlowConditions: [{
        options: { reusePolicy: 1 },
      }],
    }));

    expect(markup).toContain('href="/v1/flows/SubFlow%3Aparent-charge-1-0"');
    expect(markup).toContain('aria-label="Open SubFlow SubFlow:parent-charge-1-0"');
    expect(markup).toContain('Attach');
  });

  it('renders complete SubFlow options and terminal outputs', () => {
    const event = executeEvent(1);
    event.payload.input = {
      conditionResults: { subFlowResults: [{
        flowStatus: 3,
        errorType: 'FLOW_ERROR_TYPE_WORKER_API_FAIL',
        errorMessage: 'child failed',
        results: [{
          completedStepType: 'ChildStep',
          completedStepExecutionId: 'ChildStep-1',
          completedStepOutput: { stringValue: 'partial output' },
        }],
      }] },
    };
    const wait = waitForEvent({
      subFlowConditions: [{
        stepInput: { intValue: 7 },
        stepOptions: { skipWaitFor: true },
        options: {
          retryPolicy: { initialIntervalSeconds: 2, maximumAttempts: 4 },
          attributes: [{ key: 'region', value: { stringValue: 'west' } }],
          flowConfigOverride: { continueAsNewThreshold: 20 },
        },
      }],
    });

    const waitMarkup = renderDetails(wait);
    expect(waitMarkup).toContain('Retry initial interval');
    expect(waitMarkup).toContain('2s');
    expect(waitMarkup).toContain('Starting Step options');
    expect(waitMarkup).toContain('Initial Attributes');
    expect(waitMarkup).toContain('region');
    expect(waitMarkup).toContain('Flow configuration override');
    expect(waitMarkup).toContain('continueAsNewThreshold');

    const resultMarkup = renderDetails(event, [wait, event]);
    expect(resultMarkup).toContain('href="/v1/flows/SubFlow%3Aparent-charge-1-0"');
    expect(resultMarkup).toContain('Worker method failed');
    expect(resultMarkup).toContain('child failed');
    expect(resultMarkup).toContain('ChildStep-1');
    expect(resultMarkup).toContain('partial output');
  });
});

describe('RPC event details', () => {
  it('renders external SetAttributes without exposing its system RPC', () => {
    const event: FlowHistoryEvent = {
      eventId: 9,
      eventTime: '2026-08-05T23:44:30Z',
      type: 'RpcExecutionCompleted',
      payload: {
        isSetAttributeApi: true,
        upsertAttributes: [{
          key: 'order-status',
          value: { stringValue: 'complete' },
        }],
      },
    };

    const markup = renderDetails(event);

    expect(eventTitle(event)).toBe('Attributes updated');
    expect(eventTypeLabel(event)).toBe('SetAttributes');
    expect(markup).toContain('Updated attributes');
    expect(markup).toContain('order-status');
    expect(markup).toContain('complete');
    expect(markup).not.toContain('RPC call');
    expect(markup).not.toContain('RPC name');
  });

  it('keeps Attribute and Channel values collapsed, with two levels for maps', () => {
    const attributesEvent: FlowHistoryEvent = {
      eventId: 10,
      eventTime: '2026-08-05T23:44:31Z',
      type: 'RpcExecutionCompleted',
      payload: {
        isSetAttributeApi: true,
        upsertAttributes: [
          { key: 'status', value: { stringValue: 'ready' } },
          { key: 'orders/11', value: { stringValue: 'eleven' } },
          { key: 'orders/first%20order', value: { stringValue: 'pending' } },
          { key: 'orders/2', value: { stringValue: 'two' } },
        ],
      },
    };
    const channelEvent: FlowHistoryEvent = {
      eventId: 11,
      eventTime: '2026-08-05T23:44:32Z',
      type: 'ChannelExternalPublish',
      payload: {
        messages: [
          { channelName: 'notifications', value: { stringValue: 'sent' } },
          { channelName: 'updates/11', value: { stringValue: 'eleven' } },
          { channelName: 'updates/first%20order', value: { stringValue: 'queued' } },
          { channelName: 'updates/2', value: { stringValue: 'two' } },
        ],
      },
    };

    const attributeMarkup = renderDetails(attributesEvent);
    const channelMarkup = renderDetails(channelEvent);

    expect(attributeMarkup).toContain('<details class="semantic-record">');
    expect(attributeMarkup).toContain('<details class="semantic-record semantic-map-record">');
    expect(attributeMarkup).toContain('first order');
    expect(attributeMarkup).not.toContain('<details class="semantic-record" open="">');
    expect(channelMarkup).toContain('<details class="semantic-record channel-record">');
    expect(channelMarkup).toContain('<details class="semantic-record channel-record semantic-map-record">');
    expect(channelMarkup).toContain('first order');
    expect(channelMarkup).not.toContain('<details class="semantic-record channel-record" open="">');
    expectStrongLabelsInOrder(attributeMarkup, ['2', '11', 'first order']);
    expectStrongLabelsInOrder(channelMarkup, ['2', '11', 'first order']);
  });
});

describe('pending step method details', () => {
  it('renders an Execute left pending by a forced close', () => {
    const event: FlowHistoryEvent = {
      eventId: 14,
      eventTime: '2026-08-05T23:44:35Z',
      type: 'StepExecutePending',
      payload: {
        phase: 2,
        input: { stepInput: { stringValue: 'charge' } },
        context: {
          stepExecutionId: 'charge-1',
          fromStepExecutionId: 'authorize-1',
          stepType: 'charge',
          durability: 1,
          finalAttempt: 1,
          duration: '3s',
        },
      },
    };

    const markup = renderDetails(event);
    expect(markup).toContain('Activity phase');
    expect(markup).toContain('Started');
    expect(markup).toContain('charge-1');
    expect(markup).toContain('3s');
  });
});

describe('flow start event details', () => {
  it('renders the configured flow timeout', () => {
    const event: FlowHistoryEvent = {
      eventId: 1,
      eventTime: '2026-08-05T23:44:29Z',
      type: 'FlowStartedOrContinued',
      payload: {
        flowTimeout: '60s',
        initialStart: { startStepType: 'charge' },
      },
    };

    const markup = renderDetails(event);

    expect(markup).toContain('Flow timeout');
    expect(markup).toContain('1m');
    expect(markup).not.toContain('Timeout policy');
  });

  it('renders the configured flow timeout policy', () => {
    const event: FlowHistoryEvent = {
      eventId: 1,
      eventTime: '2026-08-05T23:44:29Z',
      type: 'FlowStartedOrContinued',
      payload: {
        flowTimeout: '60s',
        flowTimeoutPolicy: 2,
        initialStart: { startStepType: 'charge' },
      },
    };

    const markup = renderDetails(event);

    expect(markup).toContain('Flow timeout');
    expect(markup).toContain('1m');
    expect(markup).toContain('Timeout policy');
    expect(markup).toContain('Cancel');
  });

  it('links a minimal continued SubFlow state by generated Flow ID', () => {
    const event: FlowHistoryEvent = {
      eventId: 1,
      eventTime: '2026-08-05T23:44:29Z',
      type: 'FlowStartedOrContinued',
      payload: {
        continuedStart: {
          stepsToResume: [{
            stepExecutionId: 'charge-1',
            waitingCondition: {
              subFlowConditions: [{ conditionId: 'child' }],
            },
          }],
        },
      },
    };

    const markup = renderDetails(event);

    expect(markup).toContain('href="/v1/flows/SubFlow%3Aparent-charge-1-0"');
    expect(markup).toContain('SubFlow:parent-charge-1-0');
  });

  it('renders Flow timeout closure details', () => {
    const event: FlowHistoryEvent = {
      eventId: 23,
      eventTime: '2026-08-19T03:17:17.115138Z',
      type: 'FlowClosed',
      payload: {
        flowStatus: 3,
        errorType: 5,
        errorMessage: 'Flow timed out after 3600 seconds',
      },
    };

    const markup = renderDetails(event);

    expect(markup).toContain('Flow timed out after 3600 seconds');
    expect(markup).toContain('Flow timed out');
  });
});

describe('routing context and dispatch failures', () => {
  it('shows the serving Worker and dispatch sticky Worker in method context', () => {
    const event = executeEvent(1);
    (event.payload.context as Record<string, unknown>).pullRouting = { servedByAppWorkerId: 'worker-new', stickyAppWorkerId: 'worker-old', source: 2 };
    const markup = renderDetails(event);
    expect(markup).toContain('worker-new');
    expect(markup).toContain('worker-old');
    expect(markup).toContain('Flow type queue');
  });
  it.each([
    ['no Worker available for queue PaymentFlow', 'No Worker available'],
    ['Worker disconnected during pull dispatch: connection lost', 'Worker disconnected'],
    ['no pull Worker registered Step "Charge"', 'Unknown Step type: dispatch timed out'],
    ['no pull Worker registered RPC "getStatus"', 'Unknown RPC name: dispatch timed out'],
  ])('labels dispatch error %s', (detail, expected) => {
    const event = executeEvent(1);
    event.type = 'StepExecuteFailed';
    event.payload.output = { failure: { details: { detail } } };
    expect(renderDetails(event)).toContain(expected);
  });
});
