// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { useEffect, useRef, useState, type FormEvent } from 'react';
import type { FlowV2StartDefinition, FlowV2StartInputSchema } from '@superdurable/flow-definition-renderer';
import { definitionRevisionHeaders, DexAPIError, readResponseJSON } from '@/lib/http';
import {
  createMapEntry,
  createStartInputDraft,
  serializeStartInput,
  type StartInputDraft,
  type StartInputError,
} from './startInput';

interface StartFlowResult {
  flowId: string;
}

interface WorkerHealthResult {
  healthy: boolean;
  warning?: string;
}

type WorkerHealthState = 'idle' | 'checking' | 'healthy' | 'unhealthy';

export function StartFlowDialog({
  definition,
  definitionRevision,
  flowType,
  isDefaultPull,
  onClose,
  onDefinitionChanged,
  onStarted,
}: {
  definition: FlowV2StartDefinition;
  definitionRevision: string;
  flowType: string;
  isDefaultPull: boolean;
  onClose: () => void;
  onDefinitionChanged: (error: unknown) => boolean;
  onStarted: (flowID: string) => void;
}) {
  const [flowID, setFlowID] = useState('');
  const [workerTargetMode, setWorkerTargetMode] = useState<'address' | 'pull'>(isDefaultPull ? 'pull' : 'address');
  const [workerAddress, setWorkerAddress] = useState('');
  const [draft, setDraft] = useState(() => createStartInputDraft(definition.input));
  const [errors, setErrors] = useState<StartInputError[]>([]);
  const [submitError, setSubmitError] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [workerHealth, setWorkerHealth] = useState<WorkerHealthState>('idle');
  const [workerHealthAddress, setWorkerHealthAddress] = useState('');
  const [workerHealthWarning, setWorkerHealthWarning] = useState('');
  const [bypassWorkerHealthCheck, setBypassWorkerHealthCheck] = useState(false);
  const dialogRef = useRef<HTMLDialogElement>(null);
  const workerHealthSequence = useRef(0);
  const requestController = useRef<AbortController | null>(null);

  useEffect(() => {
    if (dialogRef.current && !dialogRef.current.open) dialogRef.current.showModal();
    dialogRef.current?.querySelector<HTMLInputElement>('input, textarea, select')?.focus();
    return () => {
      workerHealthSequence.current += 1;
      requestController.current?.abort();
    };
  }, []);

  const updateDraft = () => setDraft({ ...draft });
  const checkWorkerHealth = async (address: string): Promise<boolean> => {
    const trimmedAddress = address.trim();
    if (trimmedAddress === '') return false;
    const sequence = workerHealthSequence.current + 1;
    workerHealthSequence.current = sequence;
    setWorkerHealth('checking');
    setWorkerHealthWarning('');
    try {
      const result = await fetch('/api/v2/worker-health', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ workerTargetAddress: trimmedAddress }),
      }).then((response) => readResponseJSON<WorkerHealthResult>(response));
      if (workerHealthSequence.current !== sequence) return false;
      setWorkerHealthAddress(trimmedAddress);
      setWorkerHealth(result.healthy ? 'healthy' : 'unhealthy');
      setWorkerHealthWarning(result.warning ?? 'Dex Web cannot reach this Worker.');
      return result.healthy;
    } catch (error) {
      if (workerHealthSequence.current !== sequence) return false;
      setWorkerHealthAddress(trimmedAddress);
      setWorkerHealth('unhealthy');
      setWorkerHealthWarning(error instanceof Error ? error.message : 'Worker health check failed.');
      return false;
    }
  };
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    const nextErrors: StartInputError[] = [];
    if (flowID.trim() === '') nextErrors.push({ path: 'flowId', message: 'Flow ID is required.' });
    if (workerTargetMode === 'address' && workerAddress.trim() === '') {
      nextErrors.push({ path: 'workerTargetAddress', message: 'Worker address is required.' });
    }
    const input = serializeStartInput(definition.input, draft);
    nextErrors.push(...input.errors);
    setErrors(nextErrors);
    setSubmitError('');
    if (nextErrors.length > 0) {
      focusPath(dialogRef.current, nextErrors[0].path);
      return;
    }
    setSubmitting(true);
    const controller = new AbortController();
    requestController.current?.abort();
    requestController.current = controller;
    try {
      const trimmedWorkerAddress = workerAddress.trim();
      const hasCurrentHealthyCheck = workerHealth === 'healthy' && workerHealthAddress === trimmedWorkerAddress;
      if (workerTargetMode === 'address' && !bypassWorkerHealthCheck && !hasCurrentHealthyCheck) {
        const isHealthy = await checkWorkerHealth(trimmedWorkerAddress);
        if (!isHealthy) {
          focusPath(dialogRef.current, 'bypassWorkerHealthCheck');
          return;
        }
      }
      const requestPrefix = JSON.stringify({
        flowType,
        flowId: flowID.trim(),
        ...(workerTargetMode === 'pull'
          ? { workerTargetPull: true }
          : { workerTargetAddress: trimmedWorkerAddress, bypassWorkerHealthCheck }),
      });
      const body = `${requestPrefix.slice(0, -1)},"input":${input.json}}`;
      const headers = {
        'Content-Type': 'application/json', ...definitionRevisionHeaders(definitionRevision),
      };
      const result = await fetch('/api/v2/start', {
        method: 'POST',
        headers, body, signal: controller.signal,
      }).then((response) => readResponseJSON<StartFlowResult>(response));
      if (!controller.signal.aborted) onStarted(result.flowId);
    } catch (error) {
      if (controller.signal.aborted) return;
      if (onDefinitionChanged(error)) {
        onClose();
        return;
      }
      if (workerTargetMode === 'address' && error instanceof DexAPIError && error.code === 'WORKER_UNHEALTHY') {
        setWorkerHealth('unhealthy');
        setWorkerHealthAddress(workerAddress.trim());
        setWorkerHealthWarning(error.message);
        focusPath(dialogRef.current, 'bypassWorkerHealthCheck');
        return;
      }
      setSubmitError(error instanceof DexAPIError || error instanceof Error ? error.message : 'Start Flow failed');
    } finally {
      if (!controller.signal.aborted) setSubmitting(false);
    }
  };

  return (
    <dialog
      aria-labelledby="start-flow-title"
      className="sfd"
      ref={dialogRef}
      onCancel={(event) => {
        event.preventDefault();
        if (!submitting) onClose();
      }}
    >
      <form onSubmit={(event) => void submit(event)}>
        <header className="sfd-head">
          <div>
            <h2 id="start-flow-title">Start {flowType}</h2>
            <p>{definition.stepType}</p>
          </div>
          <button aria-label="Close Start Flow" disabled={submitting} onClick={onClose} type="button">×</button>
        </header>
        <div className="sfd-scroll">
          <label className="sfd-field">
            <span>Flow ID</span>
            <input
              data-start-path="flowId"
              id="start-flow-id"
              value={flowID}
              onChange={(event) => setFlowID(event.target.value)}
            />
            <FieldError errors={errors} path="flowId" />
          </label>
          <fieldset className="sfd-target-mode" disabled={submitting}>
            <legend>Worker target</legend>
            <label>
              <input
                checked={workerTargetMode === 'address'}
                name="worker-target-mode"
                type="radio"
                onChange={() => {
                  workerHealthSequence.current += 1;
                  setWorkerHealth('idle');
                  setWorkerHealthWarning('');
                  setBypassWorkerHealthCheck(false);
                  setWorkerTargetMode('address');
                }}
              />
              Worker address
            </label>
            <label>
              <input
                checked={workerTargetMode === 'pull'}
                name="worker-target-mode"
                type="radio"
                onChange={() => {
                  workerHealthSequence.current += 1;
                  setWorkerHealth('idle');
                  setWorkerHealthWarning('');
                  setBypassWorkerHealthCheck(false);
                  setWorkerTargetMode('pull');
                }}
              />
              Pull Worker
            </label>
          </fieldset>
          {workerTargetMode === 'address' && <label className="sfd-field">
            <span>Worker gRPC address</span>
            <span className="sfd-worker-control">
              <input
                data-start-path="workerTargetAddress"
                placeholder="127.0.0.1:8803"
                value={workerAddress}
                onBlur={(event) => {
                  if (event.relatedTarget instanceof HTMLElement && event.relatedTarget.dataset.workerHealthCheck === 'true') return;
                  if (workerAddress.trim() !== '' && workerHealth === 'idle') void checkWorkerHealth(workerAddress);
                }}
                onChange={(event) => {
                  workerHealthSequence.current += 1;
                  setWorkerAddress(event.target.value);
                  setWorkerHealth('idle');
                  setWorkerHealthAddress('');
                  setWorkerHealthWarning('');
                  setBypassWorkerHealthCheck(false);
                }}
              />
              <button
                className="button ghost"
                data-worker-health-check="true"
                disabled={submitting || workerHealth === 'checking' || workerAddress.trim() === ''}
                type="button"
                onClick={() => void checkWorkerHealth(workerAddress)}
              >
                {workerHealth === 'checking' ? 'Checking…' : 'Check Worker'}
              </button>
            </span>
            <FieldError errors={errors} path="workerTargetAddress" />
          </label>}
          {workerTargetMode === 'address' && workerHealth === 'healthy' && (
            <div className="sfd-worker-health" data-status="healthy" role="status">
              Worker port is reachable from Dex Web.
            </div>
          )}
          {workerTargetMode === 'address' && workerHealth === 'unhealthy' && (
            <div className="sfd-worker-health" data-status="unhealthy" role="alert">
              <strong>Worker health check failed</strong>
              <span>{workerHealthWarning}</span>
              <label className="sfd-bypass">
                <input
                  checked={bypassWorkerHealthCheck}
                  data-start-path="bypassWorkerHealthCheck"
                  type="checkbox"
                  onChange={(event) => setBypassWorkerHealthCheck(event.target.checked)}
                />
                Bypass worker health check and start anyway
              </label>
            </div>
          )}
          <fieldset className="sfd-input" disabled={submitting}>
            <legend>Start input</legend>
            <SchemaInput
              draft={draft}
              errors={errors}
              label="Input"
              path="$"
              required
              schema={definition.input}
              onChange={updateDraft}
            />
          </fieldset>
          {submitError && <div className="error-banner" role="alert">{submitError}</div>}
        </div>
        <footer className="sfd-actions">
          <button disabled={submitting} onClick={onClose} type="button">Cancel</button>
          <button
            className="button primary"
            disabled={submitting || (workerTargetMode === 'address' && (workerHealth === 'checking' || (workerHealth === 'unhealthy' && !bypassWorkerHealthCheck)))}
            type="submit"
          >
            {submitting ? (workerTargetMode === 'address' && workerHealth === 'checking' ? 'Checking Worker…' : 'Starting…') : 'Start Flow'}
          </button>
        </footer>
      </form>
    </dialog>
  );
}

function SchemaInput({ draft, errors, label, path, required, schema, onChange }: {
  draft: StartInputDraft;
  errors: StartInputError[];
  label: string;
  path: string;
  required: boolean;
  schema: FlowV2StartInputSchema;
  onChange: () => void;
}) {
  const isIncluded = draft.mode !== 'omitted';
  const valueControls = (
    <>
      {!required && (
        <label className="sfd-toggle">
          <input
            checked={isIncluded}
            type="checkbox"
            onChange={(event) => { draft.mode = event.target.checked ? 'value' : 'omitted'; onChange(); }}
          />
          Include
        </label>
      )}
      {isIncluded && schema.nullable && (
        <select
          aria-label={`${label} value mode`}
          value={draft.mode === 'null' ? 'null' : 'value'}
          onChange={(event) => { draft.mode = event.target.value === 'null' ? 'null' : 'value'; onChange(); }}
        >
          <option value="value">Value</option>
          <option value="null">Null</option>
        </select>
      )}
    </>
  );
  if (!isIncluded || draft.mode === 'null' || schema.kind === 'null') {
    return (
      <div className="sfd-row">
        <span className="sfd-label">{label}</span>
        {valueControls}
        {schema.kind === 'null' && <code>null</code>}
      </div>
    );
  }
  if (schema.kind === 'object') {
    return (
      <fieldset className="sfd-group" data-start-path={path} tabIndex={-1}>
        <legend>{label}</legend>
        {valueControls}
        {(schema.fields ?? []).map((field) => (
          <SchemaInput
            draft={draft.fields[field.name]}
            errors={errors}
            key={field.name}
            label={field.name}
            path={`${path}.${field.name}`}
            required={field.required}
            schema={field.schema}
            onChange={onChange}
          />
        ))}
      </fieldset>
    );
  }
  if (schema.kind === 'array') {
    const itemSchema = schema.items;
    if (!itemSchema) return null;
    return (
      <fieldset className="sfd-group" data-start-path={path} tabIndex={-1}>
        <legend>{label}</legend>
        {valueControls}
        {draft.items.map((item, index) => (
          <div className="sfd-collection" key={index}>
            <SchemaInput
              draft={item}
              errors={errors}
              label={`Item ${index + 1}`}
              path={`${path}[${index}]`}
              required
              schema={itemSchema}
              onChange={onChange}
            />
            {schema.fixedLength === undefined && (
              <button type="button" onClick={() => { draft.items.splice(index, 1); onChange(); }}>Remove</button>
            )}
          </div>
        ))}
        {schema.fixedLength === undefined && (
          <button type="button" onClick={() => { draft.items.push(createStartInputDraft(itemSchema)); onChange(); }}>
            Add item
          </button>
        )}
        <FieldError errors={errors} path={path} />
      </fieldset>
    );
  }
  if (schema.kind === 'map') {
    const valueSchema = schema.values;
    if (!valueSchema) return null;
    return (
      <fieldset className="sfd-group" data-start-path={path} tabIndex={-1}>
        <legend>{label}</legend>
        {valueControls}
        {draft.entries.map((entry, index) => (
          <div className="sfd-map" key={entry.id}>
            <input
              aria-label={`${label} key ${index + 1}`}
              placeholder="Key"
              value={entry.key}
              onChange={(event) => { entry.key = event.target.value; onChange(); }}
            />
            <SchemaInput
              draft={entry.value}
              errors={errors}
              label="Value"
              path={`${path}[${JSON.stringify(entry.key)}]`}
              required
              schema={valueSchema}
              onChange={onChange}
            />
            <button type="button" onClick={() => { draft.entries.splice(index, 1); onChange(); }}>Remove</button>
          </div>
        ))}
        <button type="button" onClick={() => { draft.entries.push(createMapEntry(schema)); onChange(); }}>Add entry</button>
        <FieldError errors={errors} path={path} />
      </fieldset>
    );
  }
  return (
    <label className="sfd-field">
      <span>{label}{required ? ' *' : ''}</span>
      {valueControls}
      <ScalarInput draft={draft} label={label} path={path} schema={schema} onChange={onChange} />
      <FieldError errors={errors} path={path} />
    </label>
  );
}

function ScalarInput({ draft, label, path, schema, onChange }: {
  draft: StartInputDraft;
  label: string;
  path: string;
  schema: FlowV2StartInputSchema;
  onChange: () => void;
}) {
  if (schema.enumValues) {
    return (
      <select data-start-path={path} value={draft.scalar} onChange={(event) => { draft.scalar = event.target.value; onChange(); }}>
        {schema.enumValues.map((option) => (
          <option key={option.name} value={option.value.toString()}>{option.name} ({option.value})</option>
        ))}
      </select>
    );
  }
  if (schema.kind === 'boolean') {
    return (
      <select
        aria-label={label}
        data-start-path={path}
        value={draft.booleanValue ? 'true' : 'false'}
        onChange={(event) => { draft.booleanValue = event.target.value === 'true'; onChange(); }}
      >
        <option value="false">false</option>
        <option value="true">true</option>
      </select>
    );
  }
  if (schema.format === 'date-time') {
    return (
      <input
        data-start-path={path}
        type="datetime-local"
        value={localDateTime(draft.scalar)}
        onChange={(event) => { draft.scalar = event.target.value ? new Date(event.target.value).toISOString() : ''; onChange(); }}
      />
    );
  }
  return (
    <input
      data-start-path={path}
      inputMode={schema.kind === 'integer' || schema.kind === 'number' ? 'decimal' : undefined}
      type="text"
      value={draft.scalar}
      onChange={(event) => { draft.scalar = event.target.value; onChange(); }}
    />
  );
}

function FieldError({ errors, path }: { errors: StartInputError[]; path: string }) {
  const error = errors.find((candidate) => candidate.path === path);
  return error ? <small className="sfd-error" role="alert">{error.message}</small> : null;
}

function focusPath(dialog: HTMLDialogElement | null, path: string) {
  const element = Array.from(dialog?.querySelectorAll<HTMLElement>('[data-start-path]') ?? [])
    .find((candidate) => candidate.dataset.startPath === path);
  element?.focus();
}

function localDateTime(value: string): string {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) return '';
  const offset = date.getTimezoneOffset() * 60_000;
  return new Date(date.valueOf() - offset).toISOString().slice(0, 16);
}
