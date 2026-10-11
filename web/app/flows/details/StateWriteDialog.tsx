// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { useState } from 'react';
import { readResponseJSON } from '@/lib/http';
import type { FlowSummary } from '@/lib/types';

export type StateWriteKind = 'attribute' | 'channel';

export type DebugValueType = 'string' | 'int64' | 'double' | 'bool' | 'json';

const valueTypes: { value: DebugValueType; label: string }[] = [
  { value: 'json', label: 'JSON' },
  { value: 'string', label: 'String' },
  { value: 'int64', label: 'Integer (int64)' },
  { value: 'double', label: 'Number (double)' },
  { value: 'bool', label: 'Boolean' },
];

export type ParsedDebugValue = { value: unknown } | { error: string };

// int64 stays a decimal string so values beyond 2^53 keep their precision.
export function parseDebugValue(valueType: DebugValueType, text: string): ParsedDebugValue {
  const trimmed = text.trim();
  switch (valueType) {
    case 'string':
      return { value: text };
    case 'int64':
      return /^-?\d+$/.test(trimmed) ? { value: trimmed } : { error: 'Enter a whole number.' };
    case 'double': {
      const number = Number(trimmed);
      return trimmed !== '' && Number.isFinite(number)
        ? { value: number }
        : { error: 'Enter a finite number.' };
    }
    case 'bool':
      if (trimmed === 'true') return { value: true };
      if (trimmed === 'false') return { value: false };
      return { error: 'Enter true or false.' };
    case 'json':
      try {
        return { value: JSON.parse(text) as unknown };
      } catch {
        return { error: 'Enter valid JSON.' };
      }
  }
}

const copyByKind = {
  attribute: {
    title: 'Set attribute',
    nameLabel: 'Attribute key',
    description:
      'Writes one Attribute of this run. The value replaces the current value. The Flow sees it on its next read.',
    submit: 'Set attribute',
    submitting: 'Setting…',
    url: '/api/flows/attributes',
    method: 'PUT',
  },
  channel: {
    title: 'Publish channel message',
    nameLabel: 'Channel name',
    description:
      'Publishes one message to a Channel of this run. A waiting Step consumes it like any other published message.',
    submit: 'Publish message',
    submitting: 'Publishing…',
    url: '/api/flows/channels/messages',
    method: 'POST',
  },
} as const;

export function StateWriteDialog({
  kind,
  summary,
  onClose,
  onWritten,
}: {
  kind: StateWriteKind | null;
  summary: FlowSummary;
  onClose: () => void;
  onWritten: () => Promise<void>;
}) {
  const [name, setName] = useState('');
  const [valueType, setValueType] = useState<DebugValueType>('json');
  const [text, setText] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');

  if (!kind) return null;
  const copy = copyByKind[kind];
  const canSubmit = !submitting && name.trim() !== '';

  async function submit() {
    if (!kind) return;
    const parsed = parseDebugValue(valueType, text);
    if ('error' in parsed) {
      setError(parsed.error);
      return;
    }
    setSubmitting(true);
    setError('');
    const target = kind === 'attribute' ? { key: name.trim() } : { channelName: name.trim() };
    try {
      await readResponseJSON(await fetch(copy.url, {
        method: copy.method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          flowId: summary.flowId,
          runId: summary.runId,
          ...target,
          valueType,
          value: parsed.value,
        }),
      }));
      await onWritten();
      onClose();
    } catch (writeError) {
      setError(writeError instanceof Error ? writeError.message : `${copy.title} failed`);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={onClose}>
      <div className="modal" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
        <div className="modal-header">
          <div><p className="eyebrow">Write live state</p><h2>{copy.title}</h2></div>
          <button className="icon-button" aria-label="Close" onClick={onClose}>×</button>
        </div>
        <p>{copy.description}</p>
        <label>
          {copy.nameLabel}
          <input value={name} onChange={(event) => setName(event.target.value)} />
        </label>
        <label>
          Value type
          <select
            value={valueType}
            onChange={(event) => setValueType(event.target.value as DebugValueType)}
          >
            {valueTypes.map((item) => (
              <option value={item.value} key={item.value}>{item.label}</option>
            ))}
          </select>
        </label>
        <label>
          Value
          <textarea rows={5} value={text} onChange={(event) => setText(event.target.value)} />
        </label>
        {error && <div className="error-banner">{error}</div>}
        <div className="modal-actions">
          <button className="button ghost" onClick={onClose}>Close</button>
          <button className="button" disabled={!canSubmit} onClick={() => void submit()}>
            {submitting ? copy.submitting : copy.submit}
          </button>
        </div>
      </div>
    </div>
  );
}
