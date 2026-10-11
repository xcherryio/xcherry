// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { describe, expect, it } from 'vitest';
import { parseDebugValue } from './StateWriteDialog';

describe('parseDebugValue', () => {
  it('keeps int64 text so large values keep their precision', () => {
    expect(parseDebugValue('int64', ' 9007199254740993 ')).toEqual({ value: '9007199254740993' });
    expect(parseDebugValue('int64', '1.5')).toEqual({ error: 'Enter a whole number.' });
  });

  it('parses JSON, numbers, and booleans', () => {
    expect(parseDebugValue('json', '{"approved":true}')).toEqual({ value: { approved: true } });
    expect(parseDebugValue('json', '{oops')).toEqual({ error: 'Enter valid JSON.' });
    expect(parseDebugValue('double', '2.5')).toEqual({ value: 2.5 });
    expect(parseDebugValue('double', '')).toEqual({ error: 'Enter a finite number.' });
    expect(parseDebugValue('bool', 'false')).toEqual({ value: false });
    expect(parseDebugValue('bool', 'no')).toEqual({ error: 'Enter true or false.' });
  });

  it('sends string text unchanged', () => {
    expect(parseDebugValue('string', '  padded  ')).toEqual({ value: '  padded  ' });
  });
});
