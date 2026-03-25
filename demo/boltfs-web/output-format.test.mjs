import test from 'node:test';
import assert from 'node:assert/strict';

import {normalizeTerminalOutput} from './public/output-format.mjs';

test('normalizeTerminalOutput converts lf to crlf for xterm rendering', () => {
  assert.equal(
      normalizeTerminalOutput('line1\nline2\nboltfs:/> '),
      'line1\r\nline2\r\nboltfs:/> ');
});

test('normalizeTerminalOutput preserves existing crlf without doubling carriage returns', () => {
  assert.equal(
      normalizeTerminalOutput('line1\r\nline2\r\n'),
      'line1\r\nline2\r\n');
});
