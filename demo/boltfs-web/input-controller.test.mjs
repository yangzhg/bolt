import test from 'node:test';
import assert from 'node:assert/strict';

import {createInputController} from './public/input-controller.mjs';

test('input controller locally echoes printable characters and sends line on enter', () => {
  let written = '';
  const sent = [];

  const terminal = {
    write(data) {
      written += data;
    },
  };

  const socket = {
    send(payload) {
      sent.push(JSON.parse(payload));
    },
  };

  const controller = createInputController(terminal, socket);
  controller.onData('abc');
  assert.equal(written, 'abc');
  assert.deepEqual(sent, []);

  controller.onData('\r');
  assert.equal(written, 'abc\r\n');
  assert.deepEqual(sent, [{type: 'input', data: 'abc\n'}]);
});

test('input controller handles backspace and reset', () => {
  let written = '';
  const sent = [];

  const terminal = {
    write(data) {
      written += data;
    },
  };

  const socket = {
    send(payload) {
      sent.push(JSON.parse(payload));
    },
  };

  const controller = createInputController(terminal, socket);
  controller.onData('ab');
  controller.onData('\u007f');
  controller.onData('c');
  controller.reset();
  controller.onData('\r');

  assert.equal(written, 'ab\b \bc\r\n');
  assert.deepEqual(sent, [{type: 'input', data: '\n'}]);
});
