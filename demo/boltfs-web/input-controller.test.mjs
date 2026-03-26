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

test('input controller supports ctrl-w to delete the previous word', () => {
  let written = '';

  const terminal = {
    write(data) {
      written += data;
    },
  };

  const socket = {
    send() {},
  };

  const controller = createInputController(terminal, socket);
  controller.onData('hello world');
  controller.onData('\u0017');
  controller.onData('\r');

  assert.equal(
      written,
      'hello world' + '\b \b'.repeat('world'.length) + '\r\n');
});

test('input controller supports ctrl-u to clear the current line', () => {
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
  controller.onData('schema orders');
  controller.onData('\u0015');
  controller.onData('pwd');
  controller.onData('\r');

  assert.equal(
      written,
      'schema orders' + '\b \b'.repeat('schema orders'.length) + 'pwd\r\n');
  assert.deepEqual(sent, [{type: 'input', data: 'pwd\n'}]);
});
