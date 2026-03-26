import test from 'node:test';
import assert from 'node:assert/strict';

import {
  installProcessErrorHandlers,
  MessageValidationError,
  parseClientMessage,
  sendStaticFile,
} from './server-runtime.mjs';

test('parseClientMessage accepts valid input messages', () => {
  assert.deepEqual(
      parseClientMessage(JSON.stringify({type: 'input', data: 'help\n'})),
      {type: 'input', data: 'help\n'});
});

test('parseClientMessage rejects malformed json payloads', () => {
  assert.throws(
      () => parseClientMessage('{oops'),
      (error) => error instanceof MessageValidationError &&
          error.message === 'Invalid websocket message JSON');
});

test('parseClientMessage rejects input messages without string data', () => {
  assert.throws(
      () => parseClientMessage(JSON.stringify({type: 'input', data: 1})),
      (error) => error instanceof MessageValidationError &&
          error.message === 'Input messages must include string data');
});

test('sendStaticFile returns 404 when file is missing', () => {
  let statusCode = null;
  let headers = null;
  let body = null;

  const response = {
    writeHead(code, nextHeaders) {
      statusCode = code;
      headers = nextHeaders;
    },
    end(nextBody) {
      body = nextBody;
    },
  };

  const loggerCalls = [];
  const logger = {
    warn(message) {
      loggerCalls.push(message);
    },
    error() {
      throw new Error('sendStaticFile should not call error for missing files');
    },
  };

  sendStaticFile(
      response,
      '/tmp/boltfs-web-demo-file-that-should-not-exist',
      'text/plain; charset=utf-8',
      logger);

  assert.equal(statusCode, 404);
  assert.deepEqual(headers, {'content-type': 'text/plain; charset=utf-8'});
  assert.equal(body, 'Not found');
  assert.equal(loggerCalls.length, 1);
});

test('installProcessErrorHandlers logs uncaught exceptions and sets exitCode', () => {
  const handlers = new Map();
  const logs = [];
  const fakeProcess = {
    exitCode: 0,
    on(event, handler) {
      handlers.set(event, handler);
    },
  };
  const logger = {
    error(...args) {
      logs.push(args);
    },
  };

  installProcessErrorHandlers(fakeProcess, logger);

  const error = new Error('boom');
  handlers.get('uncaughtException')(error);

  assert.equal(fakeProcess.exitCode, 1);
  assert.equal(logs.length, 1);
  assert.equal(logs[0][0], 'BoltFS web demo uncaught exception');
  assert.equal(logs[0][1], error);
});

test('installProcessErrorHandlers logs unhandled rejections and sets exitCode', () => {
  const handlers = new Map();
  const logs = [];
  const fakeProcess = {
    exitCode: 0,
    on(event, handler) {
      handlers.set(event, handler);
    },
  };
  const logger = {
    error(...args) {
      logs.push(args);
    },
  };

  installProcessErrorHandlers(fakeProcess, logger);

  handlers.get('unhandledRejection')('bad promise');

  assert.equal(fakeProcess.exitCode, 1);
  assert.equal(logs.length, 1);
  assert.equal(logs[0][0], 'BoltFS web demo unhandled rejection');
  assert.equal(logs[0][1], 'bad promise');
});
