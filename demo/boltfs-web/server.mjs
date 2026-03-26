import childProcess from 'node:child_process';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import process from 'node:process';
import url from 'node:url';

import {WebSocketServer} from 'ws';

import {
  createSessionCommand,
  defaultIdleTimeoutMs,
  resolveBoltfsBinary,
  resolveStaticDir,
} from './server-config.mjs';
import {
  installProcessErrorHandlers,
  MessageValidationError,
  parseClientMessage,
  sendNotFound,
  sendStaticFile,
} from './server-runtime.mjs';

const hereDir = path.dirname(url.fileURLToPath(import.meta.url));
const staticDir = resolveStaticDir(hereDir);
const binaryPath = resolveBoltfsBinary(hereDir, process.env.BOLTFS_BINARY ?? '');
const host = process.env.HOST ?? '0.0.0.0';
const port = Number.parseInt(process.env.PORT ?? '8080', 10);
const idleTimeoutMs = Number.parseInt(
    process.env.BOLTFS_IDLE_TIMEOUT_MS ?? String(defaultIdleTimeoutMs()),
    10);
let nextSessionId = 1;

installProcessErrorHandlers(process, console);

function ensureBinaryExists(filePath) {
  try {
    fs.accessSync(filePath, fs.constants.X_OK);
  } catch (error) {
    throw new Error(
        `BoltFS binary is not executable: ${filePath}. ` +
        'Build `_build/Release/bolt/tool/boltfs/boltfs` first or set BOLTFS_BINARY.');
  }
}

function createServer() {
  ensureBinaryExists(binaryPath);

  const server = http.createServer((request, response) => {
    const requestUrl = new URL(request.url ?? '/', `http://${request.headers.host ?? 'localhost'}`);

    if (requestUrl.pathname === '/' || requestUrl.pathname === '/index.html') {
      return sendStaticFile(
          response,
          path.join(staticDir, 'index.html'),
          'text/html; charset=utf-8');
    }

    if (requestUrl.pathname === '/app.js') {
      return sendStaticFile(
          response,
          path.join(staticDir, 'app.js'),
          'text/javascript; charset=utf-8');
    }

    if (requestUrl.pathname === '/terminal-ui.mjs') {
      return sendStaticFile(
          response,
          path.join(staticDir, 'terminal-ui.mjs'),
          'text/javascript; charset=utf-8');
    }

    if (requestUrl.pathname === '/input-controller.mjs') {
      return sendStaticFile(
          response,
          path.join(staticDir, 'input-controller.mjs'),
          'text/javascript; charset=utf-8');
    }

    if (requestUrl.pathname === '/output-format.mjs') {
      return sendStaticFile(
          response,
          path.join(staticDir, 'output-format.mjs'),
          'text/javascript; charset=utf-8');
    }

    if (requestUrl.pathname === '/style.css') {
      return sendStaticFile(
          response,
          path.join(staticDir, 'style.css'),
          'text/css; charset=utf-8');
    }

    return sendNotFound(response);
  });

  const wss = new WebSocketServer({server, path: '/ws'});

  function logSession(sessionId, message, error = null) {
    if (error) {
      console.error(`[${sessionId}] ${message}`, error);
      return;
    }
    console.log(`[${sessionId}] ${message}`);
  }

  function sendSocketMessage(socket, payload, sessionId) {
    if (socket.readyState !== socket.OPEN) {
      return false;
    }
    try {
      socket.send(JSON.stringify(payload));
      return true;
    } catch (error) {
      logSession(sessionId, 'failed to send websocket message', error);
      return false;
    }
  }

  wss.on('connection', (socket) => {
    const sessionId = `boltfs-${String(nextSessionId).padStart(3, '0')}`;
    nextSessionId += 1;
    const session = createSessionCommand(binaryPath);
    const child = childProcess.spawn(session.file, session.args, {
      cwd: process.cwd(),
      env: {
        ...process.env,
        ...session.env,
      },
      stdio: 'pipe',
    });
    let closed = false;
    let idleTimer = null;
    let exitSent = false;

    logSession(sessionId, `session started with idle timeout ${idleTimeoutMs}ms`);

    function clearIdleTimer() {
      if (idleTimer !== null) {
        clearTimeout(idleTimer);
        idleTimer = null;
      }
    }

    function closeSession(reason = 'closed', exitCode = 0) {
      if (closed) {
        return;
      }
      closed = true;
      clearIdleTimer();
      logSession(sessionId, `session closing: reason=${reason} exitCode=${exitCode}`);
      if (!exitSent) {
        exitSent = sendSocketMessage(socket, {type: 'exit', exitCode, reason}, sessionId) || exitSent;
      }
      if (socket.readyState === socket.OPEN) {
        socket.close();
      }
      if (!child.killed) {
        child.kill();
      }
    }

    function resetIdleTimer() {
      clearIdleTimer();
      idleTimer = setTimeout(() => {
        closeSession('idle_timeout', 0);
      }, idleTimeoutMs);
    }

    resetIdleTimer();

    sendSocketMessage(
        socket,
        {
          type: 'meta',
          sessionId,
          host,
          port,
          binaryPath,
          idleTimeoutMs,
        },
        sessionId);

    child.stdout.on('data', (data) => {
      sendSocketMessage(socket, {type: 'output', data: String(data)}, sessionId);
    });

    child.stderr.on('data', (data) => {
      sendSocketMessage(socket, {type: 'output', data: String(data)}, sessionId);
    });

    child.on('error', (error) => {
      logSession(sessionId, 'child process error', error);
      closeSession('process_error', 1);
    });

    child.on('exit', (exitCode) => {
      if (!closed) {
        closeSession('process_exit', exitCode ?? 0);
      }
    });

    socket.on('message', (raw) => {
      try {
        const message = parseClientMessage(raw);
        if (message.type === 'input') {
          resetIdleTimer();
          child.stdin.write(message.data);
        }
      } catch (error) {
        if (error instanceof MessageValidationError) {
          logSession(sessionId, `dropping invalid websocket message: ${error.message}`);
          return;
        }
        logSession(sessionId, 'unexpected websocket message handling error', error);
        closeSession('message_error', 1);
      }
    });

    socket.on('error', (error) => {
      logSession(sessionId, 'websocket error', error);
    });

    socket.on('close', () => {
      clearIdleTimer();
      closed = true;
      logSession(sessionId, 'socket closed');
      if (!child.killed) {
        child.kill();
      }
    });
  });

  wss.on('error', (error) => {
    console.error('BoltFS websocket server error', error);
  });

  server.on('error', (error) => {
    console.error('BoltFS web demo server error', error);
  });

  server.listen(port, host, () => {
    console.log(`BoltFS web demo listening on http://${host}:${port}`);
    console.log(`Using BoltFS binary: ${binaryPath}`);
    console.log(`Session idle timeout: ${idleTimeoutMs}ms`);
  });
}

createServer();
