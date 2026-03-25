import childProcess from 'node:child_process';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import process from 'node:process';
import url from 'node:url';

import {WebSocketServer} from 'ws';

import {
  createSessionCommand,
  resolveBoltfsBinary,
  resolveStaticDir,
} from './server-config.mjs';

const hereDir = path.dirname(url.fileURLToPath(import.meta.url));
const staticDir = resolveStaticDir(hereDir);
const binaryPath = resolveBoltfsBinary(hereDir, process.env.BOLTFS_BINARY ?? '');
const host = process.env.HOST ?? '0.0.0.0';
const port = Number.parseInt(process.env.PORT ?? '8080', 10);

function sendFile(response, filePath, contentType) {
  const body = fs.readFileSync(filePath);
  response.writeHead(200, {'content-type': contentType});
  response.end(body);
}

function sendNotFound(response) {
  response.writeHead(404, {'content-type': 'text/plain; charset=utf-8'});
  response.end('Not found');
}

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
      return sendFile(response, path.join(staticDir, 'index.html'), 'text/html; charset=utf-8');
    }

    if (requestUrl.pathname === '/app.js') {
      return sendFile(response, path.join(staticDir, 'app.js'), 'text/javascript; charset=utf-8');
    }

    if (requestUrl.pathname === '/terminal-ui.mjs') {
      return sendFile(
          response,
          path.join(staticDir, 'terminal-ui.mjs'),
          'text/javascript; charset=utf-8');
    }

    if (requestUrl.pathname === '/input-controller.mjs') {
      return sendFile(
          response,
          path.join(staticDir, 'input-controller.mjs'),
          'text/javascript; charset=utf-8');
    }

    if (requestUrl.pathname === '/output-format.mjs') {
      return sendFile(
          response,
          path.join(staticDir, 'output-format.mjs'),
          'text/javascript; charset=utf-8');
    }

    if (requestUrl.pathname === '/style.css') {
      return sendFile(response, path.join(staticDir, 'style.css'), 'text/css; charset=utf-8');
    }

    return sendNotFound(response);
  });

  const wss = new WebSocketServer({server, path: '/ws'});

  wss.on('connection', (socket) => {
    const session = createSessionCommand(binaryPath);
    const child = childProcess.spawn(session.file, session.args, {
      cwd: process.cwd(),
      env: {
        ...process.env,
        ...session.env,
      },
      stdio: 'pipe',
    });

    socket.send(
        JSON.stringify({
          type: 'meta',
          host,
          port,
          binaryPath,
        }));

    child.stdout.on('data', (data) => {
      if (socket.readyState === socket.OPEN) {
        socket.send(JSON.stringify({type: 'output', data: String(data)}));
      }
    });

    child.stderr.on('data', (data) => {
      if (socket.readyState === socket.OPEN) {
        socket.send(JSON.stringify({type: 'output', data: String(data)}));
      }
    });

    child.on('exit', (exitCode) => {
      if (socket.readyState === socket.OPEN) {
        socket.send(JSON.stringify({type: 'exit', exitCode: exitCode ?? 0}));
        socket.close();
      }
    });

    socket.on('message', (raw) => {
      const message = JSON.parse(String(raw));
      if (message.type === 'input') {
        child.stdin.write(message.data);
      }
    });

    socket.on('close', () => {
      if (!child.killed) {
        child.kill();
      }
    });
  });

  server.listen(port, host, () => {
    console.log(`BoltFS web demo listening on http://${host}:${port}`);
    console.log(`Using BoltFS binary: ${binaryPath}`);
  });
}

createServer();
