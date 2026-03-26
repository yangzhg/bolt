import {wireTerminalFocus} from './terminal-ui.mjs';
import {createInputController} from './input-controller.mjs';
import {normalizeTerminalOutput} from './output-format.mjs';

const terminalNode = document.getElementById('terminal');
const statusDot = document.getElementById('status-dot');
const statusText = document.getElementById('status-text');
const sessionIdText = document.getElementById('session-id');
const idleTimeoutText = document.getElementById('idle-timeout');

const terminal = new Terminal({
  cursorBlink: true,
  fontFamily:
    '"SFMono-Regular", "SF Mono", "Menlo", "Consolas", monospace',
  fontSize: 15,
  theme: {
    background: '#0b1117',
    foreground: '#f3f7fb',
    cursor: '#7ab7ff',
    selectionBackground: '#24415d',
    black: '#0b1117',
    red: '#ff7b72',
    green: '#56d364',
    yellow: '#f2cc60',
    blue: '#7ab7ff',
    magenta: '#d2a8ff',
    cyan: '#76e3ea',
    white: '#f3f7fb',
    brightBlack: '#607080',
    brightRed: '#ff9b95',
    brightGreen: '#7be38c',
    brightYellow: '#ffd87a',
    brightBlue: '#9ecaff',
    brightMagenta: '#e2c2ff',
    brightCyan: '#9becf0',
    brightWhite: '#ffffff',
  },
});

const fitAddon = new FitAddon.FitAddon();
terminal.loadAddon(fitAddon);
terminal.open(terminalNode);
fitAddon.fit();

const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
const socket = new WebSocket(`${protocol}//${window.location.host}/ws`);
const inputController = createInputController(terminal, socket);
let finalStateReached = false;

wireTerminalFocus(terminal, terminalNode, socket);

function setStatus(text, connected) {
  statusText.textContent = text;
  statusDot.dataset.connected = String(connected);
}

function formatDuration(milliseconds) {
  const totalSeconds = Math.floor(milliseconds / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes > 0 && seconds > 0) {
    return `${minutes}m ${seconds}s`;
  }
  if (minutes > 0) {
    return `${minutes}m`;
  }
  return `${seconds}s`;
}

function describeExit(message) {
  if (message.reason === 'idle_timeout') {
    return 'Session expired due to inactivity. Refresh to start a new session.';
  }
  if (message.reason === 'process_exit') {
    return `Session ended (exit ${message.exitCode}). Refresh to start a new session.`;
  }
  return 'Session ended. Refresh to start a new session.';
}

socket.addEventListener('open', () => {
  finalStateReached = false;
  setStatus('Connected', true);
  fitAddon.fit();
  socket.send(
      JSON.stringify({
        type: 'resize',
        cols: terminal.cols,
        rows: terminal.rows,
      }));
});

socket.addEventListener('message', (event) => {
  const message = JSON.parse(event.data);
  if (message.type === 'output') {
    terminal.write(normalizeTerminalOutput(message.data));
  } else if (message.type === 'meta') {
    sessionIdText.textContent = message.sessionId;
    idleTimeoutText.textContent = formatDuration(message.idleTimeoutMs);
    terminal.writeln(
        `\x1b[90mSession ${message.sessionId} ready. Type help to get started.\x1b[0m`);
  } else if (message.type === 'exit') {
    terminal.writeln(`\r\n\x1b[31m${describeExit(message)}\x1b[0m`);
    inputController.reset();
    finalStateReached = true;
    setStatus(message.reason === 'idle_timeout' ? 'Idle timeout' : 'Session ended', false);
  }
});

socket.addEventListener('close', () => {
  if (!finalStateReached) {
    setStatus('Disconnected', false);
  }
});

socket.addEventListener('error', () => {
  setStatus('Connection error', false);
});

terminal.onData((data) => {
  inputController.onData(data);
});

window.addEventListener('resize', () => {
  fitAddon.fit();
  if (socket.readyState === WebSocket.OPEN) {
    socket.send(
        JSON.stringify({
          type: 'resize',
          cols: terminal.cols,
          rows: terminal.rows,
        }));
  }
});
