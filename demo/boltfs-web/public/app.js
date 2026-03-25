import {wireTerminalFocus} from './terminal-ui.mjs';
import {createInputController} from './input-controller.mjs';
import {normalizeTerminalOutput} from './output-format.mjs';

const terminalNode = document.getElementById('terminal');
const statusDot = document.getElementById('status-dot');
const statusText = document.getElementById('status-text');

const terminal = new Terminal({
  cursorBlink: true,
  fontFamily: '"SFMono-Regular", "Menlo", "Consolas", monospace',
  fontSize: 15,
  theme: {
    background: '#0b1117',
    foreground: '#f4f0e8',
    cursor: '#f6bd60',
    selectionBackground: '#24415d',
    black: '#0b1117',
    red: '#f15b5b',
    green: '#5ecf88',
    yellow: '#f6bd60',
    blue: '#7fb7ff',
    magenta: '#d997ff',
    cyan: '#74d3d3',
    white: '#f4f0e8',
    brightBlack: '#506174',
    brightRed: '#ff7d7d',
    brightGreen: '#7ee2a2',
    brightYellow: '#ffd27d',
    brightBlue: '#9dcbff',
    brightMagenta: '#e2b4ff',
    brightCyan: '#98e4e4',
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

wireTerminalFocus(terminal, terminalNode, socket);

function setStatus(text, connected) {
  statusText.textContent = text;
  statusDot.dataset.connected = String(connected);
}

socket.addEventListener('open', () => {
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
    terminal.writeln(`\x1b[90mConnected to ${message.binaryPath}\x1b[0m`);
  } else if (message.type === 'exit') {
    terminal.writeln(`\r\n\x1b[31mSession ended (exit ${message.exitCode})\x1b[0m`);
    inputController.reset();
    setStatus('Session ended', false);
  }
});

socket.addEventListener('close', () => {
  setStatus('Disconnected', false);
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
