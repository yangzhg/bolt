function isPrintable(char) {
  return char >= ' ' && char !== '\u007f';
}

export function createInputController(terminal, socket) {
  let lineBuffer = '';

  function sendLine() {
    terminal.write('\r\n');
    socket.send(JSON.stringify({type: 'input', data: `${lineBuffer}\n`}));
    lineBuffer = '';
  }

  function backspace() {
    if (!lineBuffer) {
      return;
    }
    lineBuffer = lineBuffer.slice(0, -1);
    terminal.write('\b \b');
  }

  function sendRaw(char) {
    socket.send(JSON.stringify({type: 'input', data: char}));
  }

  function onData(data) {
    for (const char of data) {
      if (char === '\r') {
        sendLine();
      } else if (char === '\u007f') {
        backspace();
      } else if (char === '\u0003') {
        terminal.write('^C\r\n');
        lineBuffer = '';
        sendRaw(char);
      } else if (isPrintable(char)) {
        lineBuffer += char;
        terminal.write(char);
      }
    }
  }

  function reset() {
    lineBuffer = '';
  }

  return {
    onData,
    reset,
  };
}
