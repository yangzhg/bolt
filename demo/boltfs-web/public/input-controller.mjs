function isPrintable(char) {
  return char >= ' ' && char !== '\u007f';
}

export function createInputController(terminal, socket) {
  let lineBuffer = '';

  function eraseChars(count) {
    if (count <= 0) {
      return;
    }
    terminal.write('\b \b'.repeat(count));
  }

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
    eraseChars(1);
  }

  function deleteWord() {
    const trimmed = lineBuffer.replace(/\s+$/, '');
    const nextBuffer = trimmed.replace(/\S+$/, '');
    eraseChars(lineBuffer.length - nextBuffer.length);
    lineBuffer = nextBuffer;
  }

  function clearLine() {
    eraseChars(lineBuffer.length);
    lineBuffer = '';
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
      } else if (char === '\u0017') {
        deleteWord();
      } else if (char === '\u0015') {
        clearLine();
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
