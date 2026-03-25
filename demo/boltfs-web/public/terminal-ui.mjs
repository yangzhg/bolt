export function wireTerminalFocus(terminal, terminalNode, socket) {
  function focusTerminal() {
    terminal.focus();
  }

  focusTerminal();
  terminalNode.addEventListener('click', focusTerminal);

  socket.addEventListener('open', () => {
    focusTerminal();
  });
}
