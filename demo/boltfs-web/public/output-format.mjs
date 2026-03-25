export function normalizeTerminalOutput(text) {
  return text.replace(/\r?\n/g, '\r\n');
}
