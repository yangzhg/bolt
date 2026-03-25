import test from 'node:test';
import assert from 'node:assert/strict';

import {wireTerminalFocus} from './public/terminal-ui.mjs';

test('wireTerminalFocus focuses terminal immediately, on click, and on socket open', () => {
  let focusCount = 0;
  let clickHandler = null;
  let openHandler = null;

  const terminal = {
    focus() {
      focusCount += 1;
    },
  };

  const terminalNode = {
    addEventListener(type, handler) {
      if (type === 'click') {
        clickHandler = handler;
      }
    },
  };

  const socket = {
    addEventListener(type, handler) {
      if (type === 'open') {
        openHandler = handler;
      }
    },
  };

  wireTerminalFocus(terminal, terminalNode, socket);
  assert.equal(focusCount, 1);

  clickHandler();
  assert.equal(focusCount, 2);

  openHandler();
  assert.equal(focusCount, 3);
});
