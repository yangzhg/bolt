import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';

import {
  createSessionCommand,
  projectRootFromHere,
  resolveBoltfsBinary,
  resolveStaticDir,
} from './server-config.mjs';

const here = '/Users/bytedance/workspace/github/bolt/demo/boltfs-web';

test('projectRootFromHere resolves repository root from demo dir', () => {
  assert.equal(
      projectRootFromHere(here),
      '/Users/bytedance/workspace/github/bolt');
});

test('resolveStaticDir points at public assets directory', () => {
  assert.equal(
      resolveStaticDir(here),
      '/Users/bytedance/workspace/github/bolt/demo/boltfs-web/public');
});

test('resolveBoltfsBinary prefers explicit environment override', () => {
  assert.equal(
      resolveBoltfsBinary(
          here,
          '/tmp/custom-boltfs',
      ),
      '/tmp/custom-boltfs');
});

test('resolveBoltfsBinary falls back to release binary in repo build tree', () => {
  assert.equal(
      resolveBoltfsBinary(here, ''),
      path.join(
          '/Users/bytedance/workspace/github/bolt',
          '_build/Release/bolt/tool/boltfs/boltfs'));
});

test('createSessionCommand only launches boltfs with fixed environment', () => {
  const command = createSessionCommand('/tmp/custom-boltfs');
  assert.deepEqual(command, {
    file: '/tmp/custom-boltfs',
    args: [],
    env: {
      BOLTFS_CLIENT_MODE: 'human',
    },
  });
});
