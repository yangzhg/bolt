import path from 'node:path';

export function projectRootFromHere(hereDir) {
  return path.resolve(hereDir, '..', '..');
}

export function resolveStaticDir(hereDir) {
  return path.join(hereDir, 'public');
}

export function resolveBoltfsBinary(hereDir, override = '') {
  if (override) {
    return override;
  }

  return path.join(
      projectRootFromHere(hereDir),
      '_build/Release/bolt/tool/boltfs/boltfs');
}

export function createSessionCommand(binaryPath) {
  return {
    file: binaryPath,
    args: [],
    env: {
      BOLTFS_CLIENT_MODE: 'human',
    },
  };
}
