import fs from 'node:fs';

export class MessageValidationError extends Error {}

export function installProcessErrorHandlers(targetProcess = process, logger = console) {
  targetProcess.on('uncaughtException', (error) => {
    logger.error('BoltFS web demo uncaught exception', error);
    targetProcess.exitCode = 1;
  });

  targetProcess.on('unhandledRejection', (reason) => {
    logger.error('BoltFS web demo unhandled rejection', reason);
    targetProcess.exitCode = 1;
  });
}

export function sendNotFound(response) {
  response.writeHead(404, {'content-type': 'text/plain; charset=utf-8'});
  response.end('Not found');
}

export function sendInternalError(response) {
  response.writeHead(500, {'content-type': 'text/plain; charset=utf-8'});
  response.end('Internal server error');
}

export function sendStaticFile(response, filePath, contentType, logger = console) {
  try {
    const body = fs.readFileSync(filePath);
    response.writeHead(200, {'content-type': contentType});
    response.end(body);
  } catch (error) {
    if (error?.code === 'ENOENT') {
      logger.warn(`Static file not found: ${filePath}`);
      sendNotFound(response);
      return;
    }
    logger.error(`Failed to read static file: ${filePath}`, error);
    sendInternalError(response);
  }
}

export function parseClientMessage(raw) {
  let message;
  try {
    message = JSON.parse(String(raw));
  } catch {
    throw new MessageValidationError('Invalid websocket message JSON');
  }

  if (!message || typeof message !== 'object') {
    throw new MessageValidationError('Websocket message must be an object');
  }

  if (message.type === 'input') {
    if (typeof message.data !== 'string') {
      throw new MessageValidationError('Input messages must include string data');
    }
    return message;
  }

  return message;
}
