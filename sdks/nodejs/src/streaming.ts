/**
 * Streaming support for large cache values
 */

import { Readable } from 'stream';
import { TollMeshError, ErrorCode } from './errors';

export interface StreamOptions {
  chunkSize?: number;
  timeout?: number;
}

/**
 * Stream cache value from server
 */
export async function* streamCacheGet(
  namespace: string,
  key: string,
  options: StreamOptions = {}
): AsyncGenerator<Buffer> {
  const { chunkSize = 8192, timeout = 30000 } = options;

  // TODO: Implement streaming fetch
  // This is a placeholder for future streaming implementation
  throw new TollMeshError(
    ErrorCode.INTERNAL,
    'Streaming not yet implemented'
  );
}

/**
 * Stream cache value to server
 */
export async function streamCacheSet(
  namespace: string,
  key: string,
  stream: Readable,
  ttl?: number,
  options: StreamOptions = {}
): Promise<void> {
  const { chunkSize = 8192, timeout = 30000 } = options;

  // TODO: Implement streaming upload
  // This is a placeholder for future streaming implementation
  throw new TollMeshError(
    ErrorCode.INTERNAL,
    'Streaming not yet implemented'
  );
}

/**
 * Convert stream to buffer
 */
export async function streamToBuffer(
  stream: Readable,
  maxSize: number = 100 * 1024 * 1024 // 100MB default
): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let size = 0;

  for await (const chunk of stream) {
    if (!Buffer.isBuffer(chunk)) {
      chunks.push(Buffer.from(chunk));
    } else {
      chunks.push(chunk);
    }

    size += chunk.length;

    if (size > maxSize) {
      throw new TollMeshError(
        ErrorCode.INVALID_VALUE,
        `Stream exceeds maximum size of ${maxSize} bytes`
      );
    }
  }

  return Buffer.concat(chunks);
}

/**
 * Convert buffer to stream
 */
export function bufferToStream(buffer: Buffer): Readable {
  const readable = new Readable();
  readable.push(buffer);
  readable.push(null);
  return readable;
}

/**
 * Pipe stream with automatic chunking
 */
export async function pipeWithChunking(
  source: Readable,
  processor: (chunk: Buffer) => Promise<void>,
  chunkSize: number = 8192
): Promise<void> {
  let buffer = Buffer.alloc(0);

  for await (const data of source) {
    const chunk = Buffer.isBuffer(data) ? data : Buffer.from(data);
    buffer = Buffer.concat([buffer, chunk]);

    while (buffer.length >= chunkSize) {
      const processed = buffer.slice(0, chunkSize);
      buffer = buffer.slice(chunkSize);
      await processor(processed);
    }
  }

  // Process remaining data
  if (buffer.length > 0) {
    await processor(buffer);
  }
}
