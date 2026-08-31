/**
 * TollMeshCache SDK for Node.js
 * Distributed CRDT-based caching and coordination
 */

export { Client, ClientConfig, ConsumeResult, SeenResult, CacheValue, HealthResponse, Peer } from './client';
export { TollMeshError, ErrorCode, RateLimitError, ReplayError, CacheMissError, ErrorDetails } from './errors';
export { retry, withRetry, calculateDelay, isRetryable, sleep, RetryConfig } from './retry';
export { streamCacheGet, streamCacheSet, streamToBuffer, bufferToStream, pipeWithChunking, StreamOptions } from './streaming';

import Client from './client';

export default Client;
