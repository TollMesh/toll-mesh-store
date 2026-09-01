/**
 * TollMeshCache Node.js SDK - HTTP Client implementation
 */

import axios, { AxiosInstance, AxiosRequestConfig } from 'axios';
import { TollMeshError, ErrorCode } from './errors';

export interface ClientConfig {
  host?: string;
  port?: number;
  timeout?: number;
  verifySSL?: boolean;
  apiKey?: string;
  scheme?: 'http' | 'https';
}

export interface ConsumeResult {
  ok: boolean;
  remaining: number;
  reset_at: number;
  error?: string;
}

export interface SeenResult {
  seen: boolean;
  error?: string;
}

export interface CacheValue {
  value: string | null;
  exists: boolean;
}

export interface HealthResponse {
  status: 'healthy' | 'degraded' | 'unhealthy';
  node: string;
  peers: number;
  stats?: {
    uptime_seconds: number;
    requests_total: number;
    latency_p99_ms: number;
    cache_hit_rate: number;
  };
}

export interface Peer {
  id: string;
  address: string;
  port: number;
  latency_ms: number;
}

export interface Job {
  id: string;
  queue: string;
  payload: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  priority: number;
  retry_count: number;
  max_retries: number;
  processed_by: string;
  result: string | null;
  error: string;
  created_at: number;
  updated_at: number;
  deadline_at: number;
}

export interface SortedSetMember {
  member: string;
  score: number;
  timestamp: number;
  node: string;
}

export interface StreamEntry {
  id: string;
  timestamp: number;
  fields: Record<string, string>;
  node: string;
  sequence: number;
}

export class Client {
  private axiosInstance: AxiosInstance;
  private baseURL: string;

  constructor(config: ClientConfig = {}) {
    const {
      host = 'localhost',
      port = 8080,
      timeout = 5000,
      verifySSL = true,
      apiKey,
      scheme = 'http',
    } = config;

    this.baseURL = `${scheme}://${host}:${port}`;

    const axiosConfig: AxiosRequestConfig = {
      baseURL: this.baseURL,
      timeout,
      validateStatus: () => true, // Handle all status codes
    };

    if (!verifySSL && scheme === 'https') {
      axiosConfig.httpsAgent = { rejectUnauthorized: false } as any;
    }

    this.axiosInstance = axios.create(axiosConfig);

    if (apiKey) {
      this.axiosInstance.defaults.headers.common['X-API-Key'] = apiKey;
    }

    this.axiosInstance.defaults.headers.common['Content-Type'] = 'application/json';
  }

  /**
   * Check and consume rate limit tokens
   *
   * @param key - Rate limit key (e.g., "user-123")
   * @param limit - Maximum tokens allowed per window
   * @param windowMs - Time window in milliseconds
   * @returns Rate limit check result
   *
   * @example
   * const result = await client.consume('user-123', 100, 60000);
   * if (result.ok) {
   *   // Process request
   * } else {
   *   // Handle rate limit
   *   console.log(`Reset at: ${new Date(result.reset_at)}`);
   * }
   */
  async consume(key: string, limit: number, windowMs: number): Promise<ConsumeResult> {
    try {
      const response = await this.axiosInstance.post('/consume', {
        key,
        limit,
        window: windowMs,
      });

      if (response.status >= 400) {
        throw this.handleErrorResponse(response.data, response.status);
      }

      return response.data;
    } catch (error) {
      throw this.handleError(error);
    }
  }

  /**
   * Check replay protection - detect if nonce was already seen
   *
   * @param key - Nonce or unique identifier
   * @param ttlMs - Time-to-live in milliseconds
   * @returns Replay check result
   *
   * @example
   * const result = await client.seen('request-id-123', 300000);
   * if (result.seen) {
   *   throw new Error('Replay attack detected!');
   * }
   */
  async seen(key: string, ttlMs: number): Promise<SeenResult> {
    try {
      const response = await this.axiosInstance.post('/seen', {
        key,
        ttl: ttlMs,
      });

      if (response.status >= 400) {
        throw this.handleErrorResponse(response.data, response.status);
      }

      return response.data;
    } catch (error) {
      throw this.handleError(error);
    }
  }

  /**
   * Get value from distributed cache
   *
   * @param namespace - Cache namespace (e.g., "user-profiles")
   * @param key - Cache key within namespace
   * @returns Cached value and existence flag
   *
   * @example
   * const { value, exists } = await client.cacheGet('users', 'user-123');
   * if (exists) {
   *   console.log('Cached value:', value);
   * } else {
   *   // Fetch from source
   *   const data = await fetchUser('user-123');
   *   await client.cacheSet('users', 'user-123', JSON.stringify(data), 3600000);
   * }
   */
  async cacheGet(namespace: string, key: string): Promise<CacheValue> {
    try {
      const response = await this.axiosInstance.post('/cache/get', {
        namespace,
        key,
      });

      if (response.status >= 400) {
        throw this.handleErrorResponse(response.data, response.status);
      }

      return {
        value: response.data.value || null,
        exists: response.data.exists || false,
      };
    } catch (error) {
      throw this.handleError(error);
    }
  }

  /**
   * Set value in distributed cache
   *
   * @param namespace - Cache namespace
   * @param key - Cache key within namespace
   * @param value - Value to cache
   * @param ttlMs - Time-to-live in milliseconds (undefined = no expiration)
   *
   * @example
   * const userData = JSON.stringify({ name: 'Alice', email: 'alice@example.com' });
   * await client.cacheSet('users', 'user-123', userData, 3600000);
   */
  async cacheSet(
    namespace: string,
    key: string,
    value: string,
    ttlMs?: number
  ): Promise<void> {
    try {
      const data: any = { namespace, key, value };
      if (ttlMs !== undefined) {
        data.ttl = ttlMs;
      }

      const response = await this.axiosInstance.post('/cache/set', data);

      if (response.status >= 400) {
        throw this.handleErrorResponse(response.data, response.status);
      }
    } catch (error) {
      throw this.handleError(error);
    }
  }

  /**
   * Check server health
   *
   * @returns Health status and statistics
   *
   * @example
   * const health = await client.health();
   * console.log(`Status: ${health.status}, Peers: ${health.peers}`);
   */
  async health(): Promise<HealthResponse> {
    try {
      const response = await this.axiosInstance.get('/health');

      if (response.status >= 400) {
        throw this.handleErrorResponse(response.data, response.status);
      }

      return response.data;
    } catch (error) {
      throw this.handleError(error);
    }
  }

  /**
   * Get list of connected cluster peers
   *
   * @returns Array of connected peers with latency information
   */
  async getPeers(): Promise<Peer[]> {
    try {
      const response = await this.axiosInstance.get('/peers');

      if (response.status >= 400) {
        throw this.handleErrorResponse(response.data, response.status);
      }

      return response.data.peers || [];
    } catch (error) {
      throw this.handleError(error);
    }
  }

  // ===== Job Queues =====

  /**
   * Enqueue a job for distributed processing
   *
   * @example
   * const job = await client.enqueue('tasks', 'process-order-42', { priority: 8 });
   */
  async enqueue(
    queue: string,
    payload: string,
    opts: { priority?: number; maxRetries?: number; deadlineMs?: number } = {}
  ): Promise<Job> {
    const { priority = 5, maxRetries = 3, deadlineMs } = opts;
    return this.post('/queue/enqueue', {
      queue,
      payload,
      priority,
      max_retries: maxRetries,
      ...(deadlineMs !== undefined ? { deadline: deadlineMs } : {}),
    });
  }

  /** Claim the next available job from a queue */
  async claim(queue: string, workerId: string): Promise<Job> {
    return this.post('/queue/claim', { queue, worker_id: workerId });
  }

  /** Mark a claimed job as completed */
  async complete(queue: string, jobId: string, result: string = ''): Promise<void> {
    await this.post('/queue/complete', { queue, job_id: jobId, result });
  }

  /** Mark a claimed job as failed, triggering retry or dead-lettering */
  async fail(queue: string, jobId: string, error: string): Promise<void> {
    await this.post('/queue/fail', { queue, job_id: jobId, error });
  }

  /** Get the current status of a job */
  async jobStatus(queue: string, jobId: string): Promise<Job> {
    return this.get('/queue/status', { queue, job_id: jobId });
  }

  /** Get statistics for a queue */
  async queueStats(queue: string): Promise<Record<string, unknown>> {
    return this.get('/queue/stats', { queue });
  }

  // ===== Sorted Sets =====

  /**
   * Add or update a member's score in a sorted set
   *
   * @example
   * await client.zadd('leaderboard', 100, 'alice');
   */
  async zadd(key: string, score: number, member: string): Promise<void> {
    await this.post('/zset/add', { key, member, score });
  }

  /** Remove a member from a sorted set */
  async zrem(key: string, member: string): Promise<void> {
    await this.post('/zset/remove', { key, member });
  }

  /** Get a member's score */
  async zscore(key: string, member: string): Promise<{ score: number | null; exists: boolean }> {
    return this.get('/zset/score', { key, member });
  }

  /** Get a member's ascending-order rank (0 = lowest score) */
  async zrank(key: string, member: string): Promise<{ rank: number | null; exists: boolean }> {
    return this.get('/zset/rank', { key, member });
  }

  /** Get a member's descending-order rank (0 = highest score) */
  async zrevrank(key: string, member: string): Promise<{ rank: number | null; exists: boolean }> {
    return this.get('/zset/revrank', { key, member });
  }

  /** Get members with scores in [min, max], ascending order */
  async zrange(
    key: string,
    min: number = -Infinity,
    max: number = Infinity,
    limit: number = 100
  ): Promise<SortedSetMember[]> {
    const response = await this.get<{ members: SortedSetMember[] }>('/zset/range', { key, min, max, limit });
    return response.members || [];
  }

  /**
   * Get members with scores in [min, max], descending order (highest first)
   *
   * @example
   * const top10 = await client.zrevrange('leaderboard', undefined, undefined, 10);
   */
  async zrevrange(
    key: string,
    max: number = Infinity,
    min: number = -Infinity,
    limit: number = 100
  ): Promise<SortedSetMember[]> {
    const response = await this.get<{ members: SortedSetMember[] }>('/zset/revrange', { key, max, min, limit });
    return response.members || [];
  }

  /** Get the number of members in a sorted set */
  async zcard(key: string): Promise<number> {
    const response = await this.get<{ card: number }>('/zset/card', { key });
    return response.card || 0;
  }

  // ===== Streams =====

  /**
   * Append a new entry to a stream
   *
   * @example
   * const entry = await client.xadd('events', { type: 'login', user: 'alice' });
   */
  async xadd(stream: string, fields: Record<string, string>): Promise<StreamEntry> {
    return this.post('/stream/add', { stream, fields });
  }

  /** Get entries from a stream between start and end IDs */
  async xrange(stream: string, start: string = '0', end: string = '-', limit: number = 100): Promise<StreamEntry[]> {
    const response = await this.get<{ entries: StreamEntry[] }>('/stream/range', { stream, start, end, limit });
    return response.entries || [];
  }

  /** Get the number of entries in a stream */
  async xlen(stream: string): Promise<number> {
    const response = await this.get<{ length: number }>('/stream/len', { stream });
    return response.length || 0;
  }

  /** Create a consumer group for a stream */
  async xgroupCreate(stream: string, group: string): Promise<void> {
    await this.post('/stream/group/create', { stream, group });
  }

  /**
   * Read unacknowledged entries for a consumer in a group.
   *
   * First call for a given consumer registers it in the group. Entries
   * remain re-deliverable until acknowledged with xack.
   *
   * @example
   * await client.xgroupCreate('events', 'analytics');
   * const entries = await client.xreadgroup('analytics', 'worker-1', 'events');
   * for (const entry of entries) {
   *   // ... process entry.fields ...
   *   await client.xack('events', 'analytics', 'worker-1', entry.id);
   * }
   */
  async xreadgroup(group: string, consumer: string, stream: string, limit: number = 100): Promise<StreamEntry[]> {
    const response = await this.post<{ entries: StreamEntry[] }>('/stream/group/read', {
      stream,
      group,
      consumer,
      limit,
    });
    return response.entries || [];
  }

  /** Acknowledge that a consumer has processed up to entryId */
  async xack(stream: string, group: string, consumer: string, entryId: string): Promise<void> {
    await this.post('/stream/group/ack', { stream, group, consumer, id: entryId });
  }

  /**
   * Close client and cleanup resources
   */
  close(): void {
    // Cleanup if needed
  }

  private async get<T = any>(path: string, params?: Record<string, unknown>): Promise<T> {
    try {
      const response = await this.axiosInstance.get(path, { params });
      if (response.status >= 400) {
        throw this.handleErrorResponse(response.data, response.status);
      }
      return response.data;
    } catch (error) {
      throw this.handleError(error);
    }
  }

  private async post<T = any>(path: string, data: unknown): Promise<T> {
    try {
      const response = await this.axiosInstance.post(path, data);
      if (response.status >= 400) {
        throw this.handleErrorResponse(response.data, response.status);
      }
      return response.data;
    } catch (error) {
      throw this.handleError(error);
    }
  }

  private handleErrorResponse(data: any, status: number): TollMeshError {
    const code = data?.code || status;
    // /consume, /seen, /cache/* use "message"; the job queue, sorted set,
    // and stream endpoints use ErrorResponse{"error": ...} from api/http.go.
    const message = data?.message || data?.error || `HTTP ${status}`;
    return new TollMeshError(code, message, data?.details);
  }

  private handleError(error: any): Error {
    if (error instanceof TollMeshError) {
      return error;
    }

    if (error.response) {
      // Response received but outside 2xx range
      return this.handleErrorResponse(error.response.data, error.response.status);
    } else if (error.request) {
      // Request made but no response
      return new TollMeshError(
        ErrorCode.UNAVAILABLE,
        `No response from server: ${error.message}`
      );
    } else {
      // Error in request setup
      return new TollMeshError(
        ErrorCode.INTERNAL,
        `Request failed: ${error.message}`
      );
    }
  }
}

// Export for convenience
export default Client;
