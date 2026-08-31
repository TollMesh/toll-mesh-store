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

  /**
   * Close client and cleanup resources
   */
  close(): void {
    // Cleanup if needed
  }

  private handleErrorResponse(data: any, status: number): TollMeshError {
    const code = data?.code || status;
    const message = data?.message || `HTTP ${status}`;
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
