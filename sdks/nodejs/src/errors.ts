/**
 * Error types and codes for TollMeshCache SDK
 */

export enum ErrorCode {
  // Success
  OK = 0,

  // Client errors (4xx)
  INVALID_REQUEST = 400,
  UNAUTHORIZED = 401,
  NOT_FOUND = 404,
  CONFLICT = 409,

  // Rate limiting
  RATE_LIMITED = 429,

  // Server errors (5xx)
  INTERNAL = 500,
  UNAVAILABLE = 503,
  DEADLINE_EXCEEDED = 504,

  // TollMesh-specific errors (1000+)
  REPLAY_DETECTED = 1001,
  CACHE_MISS = 1002,
  INVALID_NAMESPACE = 1003,
  INVALID_KEY = 1004,
  INVALID_TTL = 1005,
  INVALID_VALUE = 1006,
  PEER_UNAVAILABLE = 1007,
  GOSSIP_FAILED = 1008,
  TRANSACTION_FAILED = 1009,
  SCRIPT_ERROR = 1010,
  SEARCH_FAILED = 1011,
  GRAPH_ERROR = 1012,
}

export interface ErrorDetails {
  [key: string]: any;
}

/**
 * Base exception for TollMeshCache SDK
 *
 * @example
 * try {
 *   await client.consume('key', 100, 60000);
 * } catch (error) {
 *   if (error instanceof TollMeshError) {
 *     if (error.isRateLimited()) {
 *       console.log('Rate limited!');
 *     }
 *   }
 * }
 */
export class TollMeshError extends Error {
  constructor(
    public code: number | ErrorCode,
    public message: string,
    public details?: ErrorDetails
  ) {
    super(TollMeshError.formatMessage(code, message, details));
    Object.setPrototypeOf(this, TollMeshError.prototype);
  }

  private static formatMessage(
    code: number | ErrorCode,
    message: string,
    details?: ErrorDetails
  ): string {
    let msg = `Error ${code}: ${message}`;
    if (details && Object.keys(details).length > 0) {
      msg += ` (details: ${JSON.stringify(details)})`;
    }
    return msg;
  }

  /**
   * Check if this is a rate limit error
   */
  isRateLimited(): boolean {
    return this.code === ErrorCode.RATE_LIMITED;
  }

  /**
   * Check if this is a replay detection error
   */
  isReplay(): boolean {
    return this.code === ErrorCode.REPLAY_DETECTED;
  }

  /**
   * Check if this is a not found error
   */
  isNotFound(): boolean {
    return this.code === ErrorCode.NOT_FOUND;
  }

  /**
   * Check if this is a server error (5xx)
   */
  isServerError(): boolean {
    return this.code >= 500 && this.code < 600;
  }

  /**
   * Check if this is a client error (4xx)
   */
  isClientError(): boolean {
    return this.code >= 400 && this.code < 500;
  }

  /**
   * Check if the operation should be retried
   */
  isRetryable(): boolean {
    return [
      ErrorCode.UNAVAILABLE,
      ErrorCode.DEADLINE_EXCEEDED,
      ErrorCode.GOSSIP_FAILED,
    ].includes(this.code as ErrorCode);
  }
}

/**
 * Raised when rate limit is exceeded
 */
export class RateLimitError extends TollMeshError {
  constructor(
    public resetAt: number,
    details?: ErrorDetails
  ) {
    super(
      ErrorCode.RATE_LIMITED,
      'Rate limit exceeded',
      details
    );
    Object.setPrototypeOf(this, RateLimitError.prototype);
  }
}

/**
 * Raised when replay attack is detected
 */
export class ReplayError extends TollMeshError {
  constructor(
    public nonce: string,
    details?: ErrorDetails
  ) {
    const enhancedDetails = { ...details, nonce };
    super(
      ErrorCode.REPLAY_DETECTED,
      'Replay attack detected',
      enhancedDetails
    );
    Object.setPrototypeOf(this, ReplayError.prototype);
  }
}

/**
 * Raised when cache lookup misses
 */
export class CacheMissError extends TollMeshError {
  constructor(
    public namespace: string,
    public key: string
  ) {
    super(
      ErrorCode.CACHE_MISS,
      `Cache miss for ${namespace}/${key}`,
      { namespace, key }
    );
    Object.setPrototypeOf(this, CacheMissError.prototype);
  }
}

// Export for convenience
export default TollMeshError;
