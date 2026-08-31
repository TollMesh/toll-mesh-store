/**
 * Retry logic with exponential backoff
 */

import { TollMeshError, ErrorCode } from './errors';

export interface RetryConfig {
  maxRetries?: number;
  baseDelay?: number;
  maxDelay?: number;
  jitter?: boolean;
  backoffMultiplier?: number;
}

/**
 * Calculate delay for retry attempt with exponential backoff
 */
export function calculateDelay(
  attempt: number,
  config: RetryConfig = {}
): number {
  const {
    baseDelay = 1000,
    maxDelay = 60000,
    jitter = true,
    backoffMultiplier = 2.0,
  } = config;

  let delay = Math.min(
    baseDelay * Math.pow(backoffMultiplier, attempt),
    maxDelay
  );

  if (jitter) {
    const jitterAmount = delay * 0.25;
    delay += Math.random() * jitterAmount * 2 - jitterAmount;
  }

  return Math.max(0, Math.floor(delay));
}

/**
 * Check if error is retryable
 */
export function isRetryable(error: any): boolean {
  if (error instanceof TollMeshError) {
    return error.isRetryable();
  }
  // Retry on network errors, timeouts
  return error.code === 'ECONNREFUSED' ||
    error.code === 'ENOTFOUND' ||
    error.code === 'ETIMEDOUT' ||
    error.message?.includes('timeout');
}

/**
 * Sleep for specified milliseconds
 */
export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Retry a function with exponential backoff
 */
export async function withRetry<T>(
  fn: () => Promise<T>,
  config: RetryConfig = {}
): Promise<T> {
  const maxRetries = config.maxRetries ?? 3;
  let lastError: any;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;

      if (!isRetryable(error)) {
        throw error;
      }

      if (attempt < maxRetries) {
        const delay = calculateDelay(attempt, config);
        await sleep(delay);
      }
    }
  }

  throw lastError;
}

/**
 * Decorator for retrying async functions
 */
export function retry(config: RetryConfig = {}) {
  return function <T extends any[], R>(
    target: any,
    propertyKey: string,
    descriptor: PropertyDescriptor
  ): PropertyDescriptor {
    const originalMethod = descriptor.value;

    descriptor.value = async function (...args: T) {
      return withRetry(
        () => originalMethod.apply(this, args),
        config
      );
    };

    return descriptor;
  };
}
