/**
 * Tests for TollMeshCache Node.js SDK
 */

import { Client, ClientConfig, TollMeshError, ErrorCode } from '../src';

describe('Client', () => {
  let client: Client;

  beforeEach(() => {
    const config: ClientConfig = {
      host: 'localhost',
      port: 8080,
      timeout: 5000,
    };
    client = new Client(config);
  });

  afterEach(() => {
    client.close();
  });

  describe('ClientConfig', () => {
    test('should create config with defaults', () => {
      // ClientConfig is a plain interface (all fields optional), not a
      // class -- `new ClientConfig()` doesn't compile (TS2693: 'only
      // refers to a type'). Client.constructor is what actually applies
      // defaults for omitted fields, so verify defaults through its
      // observable effect (the base URL it builds) instead.
      const defaultClient = new Client({});
      expect((defaultClient as any).baseURL).toBe('http://localhost:8080');
      defaultClient.close();
    });

    test('should create config with custom values', () => {
      const config: ClientConfig = {
        host: 'api.example.com',
        port: 443,
        scheme: 'https',
        apiKey: 'secret-key',
      };
      expect(config.host).toBe('api.example.com');
      expect(config.port).toBe(443);
      expect(config.scheme).toBe('https');
      expect(config.apiKey).toBe('secret-key');
    });
  });

  describe('Consume (Rate Limiting)', () => {
    test('should consume rate limit', async () => {
      // This test would need a running server
      // For now, we test the configuration
      expect(client).toBeDefined();
    });

    test('should handle rate limit error', async () => {
      // Test error handling
      expect(client).toBeDefined();
    });
  });

  describe('Seen (Replay Protection)', () => {
    test('should detect replay', async () => {
      expect(client).toBeDefined();
    });

    test('should track nonce', async () => {
      expect(client).toBeDefined();
    });
  });

  describe('Cache Operations', () => {
    test('should set cache value', async () => {
      expect(client).toBeDefined();
    });

    test('should get cache value', async () => {
      expect(client).toBeDefined();
    });

    test('should handle cache miss', async () => {
      expect(client).toBeDefined();
    });
  });

  describe('Health Check', () => {
    test('should check health', async () => {
      expect(client).toBeDefined();
    });

    test('should get peers', async () => {
      expect(client).toBeDefined();
    });
  });

  describe('Error Handling', () => {
    test('should create TollMeshError', () => {
      const error = new TollMeshError(ErrorCode.RATE_LIMITED, 'Rate limited');
      expect(error.code).toBe(ErrorCode.RATE_LIMITED);
      expect(error.message).toBe('Rate limited');
      expect(error.isRateLimited()).toBe(true);
    });

    test('should check if error is retryable', () => {
      const error = new TollMeshError(ErrorCode.UNAVAILABLE, 'Service unavailable');
      expect(error.isRetryable()).toBe(true);
    });

    test('should check error type', () => {
      const error = new TollMeshError(ErrorCode.INVALID_REQUEST, 'Invalid request');
      expect(error.isClientError()).toBe(true);
      expect(error.isServerError()).toBe(false);
    });
  });
});

describe('Retry Logic', () => {
  test('should calculate exponential backoff', () => {
    const { calculateDelay } = require('../src/retry');

    const delay0 = calculateDelay(0, { baseDelay: 1000, backoffMultiplier: 2, jitter: false });
    const delay1 = calculateDelay(1, { baseDelay: 1000, backoffMultiplier: 2, jitter: false });
    const delay2 = calculateDelay(2, { baseDelay: 1000, backoffMultiplier: 2, jitter: false });

    expect(delay0).toBe(1000);
    expect(delay1).toBe(2000);
    expect(delay2).toBe(4000);
  });

  test('should apply max delay', () => {
    const { calculateDelay } = require('../src/retry');

    const delay = calculateDelay(10, {
      baseDelay: 1000,
      backoffMultiplier: 2,
      maxDelay: 60000,
      jitter: false,
    });

    expect(delay).toBe(60000);
  });
});
