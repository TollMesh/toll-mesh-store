# Getting Started with TollMesh JavaScript/Node.js Client

## Prerequisites

- Node.js 14+ or browser with gRPC-web support
- npm or yarn package manager

## Installation

### npm

```bash
npm install @toll-mesh/client grpc @grpc/grpc-js @grpc/proto-loader
```

### yarn

```bash
yarn add @toll-mesh/client grpc @grpc/grpc-js @grpc/proto-loader
```

## Basic Usage

### Connect to TollMesh

```javascript
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const packageDefinition = protoLoader.loadSync('tollmesh.proto', {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});

const tollmesh = grpc.loadPackageDefinition(packageDefinition).tollmesh;

const client = new tollmesh.TollMesh(
  'localhost:50051',
  grpc.credentials.createInsecure()
);
```

### Rate Limiting

```javascript
function rateLimit() {
  const request = {
    key: 'user:123',
    limit: 100,
    window_ms: 60000
  };

  client.consume(request, (err, response) => {
    if (err) {
      console.error('Error:', err);
      return;
    }

    if (response.ok) {
      console.log('Request allowed. Remaining:', response.remaining);
    } else {
      console.log('Rate limit exceeded. Reset at:', response.reset_at);
    }
  });
}
```

### Async/Await Rate Limiting

```javascript
const util = require('util');

async function rateLimitAsync() {
  const consume = util.promisify(client.consume.bind(client));
  
  try {
    const response = await consume({
      key: 'user:123',
      limit: 100,
      window_ms: 60000
    });

    console.log('Rate limit OK:', response.ok);
    console.log('Remaining:', response.remaining);
  } catch (err) {
    console.error('Error:', err);
  }
}
```

### Cache Operations

```javascript
async function cacheOperations() {
  const set = util.promisify(client.set.bind(client));
  const get = util.promisify(client.get.bind(client));

  try {
    // Set value
    await set({
      key: 'mykey',
      value: Buffer.from('myvalue')
    });
    console.log('Value cached');

    // Get value
    const response = await get({ key: 'mykey' });
    if (response.found) {
      console.log('Value:', response.value.toString());
    }
  } catch (err) {
    console.error('Error:', err);
  }
}
```

### Search Operations

```javascript
async function search() {
  const search = util.promisify(client.search.bind(client));

  try {
    const response = await search({
      query: 'rate limit bypass',
      top_k: 5
    });

    response.results.forEach((result, index) => {
      console.log(`Result ${index + 1}:`);
      console.log('  ID:', result.id);
      console.log('  Score:', result.score);
      console.log('  Content:', result.content);
    });
  } catch (err) {
    console.error('Error:', err);
  }
}
```

### Agent Operations

```javascript
async function registerAgent() {
  const registerAgent = util.promisify(client.registerAgent.bind(client));

  try {
    const response = await registerAgent({
      id: 'agent-1',
      name: 'Browser Bot 1',
      capabilities: ['javascript', 'cookies'],
      reputation: 0.8
    });

    if (response.success) {
      console.log('Agent registered successfully');
    } else {
      console.log('Error:', response.error);
    }
  } catch (err) {
    console.error('Error:', err);
  }
}
```

### Pub/Sub Operations

```javascript
async function publish() {
  const publish = util.promisify(client.publish.bind(client));

  try {
    const response = await publish({
      topic: 'threats:evasion',
      data: Buffer.from(JSON.stringify({ type: 'evasion_attempt' }))
    });

    console.log('Published to', response.subscribers, 'subscribers');
  } catch (err) {
    console.error('Error:', err);
  }
}

function subscribe() {
  const call = client.subscribe({ topic: 'threats:*' });

  call.on('data', (message) => {
    console.log('Received message:');
    console.log('  Topic:', message.topic);
    console.log('  Data:', message.data.toString());
    console.log('  Timestamp:', message.timestamp);
  });

  call.on('error', (err) => {
    console.error('Stream error:', err);
  });

  call.on('end', () => {
    console.log('Stream ended');
  });
}
```

### Graph Operations

```javascript
async function addNode() {
  const addNode = util.promisify(client.addNode.bind(client));

  try {
    const response = await addNode({
      id: 'agent-bot-1',
      type: 'agent',
      properties: {
        name: 'Bot 1',
        type: 'browser'
      }
    });

    if (response.success) {
      console.log('Node added successfully');
    } else {
      console.log('Error:', response.error);
    }
  } catch (err) {
    console.error('Error:', err);
  }
}

async function addEdge() {
  const addEdge = util.promisify(client.addEdge.bind(client));

  try {
    const response = await addEdge({
      source: 'agent-bot-1',
      target: 'threat-evasion-1',
      type: 'detected_by',
      weight: 0.9
    });

    if (response.success) {
      console.log('Edge added successfully');
    } else {
      console.log('Error:', response.error);
    }
  } catch (err) {
    console.error('Error:', err);
  }
}
```

### Metrics

```javascript
async function getStats() {
  const getStats = util.promisify(client.getStats.bind(client));

  try {
    const response = await getStats({ component: 'all' });

    console.log('Statistics:');
    Object.entries(response.stats).forEach(([key, value]) => {
      console.log(`  ${key}: ${value}`);
    });
  } catch (err) {
    console.error('Error:', err);
  }
}
```

## Complete Example

```javascript
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const util = require('util');

async function main() {
  const packageDefinition = protoLoader.loadSync('tollmesh.proto', {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
  });

  const tollmesh = grpc.loadPackageDefinition(packageDefinition).tollmesh;
  const client = new tollmesh.TollMesh(
    'localhost:50051',
    grpc.credentials.createInsecure()
  );

  try {
    // Rate limiting
    const consume = util.promisify(client.consume.bind(client));
    const consumeResp = await consume({
      key: 'user:123',
      limit: 100,
      window_ms: 60000
    });
    console.log('Rate limit OK:', consumeResp.ok);

    // Cache operations
    const set = util.promisify(client.set.bind(client));
    await set({
      key: 'mykey',
      value: Buffer.from('myvalue')
    });
    console.log('Value cached');

    // Search
    const search = util.promisify(client.search.bind(client));
    const searchResp = await search({
      query: 'rate limit',
      top_k: 5
    });
    console.log('Found', searchResp.results.length, 'results');

    // Agent registration
    const registerAgent = util.promisify(client.registerAgent.bind(client));
    const agentResp = await registerAgent({
      id: 'agent-1',
      name: 'Browser Bot 1',
      capabilities: ['javascript', 'cookies'],
      reputation: 0.8
    });
    console.log('Agent registered:', agentResp.success);

  } catch (err) {
    console.error('Error:', err);
  }
}

main();
```

## Browser Usage (gRPC-web)

```html
<!DOCTYPE html>
<html>
<head>
  <script src="tollmesh_pb.js"></script>
  <script src="tollmesh_grpc_web_pb.js"></script>
</head>
<body>
  <script>
    const client = new tollmesh.TollMeshClient('http://localhost:8080');

    const request = new tollmesh.ConsumeRequest();
    request.setKey('user:123');
    request.setLimit(100);
    request.setWindowMs(60000);

    client.consume(request, {}, (err, response) => {
      if (err) {
        console.error('Error:', err);
        return;
      }
      console.log('Rate limit OK:', response.getOk());
    });
  </script>
</body>
</html>
```

## Error Handling

```javascript
async function handleErrors() {
  const consume = util.promisify(client.consume.bind(client));

  try {
    const response = await consume({
      key: 'user:123',
      limit: 100,
      window_ms: 60000
    });
  } catch (err) {
    console.error('Error code:', err.code);
    console.error('Error message:', err.message);
    console.error('Error details:', err.details);
  }
}
```

## Connection Options

```javascript
const options = {
  'grpc.max_receive_message_length': -1,
  'grpc.max_send_message_length': -1,
  'grpc.keepalive_time_ms': 30000,
  'grpc.keepalive_timeout_ms': 10000
};

const client = new tollmesh.TollMesh(
  'localhost:50051',
  grpc.credentials.createInsecure(),
  options
);
```

## TypeScript Support

```typescript
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

interface ConsumeRequest {
  key: string;
  limit: number;
  window_ms: number;
}

interface ConsumeResponse {
  ok: boolean;
  remaining: number;
  reset_at: number;
}

async function rateLimit(): Promise<void> {
  const packageDefinition = protoLoader.loadSync('tollmesh.proto');
  const tollmesh = grpc.loadPackageDefinition(packageDefinition).tollmesh;
  
  const client = new tollmesh.TollMesh(
    'localhost:50051',
    grpc.credentials.createInsecure()
  );

  const request: ConsumeRequest = {
    key: 'user:123',
    limit: 100,
    window_ms: 60000
  };

  const response: ConsumeResponse = await new Promise((resolve, reject) => {
    client.consume(request, (err: any, resp: ConsumeResponse) => {
      if (err) reject(err);
      else resolve(resp);
    });
  });

  console.log('Rate limit OK:', response.ok);
}
```

## Best Practices

1. **Connection Reuse**: Reuse client instances
2. **Error Handling**: Always handle errors in callbacks or try/catch
3. **Timeouts**: Set appropriate deadlines
4. **Resource Cleanup**: Close connections when done
5. **Streaming**: Use streaming for large datasets

## Testing

```javascript
const assert = require('assert');

describe('TollMesh Client', () => {
  it('should rate limit correctly', async () => {
    const consume = util.promisify(client.consume.bind(client));
    const response = await consume({
      key: 'test:123',
      limit: 10,
      window_ms: 60000
    });
    assert.strictEqual(response.ok, true);
  });
});
```

## Documentation

- [gRPC JavaScript Documentation](https://grpc.io/docs/languages/node-js/)
- [Protocol Buffers JavaScript Guide](https://developers.google.com/protocol-buffers/docs/reference/javascript-generated)
- [TollMesh Protocol Documentation](PROTOCOL.md)

## Support

For issues or questions:
- Open an issue on GitHub
- Check [DEVELOPMENT.md](DEVELOPMENT.md)
- Review [PROTOCOL.md](PROTOCOL.md)