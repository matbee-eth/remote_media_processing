# Node.js Streaming and Generators Support

This document describes the enhanced streaming and generator support for the RemoteMedia Processing SDK in Node.js/TypeScript.

## Overview

The RemoteMedia SDK now provides full support for Node.js Streams and Generators as inputs and outputs to any RemoteExecutionService. This enables efficient processing of large datasets and real-time data streams without loading everything into memory.

## Key Features

### 1. **Async Generator Support**
- Both sync and async Python generators are exposed as async generators in Node.js
- Automatic batched fetching for efficiency (configurable batch size)
- Early termination support with proper resource cleanup
- Transparent serialization/deserialization of yielded values

### 2. **Node.js Stream Integration**
- `Readable`, `Writable`, and `Transform` stream support
- Bidirectional streaming for real-time data processing
- Backpressure handling for flow control
- Integration with Node.js stream pipelines

### 3. **Type-Safe Generated Classes**
- Automatic TypeScript class generation from Python node definitions
- Concrete classes with proper typing for all methods
- Proxy classes for transparent remote execution
- Full IntelliSense support in IDEs

### 4. **Transparent Remote Execution**
- Any JavaScript/TypeScript object can be made remote
- Automatic detection of generator/stream return types
- Session management for stateful operations
- Support for pip package dependencies

## Installation

```bash
npm install @remotemedia/nodejs-client
```

## Basic Usage

### Using the Enhanced Remote Proxy Client

```typescript
import { RemoteProxyClient, RemoteExecutorConfig } from '@remotemedia/nodejs-client';

const config: RemoteExecutorConfig = {
  host: 'localhost',
  port: 50052,
  batchSize: 10, // Generator batch size
  pipPackages: ['numpy', 'pandas'] // Required Python packages
};

// Create a proxy for any object
const client = new RemoteProxyClient(config);
await client.connect();

const myProcessor = new DataProcessor();
const remoteProcessor = await client.createProxy(myProcessor);

// Generators automatically become async iterables
const dataStream = await remoteProcessor.generateData(1000);
for await (const item of dataStream) {
  console.log(item);
}
```

### Using Generated TypeScript Classes

First, generate the TypeScript definitions:

```bash
node scripts/generate-typescript-types-enhanced.js
```

Then use the generated classes:

```typescript
import { RemoteCalculatorNode } from './generated-types';

// Create a remote instance
const calculator = await RemoteCalculatorNode.create(
  { host: 'localhost', port: 50052 },
  'add' // operation parameter
);

// Use it like a local object
const result = await calculator.process({ a: 5, b: 3 });
```

## Streaming Examples

### 1. Basic Generator Streaming

```typescript
class DataProcessor {
  async *streamData(count: number): AsyncGenerator<any> {
    for (let i = 0; i < count; i++) {
      yield { id: i, value: Math.random() };
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }
}

const remote = await client.createProxy(new DataProcessor());
const stream = await remote.streamData(100);

// Process streaming data
for await (const item of stream) {
  console.log(item);
  if (item.id >= 10) break; // Early termination
}
```

### 2. Node.js Readable Stream

```typescript
import { Readable } from 'stream';

class StreamProcessor {
  createDataStream(): Readable {
    return new Readable({
      objectMode: true,
      read() {
        this.push({ timestamp: Date.now() });
      }
    });
  }
}

const remote = await client.createProxy(new StreamProcessor());
const readableStream = await remote.createDataStream();

// Use as a normal Node.js stream
readableStream.on('data', (chunk) => {
  console.log('Received:', chunk);
});
```

### 3. Stream Pipeline with Transforms

```typescript
import { pipeline } from 'stream/promises';
import { Transform } from 'stream';

// Create a transform stream
const doubleTransform = new Transform({
  objectMode: true,
  transform(chunk, encoding, callback) {
    callback(null, { ...chunk, value: chunk.value * 2 });
  }
});

// Create remote stream source
const source = await remote.createNumberStream(100);

// Pipeline with transformation
await pipeline(
  source,
  doubleTransform,
  async function* (source) {
    for await (const chunk of source) {
      console.log('Doubled:', chunk);
      yield chunk;
    }
  }
);
```

### 4. Batched Processing

```typescript
const config: RemoteExecutorConfig = {
  host: 'localhost',
  port: 50052,
  batchSize: 50 // Fetch 50 items at a time
};

const remote = await client.createProxy(processor, pythonCode, [], config);
const largeDataset = await remote.generateLargeDataset(10000);

// Items are fetched in batches of 50
for await (const item of largeDataset) {
  process(item);
}
```

## Advanced Features

### Generator Detection and Proxying

The client automatically detects when a method returns a generator and creates appropriate proxy objects:

```typescript
// Python side
class Processor:
    def generate_data(self):
        for i in range(100):
            yield i
    
    async def async_generate(self):
        for i in range(100):
            await asyncio.sleep(0.1)
            yield i

// TypeScript side - both become async generators
const syncGen = await remote.generate_data();
const asyncGen = await remote.async_generate();

for await (const item of syncGen) { /* ... */ }
for await (const item of asyncGen) { /* ... */ }
```

### Stream Type Detection

The system detects stream markers and creates appropriate Node.js stream proxies:

```typescript
// Markers in response indicate stream type
if (result.__stream__) {
  switch (result.stream_type) {
    case 'readable':
      return new RemoteReadableStream(client, result.stream_id);
    case 'writable':
      return new RemoteWritableStream(client, result.stream_id);
    case 'transform':
      return new RemoteTransformStream(client, result.stream_id);
  }
}
```

### Session Management

Remote objects maintain session state between calls:

```typescript
const remote = await client.createProxy(statefulProcessor);

// First call creates session
await remote.initialize({ config: 'value' });

// Subsequent calls use same session
await remote.processItem(item1);
await remote.processItem(item2);

// Session persists across generator calls
const results = await remote.generateResults();
for await (const result of results) {
  // Process results from same session
}
```

## Performance Considerations

1. **Batch Size**: Configure `batchSize` based on your use case:
   - Small batch size (1-10): Lower latency, more network overhead
   - Large batch size (50-100): Better throughput, higher latency

2. **Early Termination**: Breaking from a generator loop properly closes server resources:
   ```typescript
   for await (const item of generator) {
     if (condition) break; // Sends close signal to server
   }
   ```

3. **Memory Efficiency**: Generators stream data on-demand, avoiding memory issues:
   ```typescript
   // This won't load all 1M items into memory
   const huge = await remote.generateMillionItems();
   for await (const item of huge) {
     // Process one at a time
   }
   ```

## Error Handling

Errors are properly propagated from server to client:

```typescript
try {
  const generator = await remote.failingGenerator();
  for await (const item of generator) {
    // Process items
  }
} catch (error) {
  console.error('Generator failed:', error.message);
  // Includes full Python traceback if available
}
```

## API Reference

### RemoteProxyClient

```typescript
class RemoteProxyClient {
  constructor(config: RemoteExecutorConfig);
  
  // Connect to the remote service
  async connect(): Promise<void>;
  
  // Create a proxy for any object
  async createProxy<T extends object>(
    obj: T,
    pythonCode?: string,
    dependencies?: string[]
  ): Promise<RemoteProxy<T>>;
  
  // Create a stream proxy
  async createStreamProxy(
    stream: NodeJS.ReadableStream | NodeJS.WritableStream,
    pythonCode?: string
  ): Promise<RemoteReadableStream | RemoteWritableStream>;
  
  // Close the connection
  async close(): Promise<void>;
}
```

### RemoteAsyncGenerator

```typescript
class RemoteAsyncGenerator<T> implements AsyncIterable<T> {
  // Async iteration
  [Symbol.asyncIterator](): AsyncIterator<T>;
  
  // Close the generator and free resources
  async close(): Promise<void>;
}
```

### Generated Node Classes

```typescript
// Example generated class
export class RemoteTransformersPipelineNode extends TransformersPipelineNode {
  // Create remote instance
  static async create(
    config: RemoteExecutorConfig,
    ...args: ConstructorParameters<typeof TransformersPipelineNode>
  ): Promise<RemoteTransformersPipelineNode>;
  
  // All methods return promises
  async process(data: any): Promise<any>;
  
  // Generators return RemoteAsyncGenerator
  async *generateStream(data: any): RemoteAsyncGenerator<any>;
  
  // Streams return RemoteReadableStream/RemoteWritableStream
  createReadStream(): RemoteReadableStream;
}
```

## Migration Guide

### From Basic Remote Execution

Before:
```typescript
const response = await client.ExecuteNode({
  node_type: 'DataProcessor',
  input_data: Buffer.from(JSON.stringify(data))
});
const result = JSON.parse(response.output_data);
```

After:
```typescript
const processor = await RemoteDataProcessor.create(config);
const result = await processor.process(data);

// Or with streaming
const stream = await processor.generateData(1000);
for await (const item of stream) {
  // Process streaming data
}
```

### From Manual Generator Handling

Before:
```typescript
// Manual generator detection and iteration
if (result.__generator__) {
  // Complex manual fetching logic
}
```

After:
```typescript
// Automatic generator detection and proxy creation
const generator = await remote.generateData();
for await (const item of generator) {
  // Simple async iteration
}
```

## Best Practices

1. **Use Batching for Large Datasets**
   ```typescript
   const config = { batchSize: 50 };
   const remote = await createRemoteProxy(Processor, config);
   ```

2. **Handle Cleanup Properly**
   ```typescript
   const generator = await remote.generateData();
   try {
     for await (const item of generator) {
       // Process
     }
   } finally {
     await generator.close(); // Ensure cleanup
   }
   ```

3. **Type Your Data**
   ```typescript
   interface MyData {
     id: number;
     value: string;
   }
   
   async *generateTypedData(): AsyncGenerator<MyData> {
     // Implementation
   }
   ```

4. **Use Generated Types for Better DX**
   ```typescript
   // Get full IntelliSense and type checking
   const pipeline = await RemoteTransformersPipelineNode.create(
     config,
     'text-generation',
     'gpt2'
   );
   ```

## Troubleshooting

### Common Issues

1. **"Sync iteration not supported"**
   - Use `for await...of` instead of regular `for...of`
   - All remote generators are async in Node.js

2. **"Generator already closed"**
   - Don't reuse generator instances after iteration completes
   - Create a new generator for each iteration

3. **Performance Issues**
   - Increase `batchSize` for better throughput
   - Use streaming instead of loading all data at once

### Debug Mode

Enable debug logging:
```typescript
process.env.DEBUG = 'remotemedia:*';
```

## Examples

See the `examples/` directory for complete working examples:
- `streaming-generators-demo.ts` - Basic streaming examples
- `generated-types-streaming-demo.ts` - Using generated types
- `proxy/comprehensive-demo.ts` - Advanced proxy features

## Future Enhancements

- Transform stream support with bidirectional data flow
- WebSocket transport for lower latency streaming
- Automatic reconnection for long-running streams
- Stream composition and merging utilities