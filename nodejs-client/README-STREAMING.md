# Node.js Streaming Integration for RemoteMedia Processing

This document describes the streaming capabilities added to the RemoteMedia Node.js client, enabling seamless integration between Node.js streams/generators and Python pipelines.

## Overview

The enhanced Node.js client provides several approaches for streaming data between Node.js and Python:

1. **Frame-by-frame pipeline execution** - Process individual frames through a complete pipeline
2. **Bidirectional streaming** - True streaming connection using gRPC StreamNode API
3. **Remote proxy with streaming** - Transparent remote execution with generator support

## Key Features

- ✅ **Async Generators**: Full support for Python async generators with transparent streaming
- ✅ **Node.js Streams**: Integration with Readable, Writable, and Transform streams
- ✅ **Batched Fetching**: Efficient batch retrieval of generator items
- ✅ **Type Safety**: Enhanced TypeScript types with concrete class generation
- ✅ **Pipeline Integration**: Direct execution of Python pipelines from Node.js
- ✅ **Memory Efficient**: Stream data without loading everything into memory

## Installation

```bash
npm install @remotemedia/nodejs-client
```

## Quick Start

### 1. Simple Pipeline Execution

Execute a Python pipeline from Node.js with streaming input:

```typescript
import { RemoteProxyClientStreaming } from '@remotemedia/nodejs-client';

const client = new RemoteProxyClientStreaming({
  host: 'localhost',
  port: 50052
});

// Create and execute pipeline
const pipeline = await client.createProxy({
  // Your pipeline configuration
});

// Stream data through pipeline
for await (const frame of audioFrameGenerator()) {
  const result = await pipeline.process_frame(frame);
  console.log('Processed:', result);
}
```

### 2. Generator Streaming

Automatically stream Python generators back to Node.js:

```typescript
// Python method returns a generator
const results = await remoteObject.generate_data(config);

// Automatically streams items
for await (const item of results) {
  console.log('Received:', item);
}
```

### 3. Bidirectional Streaming

True bidirectional streaming using gRPC:

```typescript
import { PipelineStreamClient } from './bidirectional-streaming';

const client = new PipelineStreamClient({
  host: 'localhost',
  port: 50052,
  nodeType: 'YourProcessingNode'
});

await client.connect();

// Send data
await client.send({ audio: frameData });

// Receive processed data
client.on('data', (result) => {
  console.log('Processed:', result);
});
```

## Architecture

### Remote Proxy Client Streaming

The enhanced `RemoteProxyClient` adds streaming capabilities:

```typescript
class RemoteProxyClient {
  // Creates proxy that handles generators transparently
  async createProxy<T>(obj: T, pythonCode?: string): Promise<T>;
  
  // Generator results automatically return AsyncIterables
  // Properties accessible with await
  // All methods work transparently
}
```

### Generator Detection and Streaming

When a Python method returns a generator:

1. Server detects generator and stores in session
2. Returns marker: `{"__generator__": true, "generator_id": "..."}`
3. Client creates `RemoteAsyncGenerator` proxy
4. Items fetched in batches as consumed

### Batched Fetching

Generators fetch items in configurable batches:

```typescript
const generator = new RemoteAsyncGenerator(client, generatorId, {
  batchSize: 10,  // Fetch 10 items at a time
  prefetch: true  // Prefetch next batch while consuming
});
```

## Examples

### Audio Pipeline Processing

Process audio frames through a VAD + speech recognition pipeline:

```typescript
// Generate audio frames
async function* generateAudioFrames() {
  for (let i = 0; i < 1000; i++) {
    yield {
      audio: new Float32Array(320),  // 20ms at 16kHz
      sampleRate: 16000,
      timestamp: Date.now()
    };
    await sleep(20);  // Real-time simulation
  }
}

// Process through pipeline
const pipeline = await createAudioPipeline(client);
for await (const frame of generateAudioFrames()) {
  const results = await pipeline.process(frame);
  
  if (results.is_speech) {
    console.log('Speech detected!');
  }
}
```

### Node.js Stream Integration

Use with Node.js streams:

```typescript
import { pipeline } from 'stream/promises';

const audioStream = createReadableStream();
const processor = new RemoteTransformStream(remotePipeline);
const output = createWritableStream();

await pipeline(audioStream, processor, output);
```

### Type-Safe Node Classes

Generate concrete TypeScript classes from Python nodes:

```typescript
// Run type generation
npm run generate-types:enhanced

// Use generated types
import { AudioTransform, VoiceActivityDetector } from './generated-types';

const transform = new AudioTransform({
  output_sample_rate: 16000,
  output_channels: 1
});

const vad = new VoiceActivityDetector({
  frame_duration_ms: 30,
  speech_threshold: 0.3
});
```

## Testing

Run the comprehensive test suite:

```bash
# Test generator support
npm run test:executenode:generators

# Test proxy streaming
npm run test:proxy:streaming

# Test full integration
npm run test:streaming:integration

# Run all streaming tests
npm run test:streaming
```

## Examples

Try the example applications:

```bash
# Simple pipeline execution
npm run example:simple-pipeline

# Frame-by-frame streaming
npm run example:pipeline-stream

# Bidirectional streaming
npm run example:bidirectional

# Generator streaming demo
npm run example:streaming-generators
```

## Server Configuration

Ensure the Python server has generator support enabled:

```python
# remote_service/src/server.py
# Generator support is built-in - no configuration needed
```

## Performance Considerations

1. **Batch Size**: Adjust batch size based on item size and network latency
2. **Prefetching**: Enable prefetching for smooth streaming
3. **Memory**: Generators don't load all data into memory
4. **Latency**: Bidirectional streaming has lower latency than request/response

## Troubleshooting

### Common Issues

1. **Generator not streaming**: Ensure server returns generator markers
2. **Type errors**: Regenerate types after Python changes
3. **Connection issues**: Check gRPC server is running on correct port

### Debug Logging

Enable debug logs:

```typescript
const client = new RemoteProxyClient({
  host: 'localhost',
  port: 50052,
  debug: true
});
```

## API Reference

### RemoteProxyClientStreaming

```typescript
class RemoteProxyClientStreaming {
  constructor(config: RemoteExecutorConfig);
  async connect(): Promise<void>;
  async createProxy<T>(obj: T, pythonCode?: string): Promise<T>;
  async executeMethod(objectId: string, method: string, args: any[]): Promise<any>;
  async close(): Promise<void>;
}
```

### RemoteAsyncGenerator

```typescript
class RemoteAsyncGenerator<T> implements AsyncIterable<T> {
  constructor(client: RemoteProxyClient, generatorId: string, options?: GeneratorOptions);
  [Symbol.asyncIterator](): AsyncIterator<T>;
}
```

### Stream Classes

```typescript
class RemoteReadableStream extends Readable {
  constructor(generator: AsyncIterable<any>, options?: ReadableOptions);
}

class RemoteWritableStream extends Writable {
  constructor(remoteProcessor: any, options?: WritableOptions);
}

class RemoteTransformStream extends Transform {
  constructor(remoteProcessor: any, options?: TransformOptions);
}
```

## Future Enhancements

- [ ] WebSocket transport for lower latency
- [ ] Automatic reconnection handling
- [ ] Stream compression options
- [ ] Progress callbacks for long operations
- [ ] Parallel stream processing

## Contributing

See the main project README for contribution guidelines.