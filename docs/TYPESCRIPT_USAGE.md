# TypeScript/Node.js Integration Guide

This guide explains how to use the RemoteMedia Processing SDK from TypeScript and Node.js applications.

## Overview

The RemoteMedia Processing SDK provides TypeScript interface definitions that enable type-safe interaction with the remote processing service from Node.js applications. This allows you to:

- Execute processing nodes remotely with full type safety
- Stream data through processing pipelines
- Create custom nodes that run on powerful remote servers
- Leverage ML models without local GPU requirements

## Generating TypeScript Definitions

First, ensure the remote service is running:

```bash
cd remote_service
docker-compose up
```

Then generate the TypeScript definitions:

```bash
python scripts/generate_typescript_defs.py --output remotemedia-types.d.ts
```

This creates a `remotemedia-types.d.ts` file with all interface definitions.

## Basic Usage

### 1. Install Dependencies

```bash
npm install @grpc/grpc-js @grpc/proto-loader
```

### 2. Import Types

```typescript
import type {
  RemoteExecutorConfig,
  NodeType,
  AudioTransformConfig,
  ExecutionResponse
} from './remotemedia-types';
```

### 3. Create a Client

```typescript
const config: RemoteExecutorConfig = {
  host: 'localhost',
  port: 50052,
  protocol: 'grpc',
  timeout: 30,
  sslEnabled: false
};

const client = new RemoteMediaClient(config);
```

### 4. Execute Nodes

```typescript
// Configure an audio transform node
const audioConfig: AudioTransformConfig = {
  sampleRate: 16000,
  channels: 1,
  dtype: 'float32'
};

// Execute the node
const result = await client.executeNode(
  'AudioTransform',
  audioConfig,
  audioData
);

if (result.status === 'success') {
  console.log('Transform completed:', result.data);
  console.log('Execution time:', result.metrics?.durationMs, 'ms');
}
```

## Available Node Types

The TypeScript definitions include interfaces for all built-in nodes:

### Audio Processing
- `AudioTransform` - Resample and convert audio formats
- `AudioBuffer` - Buffer audio samples
- `VoiceActivityDetector` - Detect speech in audio
- `VADTriggeredBuffer` - Buffer audio triggered by voice activity

### ML Nodes
- `UltravoxNode` - Speech-to-text using Ultravox model
- `KokoroTTSNode` - Text-to-speech synthesis

### Transform Nodes
- `TransformNode` - Apply custom transformations
- `FilterNode` - Filter data based on conditions
- `BatchNode` - Batch items for processing

### Utility Nodes
- `CalculatorNode` - Perform calculations
- `TextProcessorNode` - Process text data
- `CodeExecutorNode` - Execute custom Python code

## Streaming Data

For real-time processing, use the streaming API:

```typescript
const stream = client.streamNode(
  'VoiceActivityDetector',
  { sampleRate: 16000, vadMode: 2 },
  (result) => {
    if (result.isSpeech) {
      console.log('Speech detected!');
    }
  }
);

// Send audio chunks
for (const chunk of audioChunks) {
  await stream.send(chunk);
}

await stream.close();
```

## Custom Node Execution

You can also execute custom Python code remotely:

```typescript
const customCode = `
def process(data):
    # Your custom processing logic
    return data * 2
`;

const result = await client.executeCustomTask(
  customCode,
  'process',
  inputData,
  ['numpy', 'scipy'] // Dependencies to install
);
```

## Type Safety Benefits

The TypeScript definitions provide:

1. **Autocomplete** - IDE suggestions for node configurations
2. **Type Checking** - Catch configuration errors at compile time
3. **Documentation** - Inline documentation for all interfaces
4. **Refactoring** - Safe refactoring with type information

## Example Project Structure

```
my-app/
├── src/
│   ├── audio-processor.ts
│   ├── ml-pipeline.ts
│   └── index.ts
├── remotemedia-types.d.ts
├── package.json
└── tsconfig.json
```

## Advanced Usage

### Custom Serialization

While JSON is the default, you can implement custom serialization:

```typescript
class CustomSerializer {
  serialize(data: any): Buffer {
    // Custom serialization logic
  }
  
  deserialize(buffer: Buffer): any {
    // Custom deserialization logic
  }
}
```

### Error Handling

```typescript
try {
  const result = await client.executeNode(...);
  
  if (result.status === 'error') {
    console.error('Execution failed:', result.error?.message);
    console.error('Traceback:', result.error?.traceback);
  }
} catch (error) {
  // Network or gRPC errors
  console.error('Communication error:', error);
}
```

### Metrics and Monitoring

```typescript
const result = await client.executeNode(...);

if (result.metrics) {
  console.log({
    duration: result.metrics.durationMs,
    memory: result.metrics.memoryPeakMb,
    cpuTime: result.metrics.cpuTimeMs
  });
}
```

## WebRTC Integration

For browser-based applications, combine with WebRTC:

```typescript
// In the browser
const pc = new RTCPeerConnection();
const stream = await navigator.mediaDevices.getUserMedia({ audio: true });

// Send audio to Node.js server
// Node.js server forwards to RemoteMedia service
// Results streamed back via WebRTC data channel
```

## Best Practices

1. **Connection Pooling** - Reuse client connections
2. **Error Recovery** - Implement retry logic for transient failures
3. **Streaming for Large Data** - Use streaming API for real-time or large datasets
4. **Type Guards** - Create type guards for runtime validation
5. **Monitoring** - Track metrics for performance optimization

## Troubleshooting

### Common Issues

1. **Connection Refused**
   - Ensure remote service is running
   - Check firewall settings
   - Verify host and port configuration

2. **Type Errors**
   - Regenerate TypeScript definitions if service updated
   - Ensure TypeScript version compatibility

3. **Serialization Errors**
   - Use JSON format for TypeScript clients
   - Check data types match expected formats

## Next Steps

- Explore the [example client](../examples/nodejs-client/example-client.ts)
- Read about [custom node development](./CUSTOM_NODES.md)
- Learn about [deployment options](./DEPLOYMENT.md)
- Join our [community forum](https://forum.remotemedia.dev)