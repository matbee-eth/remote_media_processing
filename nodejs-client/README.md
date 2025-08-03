# RemoteMedia Node.js Client

Official Node.js/TypeScript client for the RemoteMedia Processing SDK. Execute any registered server node remotely with a simple, intuitive API.

## Features

- 🚀 **Simple API** - Execute remote nodes with just a few lines of code
- 🔄 **Streaming Support** - Real-time bidirectional streaming for compatible nodes
- 🎯 **Type Safety** - Full TypeScript support with comprehensive type definitions
- 🔁 **Automatic Retry** - Built-in retry logic with exponential backoff
- 🐍 **Python-like API** - Familiar patterns for Python developers
- 📦 **Zero Configuration** - Works out of the box with sensible defaults

## Installation

```bash
npm install @remotemedia/nodejs-client
```

Or with yarn:

```bash
yarn add @remotemedia/nodejs-client
```

## Quick Start

```typescript
import { RemoteProxyClient } from '@remotemedia/nodejs-client';

// Connect to the server
const client = new RemoteProxyClient({
  host: 'localhost',
  port: 50052
});

// Create a node proxy
const sentimentAnalyzer = await client.createNodeProxy(
  'TransformersPipelineNode',
  {
    task: 'sentiment-analysis',
    model: 'distilbert-base-uncased-finetuned-sst-2-english'
  }
);

// Process data
const result = await sentimentAnalyzer.process("I love this library!");
console.log(result);
// Output: [{ label: 'POSITIVE', score: 0.9998 }]
```

## Usage Patterns

### Python-style Context Manager

Use the `withRemoteProxy` helper for automatic connection management:

```typescript
import { withRemoteProxy } from '@remotemedia/nodejs-client';

await withRemoteProxy({ host: 'localhost', port: 50052 }, async (client) => {
  const node = await client.createNodeProxy('CalculatorNode');
  const result = await node.process({
    operation: 'multiply',
    args: [21, 2]
  });
  console.log(result); // { result: 42 }
});
```

### Using Helper Classes

The `RemoteNodes` class provides convenient methods for common node types:

```typescript
import { withRemoteProxy, RemoteNodes } from '@remotemedia/nodejs-client';

await withRemoteProxy({ host: 'localhost', port: 50052 }, async (client) => {
  const nodes = new RemoteNodes(client);
  
  // Text generation
  const textGen = await nodes.transformersPipeline({
    task: 'text-generation',
    model: 'gpt2',
    model_kwargs: {
      max_length: 50,
      temperature: 0.8
    }
  });
  
  const generated = await textGen.process("The future of AI is");
  console.log(generated);
});
```

### Pipeline Processing

Chain multiple nodes together:

```typescript
import { withRemoteProxy, NodePipeline } from '@remotemedia/nodejs-client';

await withRemoteProxy({ host: 'localhost', port: 50052 }, async (client) => {
  // Create nodes
  const textProcessor = await client.createNodeProxy('TextProcessorNode');
  const sentimentAnalyzer = await client.createNodeProxy(
    'TransformersPipelineNode',
    { task: 'sentiment-analysis' }
  );
  
  // Build pipeline
  const pipeline = new NodePipeline()
    .add(textProcessor)
    .add(sentimentAnalyzer);
  
  // Process through pipeline
  const result = await pipeline.process({
    text: "this is amazing",
    operations: ["uppercase"]
  });
});
```

### Batch Processing

Process multiple items efficiently:

```typescript
import { batchProcess } from '@remotemedia/nodejs-client';

const items = [
  "I love this!",
  "This is terrible.",
  "It's okay."
];

const results = await batchProcess(sentimentAnalyzer, items, {
  batchSize: 10,
  parallel: true,
  onProgress: (completed, total) => {
    console.log(`Progress: ${completed}/${total}`);
  }
});
```

### Streaming

For nodes that support streaming:

```typescript
const audioProcessor = await client.createNodeProxy('AudioTransform');

const stream = audioProcessor.processStream(
  (data) => {
    console.log('Received:', data);
  },
  (error) => {
    console.error('Stream error:', error);
  }
);

// Send data
await stream.send({ samples: audioData });
await stream.close();
```

### Error Handling and Retry

Use the built-in retry helper:

```typescript
import { retryOperation } from '@remotemedia/nodejs-client';

const result = await retryOperation(
  async () => {
    const node = await client.createNodeProxy('TransformersPipelineNode', config);
    return await node.process(data);
  },
  {
    maxAttempts: 3,
    initialDelay: 1000,
    shouldRetry: (error) => error.code === 'UNAVAILABLE'
  }
);
```

## Available Node Types

### NLP/ML Nodes

- **TransformersPipelineNode** - Hugging Face transformers pipelines
  - Tasks: sentiment-analysis, text-generation, question-answering, etc.
  - Supports model_kwargs for fine-tuning generation

### Audio Processing

- **AudioTransform** - Resample and convert audio formats
- **AudioBuffer** - Buffer audio data for batch processing

### Text Processing

- **TextProcessorNode** - Basic text operations (uppercase, word count, etc.)

### Utility Nodes

- **CalculatorNode** - Mathematical operations
- **FormatConverter** - Convert between data formats

### Advanced Nodes

- **CodeExecutorNode** - Execute Python code (⚠️ Security risk!)
- **SerializedClassExecutorNode** - Execute serialized Python objects

## Configuration Options

```typescript
const client = new RemoteProxyClient({
  host: 'localhost',
  port: 50052,
  
  // Optional settings
  timeout: 30,                    // Request timeout in seconds
  sslEnabled: false,              // Enable SSL/TLS
  maxMessageSize: 4 * 1024 * 1024, // Max message size (4MB)
  
  // Retry configuration
  retry: {
    maxAttempts: 3,
    initialBackoff: 1000,
    maxBackoff: 5000,
    backoffMultiplier: 1.5
  }
});
```

## Server Discovery

List available nodes and check server status:

```typescript
// List all available nodes
const nodes = await client.listNodes();
nodes.forEach(node => {
  console.log(`${node.node_type}: ${node.description}`);
});

// Get server status
const status = await client.getStatus();
console.log(`Server version: ${status.version}`);
console.log(`Uptime: ${status.uptime_seconds}s`);
```

## TypeScript Support

Full TypeScript support with comprehensive type definitions:

```typescript
import { 
  RemoteProxyClient,
  RemoteExecutorConfig,
  NodeConfig,
  ExecutionOptions,
  RemoteNodeProxy,
  ServerStatus
} from '@remotemedia/nodejs-client';
```

## Examples

See the [examples](./examples) directory for complete working examples:

- `basic-usage.ts` - Simple getting started example
- `sentiment-analysis.ts` - NLP sentiment analysis
- `pipeline-processing.ts` - Multi-step pipeline processing
- `streaming-audio.ts` - Real-time audio streaming
- `batch-processing.ts` - Efficient batch operations

## Error Handling

The client provides detailed error information:

```typescript
try {
  const result = await node.process(data);
} catch (error) {
  if (error.code === 'DEADLINE_EXCEEDED') {
    console.error('Request timed out');
  } else if (error.code === 'UNAVAILABLE') {
    console.error('Server is unavailable');
  } else {
    console.error('Error:', error.message);
  }
}
```

## Development

```bash
# Install dependencies
npm install

# Build the project
npm run build

# Run tests
npm test

# Run examples
npm run example:sentiment
```

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details.

## License

MIT License - see [LICENSE](LICENSE) for details.

## Support

- 📚 [Documentation](https://docs.remotemedia.io)
- 💬 [Discord Community](https://discord.gg/remotemedia)
- 🐛 [Issue Tracker](https://github.com/remotemedia/nodejs-client/issues)
- 📧 [Email Support](mailto:support@remotemedia.io)