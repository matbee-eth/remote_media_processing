# RemoteProxyClient for Node.js/TypeScript

The RemoteProxyClient provides a Python-like API for transparent remote object execution from Node.js/TypeScript applications.

## Overview

The RemoteProxyClient allows you to:
- Execute Python objects remotely with a simple, intuitive API
- Use async/await patterns similar to the Python RemoteProxyClient
- Maintain object state across multiple method calls
- Handle errors gracefully with proper type safety

## Installation

```bash
npm install @grpc/grpc-js @grpc/proto-loader
```

## Basic Usage

### Direct Usage Pattern

```typescript
import { RemoteProxyClient, RemoteExecutorConfig } from './src/remote-proxy-client';

// Configure connection
const config: RemoteExecutorConfig = {
  host: "localhost",
  port: 50052
};

// Create client
const client = new RemoteProxyClient(config);
await client.connect();

// Create a local object
const calculator = new Calculator();

// Create remote proxy with Python implementation
const remoteCalc = await client.createProxy(calculator, pythonCode);

// Use it exactly like a local object (just add await)
const result = await remoteCalc.add(5, 3);
console.log(result); // 8

// Clean up
await client.close();
```

### Async With Pattern (Python-style)

```typescript
import { withRemoteProxy } from './src/remote-proxy-client';

// Use the Python-style "async with" pattern
await withRemoteProxy(config, async (client) => {
  const calculator = new Calculator();
  const remoteCalc = await client.createProxy(calculator, pythonCode);
  
  // Use it exactly like a local object (just add await)
  const result = await remoteCalc.add(5, 3);
  console.log(result); // 8
});
// Client automatically closed here
```

## API Reference

### RemoteProxyClient

```typescript
class RemoteProxyClient {
  constructor(config: RemoteExecutorConfig)
  async connect(): Promise<void>
  async createProxy<T>(obj: T, pythonCode: string, dependencies?: string[]): Promise<RemoteProxy<T>>
  async close(): Promise<void>
}
```

### RemoteExecutorConfig

```typescript
interface RemoteExecutorConfig {
  host: string;              // Remote service host
  port: number;              // Remote service port
  protocol?: 'grpc' | 'http'; // Communication protocol (default: 'grpc')
  timeout?: number;          // Request timeout in seconds
  sslEnabled?: boolean;      // Use SSL/TLS
  pipPackages?: string[];    // Python packages to install
}
```

### RemoteProxy Type

```typescript
type RemoteProxy<T> = {
  [K in keyof T]: T[K] extends (...args: any[]) => any
    ? (...args: Parameters<T[K]>) => Promise<Awaited<ReturnType<T[K]>>>
    : never;
};
```

## Examples

### Calculator Example

```typescript
// Define interface
interface Calculator {
  add(a: number, b: number): number;
  multiply(a: number, b: number): number;
}

// Python implementation
const pythonCode = `
class Calculator:
    def add(self, a, b):
        return a + b
    
    def multiply(self, a, b):
        return a * b
`;

// Use remotely
await withRemoteProxy(config, async (client) => {
  const calc = new Calculator();
  const remote = await client.createProxy(calc, pythonCode);
  
  const sum = await remote.add(5, 3);      // 8
  const product = await remote.multiply(4, 7); // 28
});
```

### Stateful Object Example

```typescript
const counterCode = `
class Counter:
    def __init__(self):
        self.count = 0
    
    def increment(self):
        self.count += 1
        return self.count
    
    def get_count(self):
        return self.count
`;

await withRemoteProxy(config, async (client) => {
  const counter = new Counter();
  const remote = await client.createProxy(counter, counterCode);
  
  await remote.increment(); // 1
  await remote.increment(); // 2
  const count = await remote.get_count(); // 2
});
```

### Using Python Dependencies

```typescript
const dataProcessorCode = `
import numpy as np
import pandas as pd

class DataProcessor:
    def analyze(self, data):
        df = pd.DataFrame(data)
        return {
            'mean': df.mean().to_dict(),
            'std': df.std().to_dict(),
            'correlation': df.corr().to_dict()
        }
`;

await withRemoteProxy(config, async (client) => {
  const processor = new DataProcessor();
  const remote = await client.createProxy(
    processor, 
    dataProcessorCode,
    ['numpy', 'pandas'] // Dependencies
  );
  
  const result = await remote.analyze({
    x: [1, 2, 3, 4, 5],
    y: [2, 4, 5, 4, 5]
  });
});
```

## How It Works

1. **Object Definition**: Define a TypeScript interface and class for type safety
2. **Python Implementation**: Provide Python code that implements the same interface
3. **Proxy Creation**: The client creates a proxy that intercepts method calls
4. **Remote Execution**: Method calls are serialized and sent to the remote Python service
5. **Result Handling**: Results are deserialized and returned as promises

## Best Practices

1. **Type Safety**: Always define TypeScript interfaces for your objects
2. **Error Handling**: Wrap remote calls in try-catch blocks
3. **Connection Management**: Use the `withRemoteProxy` helper for automatic cleanup
4. **Python Code**: Ensure Python code matches the TypeScript interface
5. **Dependencies**: List all required Python packages in the dependencies array

## Differences from Python API

While the API is designed to be similar to Python's RemoteProxyClient, there are some differences:

1. **Async/Await**: All remote methods return Promises and must use `await`
2. **Type Definitions**: TypeScript requires explicit interface definitions
3. **Python Code**: You must provide the Python implementation as a string
4. **No Direct Serialization**: Objects are not directly serialized like in Python

## Troubleshooting

### Connection Issues
- Ensure the remote service is running
- Check firewall settings
- Verify host and port configuration

### Method Not Found
- Ensure Python method names match TypeScript interface
- Check for typos in method names
- Verify the Python code syntax

### Import Errors
- List all required packages in the dependencies array
- Ensure packages are available on PyPI
- Check for version compatibility

## Advanced Usage

### Custom Serialization

For complex data types, you may need custom serialization:

```typescript
const complexCode = `
import json
import base64

class ImageProcessor:
    def process_image(self, image_data):
        # Decode base64 image
        import PIL.Image
        import io
        
        image_bytes = base64.b64decode(image_data)
        image = PIL.Image.open(io.BytesIO(image_bytes))
        
        # Process image...
        
        # Return as base64
        output = io.BytesIO()
        image.save(output, format='PNG')
        return base64.b64encode(output.getvalue()).decode()
`;
```

### Streaming Results

For large results, consider implementing streaming:

```typescript
const streamingCode = `
class DataStreamer:
    def process_chunks(self, data):
        # Process in chunks
        chunk_size = 1000
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i+chunk_size]
            # Process chunk
            yield processed_chunk
`;
```

## Future Enhancements

- Direct object serialization without Python code strings
- Automatic Python code generation from TypeScript
- Bi-directional streaming support
- WebSocket transport option

## See Also

- [RemoteMedia Python Documentation](../../../README.md)
- [TypeScript Usage Guide](../../../docs/TYPESCRIPT_USAGE.md)
- [Example Code](../examples/proxy/)