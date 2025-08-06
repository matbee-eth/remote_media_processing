# Pipeline Export and JavaScript Integration Developer Guide

This guide explains how to make pipelines exportable and accessible to JavaScript/TypeScript clients via gRPC.

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Python: Creating Exportable Pipelines](#python-creating-exportable-pipelines)
4. [Python: Registering Pipelines](#python-registering-pipelines)
5. [JavaScript: Discovering and Using Pipelines](#javascript-discovering-and-using-pipelines)
6. [Advanced Features](#advanced-features)
7. [Best Practices](#best-practices)
8. [Troubleshooting](#troubleshooting)

## Overview

The Pipeline Export system enables:
- **Cross-language interoperability**: Python pipelines accessible from JavaScript/TypeScript
- **Dynamic discovery**: JavaScript clients can discover registered pipelines at runtime
- **Bidirectional data flow**: Stream data to and from pipelines in real-time
- **Type safety**: Generated TypeScript definitions for pipeline configurations
- **Session management**: Automatic resource cleanup and session tracking

## Architecture

```
┌─────────────────────┐     gRPC      ┌──────────────────────┐
│  JavaScript Client  │◄──────────────►│   Python Server      │
│                     │                │                      │
│  - PipelineClient   │                │  - PipelineRegistry  │
│  - PipelineBuilder  │                │  - gRPC Service      │
│  - Type Definitions │                │  - Pipeline Export   │
└─────────────────────┘                └──────────────────────┘
```

### Key Components

1. **PipelineRegistry** (Python): Central registry for managing pipeline definitions
2. **Pipeline.export_definition()** (Python): Serializes pipeline structure for export
3. **gRPC Service Extensions**: New service methods for pipeline management
4. **PipelineClient** (JavaScript): Client for discovering and executing pipelines
5. **I/O Nodes**: Special nodes for JavaScript data injection/extraction

## Python: Creating Exportable Pipelines

### Basic Pipeline Export

```python
from remotemedia.core import Pipeline
from remotemedia.nodes import PassThroughNode, CalculatorNode

# Create a pipeline with a name (required for export)
pipeline = Pipeline(
    name="math_pipeline",
    nodes=[
        PassThroughNode(),
        CalculatorNode(),
        PassThroughNode()
    ]
)

# Export the pipeline definition
definition = pipeline.export_definition()
print(definition)
# Output: {
#   "name": "math_pipeline",
#   "nodes": [...],
#   "connections": [...],
#   "metadata": {...}
# }
```

### Pipeline with JavaScript I/O Points

```python
from remotemedia.nodes import DataSourceNode, DataSinkNode, JavaScriptBridgeNode

# Create a pipeline with JavaScript integration points
pipeline = Pipeline(
    name="interactive_pipeline",
    nodes=[
        # Receives data from JavaScript
        DataSourceNode(buffer_size=100, name="js_input"),
        
        # Your processing logic
        YourCustomProcessingNode(),
        
        # Bidirectional JavaScript communication
        JavaScriptBridgeNode(name="js_bridge"),
        
        # More processing
        AnotherProcessingNode(),
        
        # Send results back to JavaScript
        DataSinkNode(result_key="output", name="js_output")
    ]
)
```

### Making Nodes Export-Compatible

Ensure your custom nodes have proper metadata:

```python
class MyCustomNode(Node):
    def __init__(self, param1: str, param2: int = 10):
        super().__init__()
        self.param1 = param1
        self.param2 = param2
        
        # Optional: Add export metadata
        self.export_config = {
            "node_type": "MyCustomNode",
            "parameters": {
                "param1": param1,
                "param2": param2
            },
            "capabilities": ["streaming", "batch"],
            "description": "Custom processing node"
        }
    
    async def process(self, data):
        # Your processing logic
        return processed_data
```

## Python: Registering Pipelines

### Using PipelineRegistry

```python
from remotemedia.core.pipeline_registry import PipelineRegistry

# Get the global registry instance
registry = PipelineRegistry.get_instance()

# Register a pipeline
pipeline_id = await registry.register_pipeline(
    name="my_pipeline",
    pipeline=pipeline,
    metadata={
        "description": "Audio processing pipeline",
        "version": "1.0.0",
        "author": "Your Name",
        "tags": ["audio", "realtime"],
        "requirements": ["numpy", "scipy"]
    }
)

print(f"Registered pipeline: {pipeline_id}")
```

### Auto-Registration on Server Start

```python
# In your server initialization (e.g., webrtc_pipeline_server.py)
from remotemedia.core.pipeline_registry import PipelineRegistry
from your_pipelines import create_webrtc_pipeline

async def initialize_server():
    registry = PipelineRegistry.get_instance()
    
    # Create and register your pipeline
    pipeline = create_webrtc_pipeline()
    await registry.register_pipeline(
        name="webrtc_pipeline",
        pipeline=pipeline,
        metadata={
            "description": "WebRTC audio/video processing",
            "auto_start": True
        }
    )
    
    # Start the gRPC server
    await start_grpc_server()
```

### Dynamic Pipeline Registration

```python
# Register pipelines based on configuration
import yaml

async def register_pipelines_from_config(config_file):
    with open(config_file) as f:
        config = yaml.safe_load(f)
    
    registry = PipelineRegistry.get_instance()
    
    for pipeline_config in config['pipelines']:
        pipeline = build_pipeline_from_config(pipeline_config)
        await registry.register_pipeline(
            name=pipeline_config['name'],
            pipeline=pipeline,
            metadata=pipeline_config.get('metadata', {})
        )
```

## JavaScript: Discovering and Using Pipelines

### Basic Pipeline Discovery

```javascript
import { PipelineClient } from '@remote_media_processing/nodejs-client';

const client = new PipelineClient({
  host: 'localhost',
  port: 50052
});

// List all available pipelines
const pipelines = await client.listPipelines();
pipelines.forEach(p => {
  console.log(`${p.name}: ${p.description}`);
  console.log(`  Nodes: ${p.node_count}`);
  console.log(`  Tags: ${p.tags.join(', ')}`);
});

// Get detailed info about a specific pipeline
const info = await client.getPipelineInfo('webrtc_pipeline');
console.log('Pipeline structure:', info.definition);
console.log('Input nodes:', info.definition.nodes.filter(n => n.is_source));
console.log('Output nodes:', info.definition.nodes.filter(n => n.is_sink));
```

### Executing Pipelines

```javascript
// Simple execution with input/output
const result = await client.executePipeline('math_pipeline', {
  operation: 'multiply',
  args: [10, 5]
});
console.log('Result:', result); // { result: 50 }

// Execution with options
const result = await client.executePipeline('audio_pipeline', audioData, {
  timeout: 30000,  // 30 seconds
  config: {
    sample_rate: 16000,
    channels: 1
  }
});
```

### Streaming Data Through Pipelines

```javascript
// Create a streaming connection
const stream = client.streamPipeline('realtime_pipeline', {
  bidirectional: true,
  bufferSize: 10
});

// Handle events
stream.on('data', (chunk) => {
  console.log('Received:', chunk);
  processChunk(chunk);
});

stream.on('error', (error) => {
  console.error('Stream error:', error);
  handleError(error);
});

stream.on('end', () => {
  console.log('Pipeline stream ended');
  cleanup();
});

// Send data
for await (const chunk of dataSource) {
  await stream.send(chunk);
  
  // Optionally wait for backpressure
  if (stream.bufferFull) {
    await stream.drain();
  }
}

// Close the stream
await stream.end();
```

### Creating Pipelines from JavaScript

```javascript
import { PipelineBuilder } from '@remote_media_processing/nodejs-client';

// Build a pipeline definition
const builder = new PipelineBuilder('custom_pipeline');

builder
  // Add nodes with configuration
  .addNode('DataSourceNode', {
    buffer_size: 100,
    timeout_seconds: 30
  })
  .addNode('AudioTransform', {
    output_sample_rate: 16000,
    output_channels: 1
  })
  .addNode('CustomProcessorNode', {
    mode: 'fast',
    quality: 'high'
  })
  .addNode('DataSinkNode', {
    result_key: 'processed_audio'
  })
  
  // Define connections (by index)
  .connect(0, 1)  // source -> audio transform
  .connect(1, 2)  // audio transform -> processor
  .connect(2, 3)  // processor -> sink
  
  // Add metadata
  .setMetadata({
    description: 'Custom audio processing pipeline',
    author: 'JavaScript Client',
    version: '1.0.0',
    tags: ['audio', 'custom']
  });

// Register the pipeline on the server
const pipelineId = await client.registerPipeline(
  'custom_pipeline',
  builder.build()
);

// Execute the newly registered pipeline
const result = await client.executePipeline(pipelineId, audioData);
```

## Advanced Features

### Pipeline Composition

```javascript
// Compose pipelines from existing ones
const composed = new PipelineBuilder('composed_pipeline');

// Add an entire pipeline as a sub-pipeline
composed.addPipeline('preprocessing_pipeline')
        .addPipeline('main_processing_pipeline')
        .addPipeline('postprocessing_pipeline')
        .connectPipelines(0, 1)
        .connectPipelines(1, 2);

await client.registerPipeline('composed_pipeline', composed.build());
```

### Conditional Execution

```javascript
// Execute different pipelines based on input
async function processData(data) {
  const pipelines = await client.listPipelines();
  
  // Select pipeline based on data type
  let pipelineId;
  if (data.type === 'audio') {
    pipelineId = pipelines.find(p => p.tags.includes('audio'))?.id;
  } else if (data.type === 'video') {
    pipelineId = pipelines.find(p => p.tags.includes('video'))?.id;
  }
  
  if (!pipelineId) {
    throw new Error(`No pipeline for type: ${data.type}`);
  }
  
  return await client.executePipeline(pipelineId, data);
}
```

### Session Management

```javascript
// Create a persistent session for multiple executions
const session = await client.createSession('webrtc_pipeline');

try {
  // Execute multiple times with the same session
  for (const chunk of audioChunks) {
    const result = await session.execute(chunk);
    results.push(result);
  }
} finally {
  // Always clean up the session
  await session.close();
}
```

### Pipeline Metrics and Monitoring

```javascript
// Monitor pipeline performance
const metrics = await client.getPipelineMetrics('my_pipeline');

console.log('Pipeline Metrics:');
console.log(`  Total Executions: ${metrics.execution_count}`);
console.log(`  Success Rate: ${metrics.success_rate}%`);
console.log(`  Average Latency: ${metrics.average_latency_ms}ms`);
console.log(`  P95 Latency: ${metrics.p95_latency_ms}ms`);
console.log(`  Active Sessions: ${metrics.active_sessions}`);

// Get detailed execution history
const history = await client.getExecutionHistory('my_pipeline', {
  limit: 10,
  includeErrors: true
});

history.forEach(exec => {
  console.log(`${exec.timestamp}: ${exec.status} (${exec.duration_ms}ms)`);
  if (exec.error) {
    console.log(`  Error: ${exec.error}`);
  }
});
```

## Best Practices

### 1. Pipeline Naming

Use descriptive, hierarchical names:
```python
# Good
"audio.processing.noise_reduction"
"video.effects.blur"
"ml.inference.sentiment_analysis"

# Bad
"pipeline1"
"test"
"my_pipeline"
```

### 2. Metadata Standards

Always include comprehensive metadata:
```python
metadata = {
    "description": "Clear description of what the pipeline does",
    "version": "1.0.0",  # Semantic versioning
    "author": "team-name or email",
    "tags": ["domain", "type", "performance"],
    "requirements": ["package==version"],
    "input_schema": {...},  # JSON schema for validation
    "output_schema": {...},
    "performance": {
        "latency_ms": 100,
        "throughput_rps": 1000
    }
}
```

### 3. Error Handling

```javascript
// Always handle pipeline errors gracefully
try {
  const result = await client.executePipeline(pipelineId, data);
  return result;
} catch (error) {
  if (error.code === 'PIPELINE_NOT_FOUND') {
    console.error(`Pipeline ${pipelineId} not found`);
    // Try fallback pipeline
  } else if (error.code === 'EXECUTION_TIMEOUT') {
    console.error('Pipeline execution timed out');
    // Retry with longer timeout
  } else {
    console.error('Unexpected error:', error);
    throw error;
  }
}
```

### 4. Resource Management

```javascript
// Use try-finally for cleanup
const stream = client.streamPipeline('pipeline_id');
try {
  await processWithStream(stream);
} finally {
  await stream.close();
}

// Or use helper functions
await client.withStream('pipeline_id', async (stream) => {
  // Stream is automatically closed after this block
  await processWithStream(stream);
});
```

### 5. Type Safety

Generate and use TypeScript definitions:
```typescript
import { PipelineDefinition, NodeConfig } from './generated-types';

// Type-safe pipeline building
const definition: PipelineDefinition = {
  name: 'typed_pipeline',
  nodes: [
    { type: 'DataSourceNode', config: { buffer_size: 100 } as NodeConfig },
    // TypeScript ensures correct configuration
  ],
  connections: [{ from: 0, to: 1 }]
};
```

## Troubleshooting

### Common Issues

#### Pipeline Not Found
```javascript
// Check if pipeline is registered
const pipelines = await client.listPipelines();
const exists = pipelines.some(p => p.name === 'my_pipeline');
if (!exists) {
  console.error('Pipeline not registered');
}
```

#### Connection Errors
```javascript
// Implement retry logic
async function connectWithRetry(options, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return new PipelineClient(options);
    } catch (error) {
      console.log(`Connection attempt ${i + 1} failed`);
      await new Promise(r => setTimeout(r, 1000 * Math.pow(2, i)));
    }
  }
  throw new Error('Failed to connect after retries');
}
```

#### Stream Backpressure
```javascript
// Handle backpressure properly
const stream = client.streamPipeline('pipeline_id');

stream.on('drain', () => {
  console.log('Stream drained, resume sending');
  resumeSending();
});

async function sendData(data) {
  const canContinue = stream.send(data);
  if (!canContinue) {
    console.log('Backpressure detected, pausing');
    await new Promise(resolve => stream.once('drain', resolve));
  }
}
```

### Debugging

Enable debug logging:
```javascript
// Set debug environment variable
process.env.DEBUG = 'pipeline:*';

// Or use client debug mode
const client = new PipelineClient({
  host: 'localhost',
  port: 50052,
  debug: true
});

// Log all pipeline events
client.on('debug', (msg) => console.log('[DEBUG]', msg));
```

### Performance Optimization

```javascript
// Batch operations for better performance
const batchSize = 100;
const results = [];

for (let i = 0; i < data.length; i += batchSize) {
  const batch = data.slice(i, i + batchSize);
  const batchResults = await Promise.all(
    batch.map(item => client.executePipeline('pipeline_id', item))
  );
  results.push(...batchResults);
}

// Use streaming for large datasets
const stream = client.streamPipeline('pipeline_id', {
  highWaterMark: 1000,  // Buffer size
  concurrency: 10       // Parallel processing
});
```

## API Reference

### Python API

```python
# Pipeline export
definition = pipeline.export_definition()

# Pipeline registry
registry = PipelineRegistry.get_instance()
pipeline_id = await registry.register_pipeline(name, pipeline, metadata)
await registry.unregister_pipeline(pipeline_id)
pipelines = await registry.list_pipelines(filter_dict)
info = await registry.get_pipeline_info(pipeline_id)
result = await registry.execute_pipeline(pipeline_id, input_data, config)
```

### JavaScript API

```javascript
// Pipeline client
const client = new PipelineClient(options);
const pipelines = await client.listPipelines(filter);
const info = await client.getPipelineInfo(pipelineId);
const result = await client.executePipeline(pipelineId, data, options);
const stream = client.streamPipeline(pipelineId, options);
await client.registerPipeline(name, definition, options);
await client.unregisterPipeline(pipelineId);

// Pipeline builder
const builder = new PipelineBuilder(name);
builder.addNode(type, config);
builder.connect(fromIndex, toIndex);
builder.setMetadata(metadata);
const definition = builder.build();
```

## Conclusion

The Pipeline Export system provides a powerful way to make Python processing pipelines accessible from JavaScript/TypeScript clients. By following this guide, you can:

1. Create exportable pipelines in Python
2. Register them with the PipelineRegistry
3. Discover and execute them from JavaScript
4. Stream data bidirectionally
5. Monitor performance and handle errors

For more examples, see the `examples/` directory in both the Python and Node.js client packages.