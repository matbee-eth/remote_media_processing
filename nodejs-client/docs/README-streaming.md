# Streaming Solution Example

This example demonstrates advanced streaming patterns for the RemoteMedia Node.js client, similar to the Python `streaming_solution.py`.

## Features

The streaming solution provides:

### 1. Async Generator Streaming
- Process data streams using `for await` loops
- Configurable batch sizes
- Early termination support
- Memory-efficient processing

### 2. Manual Stream Control
- Explicit stream initialization, fetching, and cleanup
- Granular control over batch processing
- Custom stream management

### 3. Real-World Examples
- Log file processing with error detection
- Sentiment analysis with retry logic
- Batch processing with progress tracking

### 4. Error Handling & Resilience
- Automatic retry with exponential backoff
- Graceful error handling
- Stream cleanup on failures

## Usage

### Run the Complete Example
```bash
npm run example:streaming
```

### Use Individual Components

#### Async Generator Streaming
```typescript
import { streamFromRemote } from './streaming-solution';

for await (const item of streamFromRemote(nodeProxy, 'process_data', {
  batchSize: 10,
  totalItems: 100
})) {
  console.log('Processing:', item);
  
  // Early termination
  if (someCondition) break;
}
```

#### Manual Stream Control
```typescript
import { ManualStreamController } from './streaming-solution';

const controller = new ManualStreamController(nodeProxy);

await controller.initStream('my_stream', 'process_data', ...args);

while (true) {
  const batch = await controller.nextBatch('my_stream', 15);
  if (batch.items.length === 0) break;
  
  // Process batch
  for (const item of batch.items) {
    // ... process item
  }
  
  if (!batch.hasMore) break;
}

await controller.closeStream('my_stream');
```

#### Batch Processing with Progress
```typescript
import { batchProcess } from '../src';

const results = await batchProcess(nodeProxy, items, {
  batchSize: 5,
  parallel: false,
  onProgress: (completed, total) => {
    console.log(`Progress: ${completed}/${total}`);
  }
});
```

## Key Differences from Python Version

1. **Async Generators**: Uses TypeScript async generators instead of Python generators
2. **Promise-based**: Leverages Promise.all for parallel processing
3. **Type Safety**: Full TypeScript support with proper typing
4. **Error Handling**: Uses try/catch with async/await patterns
5. **Integration**: Works seamlessly with existing Node.js client utilities

## Examples Included

1. **File Chunk Streaming**: Process large files in manageable chunks
2. **Data Stream Processing**: Handle continuous data streams with early termination
3. **Manual Stream Control**: Explicit control over stream lifecycle
4. **Log Processing**: Real-world log analysis with error detection
5. **Retry Logic**: Automatic retry for unreliable operations
6. **Remote Node Integration**: Works with actual remote nodes when available

## Prerequisites

- Node.js 16+ with TypeScript support
- RemoteMedia server running (for remote node examples)
- All dependencies installed via `npm install`

## Performance Benefits

- **Memory Efficient**: Processes data in configurable batches
- **Early Termination**: Stop processing when conditions are met
- **Parallel Processing**: Leverage Promise.all for concurrent operations
- **Retry Logic**: Handle temporary failures gracefully
- **Progress Tracking**: Monitor long-running operations