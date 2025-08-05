/**
 * Practical streaming solution using the current RemoteProxyClient.
 * 
 * This example demonstrates advanced streaming patterns similar to the Python streaming_solution.py
 */

import { withRemoteProxy, batchProcess, retryOperation } from '../src';
import { NodeType } from '../src';

/**
 * Stream data from a remote node using async generators
 */
async function* streamFromRemote<T>(
  nodeProxy: any,
  method: string,
  options: {
    batchSize?: number;
    totalItems?: number;
    args?: any[];
  } = {}
): AsyncGenerator<T, void, unknown> {
  const { batchSize = 10, totalItems = 100, args = [] } = options;
  let processed = 0;

  while (processed < totalItems) {
    const remainingItems = Math.min(batchSize, totalItems - processed);

    try {
      // Simulate batch fetching by calling the method with batch parameters
      const batchResult = await nodeProxy.process({
        method,
        batch_size: remainingItems,
        offset: processed,
        args
      });

      // Handle different response formats
      const items = Array.isArray(batchResult) ? batchResult : batchResult.items || [batchResult];

      for (const item of items) {
        yield item;
        processed++;

        if (processed >= totalItems) {
          break;
        }
      }

      // If we got fewer items than requested, we're done
      if (items.length < remainingItems) {
        break;
      }

    } catch (error) {
      console.error(`Error processing batch at offset ${processed}:`, error);
      break;
    }
  }
}

/**
 * Manual streaming controller for more granular control
 */
class ManualStreamController {
  private nodeProxy: any;
  private streams: Map<string, any> = new Map();

  constructor(nodeProxy: any) {
    this.nodeProxy = nodeProxy;
  }

  async initStream(streamId: string, method: string, ...args: any[]): Promise<{ streamId: string; totalItems?: number }> {
    const streamInfo = {
      streamId,
      method,
      args,
      offset: 0,
      totalItems: 1000, // Could be determined dynamically
    };

    this.streams.set(streamId, streamInfo);

    return {
      streamId,
      totalItems: streamInfo.totalItems
    };
  }

  async nextBatch(streamId: string, batchSize: number = 10): Promise<{ items: any[]; hasMore: boolean }> {
    const streamInfo = this.streams.get(streamId);
    if (!streamInfo) {
      throw new Error(`Stream ${streamId} not found`);
    }

    const remainingItems = streamInfo.totalItems - streamInfo.offset;
    const actualBatchSize = Math.min(batchSize, remainingItems);

    if (actualBatchSize <= 0) {
      return { items: [], hasMore: false };
    }

    try {
      const result = await this.nodeProxy.process({
        method: streamInfo.method,
        batch_size: actualBatchSize,
        offset: streamInfo.offset,
        args: streamInfo.args
      });

      const items = Array.isArray(result) ? result : result.items || [result];
      streamInfo.offset += items.length;

      const hasMore = streamInfo.offset < streamInfo.totalItems && items.length === actualBatchSize;

      return { items, hasMore };
    } catch (error) {
      console.error(`Error fetching batch for stream ${streamId}:`, error);
      return { items: [], hasMore: false };
    }
  }

  async closeStream(streamId: string): Promise<void> {
    this.streams.delete(streamId);
  }
}

/**
 * Streaming data processor that simulates the Python version
 */
class StreamingDataProcessor {
  private chunkData: string[] = [];

  constructor() {
    // Initialize with some mock data
    for (let i = 0; i < 1000; i++) {
      this.chunkData.push(`Chunk data ${i}: ${'x'.repeat(40)}...`);
    }
  }

  async process(request: any): Promise<any> {
    const { method, batch_size = 10, offset = 0, args = [] } = request;

    switch (method) {
      case 'read_large_file':
        return this.readLargeFile(batch_size, offset, args[0]);

      case 'process_data_stream':
        return this.processDataStream(batch_size, offset, args[0] || 20);

      case 'analyze_logs':
        return this.analyzeLogs(batch_size, offset, args[0], args[1] || 1000);

      default:
        throw new Error(`Unknown method: ${method}`);
    }
  }

  private readLargeFile(batchSize: number, offset: number, _filename?: string): string[] {
    const start = offset;
    const end = Math.min(start + batchSize, this.chunkData.length);
    return this.chunkData.slice(start, end);
  }

  private processDataStream(batchSize: number, offset: number, totalCount: number): any[] {
    const items = [];
    for (let i = 0; i < batchSize && (offset + i) < totalCount; i++) {
      items.push({
        id: offset + i,
        value: Math.floor(Math.random() * 50),
        processed_at: new Date().toISOString()
      });
    }
    return items;
  }

  private analyzeLogs(batchSize: number, offset: number, _logFile?: string, totalLines: number = 1000): any[] {
    const logs = [];
    for (let i = 0; i < batchSize && (offset + i) < totalLines; i++) {
      const lineNum = offset + i + 1;
      let severity = "INFO";

      if (lineNum % 50 === 0) severity = "WARNING";
      if (lineNum % 100 === 0) severity = "ERROR";

      logs.push({
        line: lineNum,
        severity,
        message: `Log entry ${lineNum}: ${severity} - Something happened`,
        timestamp: Date.now() + i
      });
    }
    return logs;
  }
}

/**
 * Test the streaming solution
 */
async function testStreamingSolution() {
  await withRemoteProxy({ host: 'localhost', port: 50052 }, async (_client) => {
    // For demo purposes, we'll create a mock processor locally
    // In real usage, this would be a remote node
    const processor = new StreamingDataProcessor();

    console.log('=== Streaming Solution Using Async Generators ===\n');

    // Example 1: Stream file chunks
    console.log('1. Streaming file chunks (10 at a time):');
    const startTime = Date.now();
    let chunksProcessed = 0;

    for await (const chunk of streamFromRemote(processor, 'read_large_file', {
      batchSize: 10,
      totalItems: 30,
      args: ['bigdata.bin']
    })) {
      chunksProcessed++;
      if (chunksProcessed <= 3) {
        console.log(`   Processing: ${(chunk as string).substring(0, 50)}...`);
      } else if (chunksProcessed === 4) {
        console.log('   ... (processing more chunks) ...');
      }

      // Simulate processing each chunk
      await new Promise(resolve => setTimeout(resolve, 10));
    }

    console.log(`   Total chunks processed: ${chunksProcessed}`);
    console.log(`   Time taken: ${(Date.now() - startTime) / 1000}s`);

    // Example 2: Stream data processing results with early termination
    console.log('\n2. Streaming processed data (5 at a time):');
    let itemsProcessed = 0;

    for await (const item of streamFromRemote(processor, 'process_data_stream', {
      batchSize: 5,
      totalItems: 20,
      args: [20]
    })) {
      itemsProcessed++;
      if (itemsProcessed <= 3) {
        console.log(`   Received: ${JSON.stringify(item)}`);
      }

      // Early termination example
      const typedItem = item as { value: number };
      if (typedItem.value > 30) {
        console.log(`   Stopping early at value ${typedItem.value}`);
        break;
      }
    }

    console.log(`   Items processed before stopping: ${itemsProcessed}`);

    console.log('\n✅ Benefits of this approach:');
    console.log('   - Works with async/await and for-await loops');
    console.log('   - Allows processing items in configurable batches');
    console.log('   - Supports early termination with break');
    console.log('   - Memory efficient for large datasets');
    console.log('   - TypeScript-friendly with proper typing');
  });
}

/**
 * Test manual streaming control
 */
async function testManualStreaming() {
  await withRemoteProxy({ host: 'localhost', port: 50052 }, async (_client) => {
    const processor = new StreamingDataProcessor();
    const controller = new ManualStreamController(processor);

    console.log('\n\n=== Manual Streaming Control ===\n');

    // Initialize a stream
    const streamInfo = await controller.initStream('my_stream', 'read_large_file', 'data.txt');
    console.log(`Stream initialized: ${JSON.stringify(streamInfo)}`);

    // Fetch batches manually
    console.log('\nFetching batches manually:');
    let totalItems = 0;

    while (true) {
      const batchResult = await controller.nextBatch('my_stream', 15);
      const items = batchResult.items;

      if (items.length === 0) {
        break;
      }

      totalItems += items.length;
      console.log(`   Got batch of ${items.length} items (total so far: ${totalItems})`);

      // Process the batch - show first 2 of each batch
      for (const item of items.slice(0, 2)) {
        console.log(`     - ${item.substring(0, 40)}...`);
      }

      if (!batchResult.hasMore) {
        break;
      }
    }

    // Clean up
    await controller.closeStream('my_stream');
    console.log(`\nStream closed. Total items processed: ${totalItems}`);
  });
}

/**
 * Demonstrate real-world streaming with actual remote nodes
 */
async function testRealRemoteStreaming() {
  await withRemoteProxy({ host: 'localhost', port: 50052 }, async (client) => {
    console.log('\n\n=== Real Remote Node Streaming ===\n');

    const reviews = [
      "This product is absolutely amazing! Best purchase I've ever made.",
      "Terrible quality, broke after one day. Very disappointed.",
      "It's okay, nothing special but does the job.",
      "Outstanding service and fast delivery. Highly recommend!",
      "Not worth the price. Found better alternatives elsewhere.",
      "Excellent value for money, exceeded my expectations.",
      "Poor customer service, would not recommend.",
      "Perfect for my needs, works as advertised."
    ];

    try {
      // Create a real remote node for text processing
      const textProcessor = await client.createNodeProxy(
        NodeType.TransformersPipelineNode,
        {
          task: 'sentiment-analysis',
          model: 'distilbert-base-uncased-finetuned-sst-2-english'
        }
      );

      console.log('Streaming sentiment analysis results:');

      // Process reviews in batches using batchProcess utility
      const results = await batchProcess(textProcessor, reviews, {
        batchSize: 3,
        parallel: false, // Process sequentially for streaming effect
        onProgress: (completed, total) => {
          console.log(`   Progress: ${completed}/${total} reviews processed`);
        }
      });

      // Display results
      console.log('\nFinal results:');
      results.forEach((result: any, index: number) => {
        const [sentiment] = result as any[];
        const emoji = sentiment.label === 'POSITIVE' ? '😊' : '😞';
        const percentage = (sentiment.score * 100).toFixed(1);
        console.log(`${emoji} ${sentiment.label} (${percentage}%): "${reviews[index]}"`);
      });

    } catch (error) {
      console.log('Remote node not available, using mock data instead');

      // Fallback to mock processing
      const mockResults = await Promise.all(
        reviews.map(async (review: string, _index: number) => {
          await new Promise(resolve => setTimeout(resolve, 100)); // Simulate processing
          const isPositive = review.toLowerCase().includes('amazing') ||
            review.toLowerCase().includes('excellent') ||
            review.toLowerCase().includes('outstanding');
          return [{
            label: isPositive ? 'POSITIVE' : 'NEGATIVE',
            score: isPositive ? 0.9 : 0.1
          }];
        })
      );

      console.log('Mock sentiment analysis results:');
      mockResults.forEach((result: any, index: number) => {
        const [sentiment] = result;
        const emoji = sentiment.label === 'POSITIVE' ? '😊' : '😞';
        const percentage = (sentiment.score * 100).toFixed(1);
        console.log(`${emoji} ${sentiment.label} (${percentage}%): "${reviews[index]}"`);
      });
    }
  });
}

/**
 * Demonstrate a real-world log processing use case
 */
async function demonstrateLogProcessing() {
  console.log('\n\n=== Real-World Use Case: Log Processing ===\n');

  const processor = new StreamingDataProcessor();
  const controller = new ManualStreamController(processor);

  console.log('Processing log file in chunks...');

  // Initialize log processing stream
  await controller.initStream('log_stream', 'analyze_logs', 'system.log', 500);

  const errorLines: number[] = [];
  let lineCount = 0;
  let errorCount = 0;
  let warningCount = 0;

  // Process logs in batches of 50
  while (true) {
    const batch = await controller.nextBatch('log_stream', 50);

    if (batch.items.length === 0) {
      break;
    }

    // Process each log entry
    for (const logEntry of batch.items) {
      lineCount++;

      // Collect errors and warnings
      if (logEntry.severity === 'ERROR') {
        errorLines.push(logEntry.line);
        errorCount++;
        console.log(`   ERROR found at line ${logEntry.line}`);
      } else if (logEntry.severity === 'WARNING') {
        warningCount++;
      }
    }

    // Show progress every 100 lines
    if (lineCount % 100 === 0) {
      console.log(`   Processed ${lineCount} lines...`);
    }

    if (!batch.hasMore) {
      break;
    }
  }

  await controller.closeStream('log_stream');

  console.log(`\nLog analysis complete:`);
  console.log(`   Total lines: ${lineCount}`);
  console.log(`   Errors: ${errorCount}`);
  console.log(`   Warnings: ${warningCount}`);
  console.log(`   Error lines: ${errorLines}`);
}

/**
 * Demonstrate streaming with retry logic
 */
async function demonstrateStreamingWithRetry() {
  console.log('\n\n=== Streaming with Retry Logic ===\n');

  await withRemoteProxy({ host: 'localhost', port: 50052 }, async (_client) => {
    const processor = new StreamingDataProcessor();

    // Simulate unreliable processing with retry
    const processWithRetry = async (data: any) => {
      return await retryOperation(
        async () => {
          // Simulate occasional failures
          if (Math.random() < 0.2) {
            throw new Error('Temporary processing failure');
          }
          return await processor.process(data);
        },
        {
          maxAttempts: 3,
          initialDelay: 100,
          shouldRetry: (error) => error.message.includes('Temporary')
        }
      );
    };

    console.log('Processing with automatic retry on failures:');

    for (let i = 0; i < 10; i++) {
      try {
        const result = await processWithRetry({
          method: 'process_data_stream',
          batch_size: 1,
          offset: i,
          args: [10]
        });

        console.log(`   Batch ${i + 1}: Successfully processed ${result.length} items`);
      } catch (error) {
        console.log(`   Batch ${i + 1}: Failed after retries - ${error}`);
      }
    }
  });
}

/**
 * Main function to run all examples
 */
async function main() {
  try {
    await testStreamingSolution();
    await testManualStreaming();
    await testRealRemoteStreaming();
    await demonstrateLogProcessing();
    await demonstrateStreamingWithRetry();
  } catch (error) {
    console.error('Error running streaming examples:', error);
  }
}

// Export for module usage
export {
  streamFromRemote,
  ManualStreamController,
  StreamingDataProcessor
};

// Run examples if this file is executed directly
if (require.main === module) {
  main();
}