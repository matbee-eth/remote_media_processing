/**
 * Demonstration of using generated TypeScript types with streaming support
 * 
 * This example shows how to use the auto-generated concrete classes
 * with full streaming and generator capabilities.
 */

import { createRemoteProxy, RemoteExecutor } from '../generated-types/proxy-factory';
import { RemoteExecutorConfig } from '../generated-types/base';

// Example of a custom node with streaming capabilities
class DataStreamProcessor {
  constructor(
    public bufferSize: number = 1000,
    public processingDelay: number = 10
  ) {}

  /**
   * Stream data chunks with processing
   */
  async *processDataStream(
    dataSource: string, 
    limit: number = 100
  ): AsyncGenerator<ProcessedData> {
    console.log(`Starting to process stream from ${dataSource}`);
    
    for (let i = 0; i < limit; i++) {
      // Simulate processing delay
      await new Promise(resolve => setTimeout(resolve, this.processingDelay));
      
      yield {
        id: i,
        source: dataSource,
        processedAt: new Date().toISOString(),
        value: Math.random() * 100,
        metadata: {
          bufferSize: this.bufferSize,
          sequenceNumber: i
        }
      };
      
      // Log progress every 10 items
      if ((i + 1) % 10 === 0) {
        console.log(`Processed ${i + 1}/${limit} items`);
      }
    }
    
    console.log(`Stream processing complete for ${dataSource}`);
  }

  /**
   * Batch process data with windowing
   */
  async *batchProcess(
    items: AsyncIterable<any>, 
    batchSize: number = 10
  ): AsyncGenerator<BatchResult> {
    let batch: any[] = [];
    let batchNumber = 0;
    
    for await (const item of items) {
      batch.push(item);
      
      if (batch.length >= batchSize) {
        yield {
          batchNumber: batchNumber++,
          items: batch,
          processedCount: batch.length,
          timestamp: Date.now()
        };
        batch = [];
      }
    }
    
    // Yield remaining items
    if (batch.length > 0) {
      yield {
        batchNumber: batchNumber++,
        items: batch,
        processedCount: batch.length,
        timestamp: Date.now()
      };
    }
  }

  /**
   * Transform stream with filtering
   */
  async *filterTransform(
    input: AsyncIterable<ProcessedData>,
    minValue: number = 50
  ): AsyncGenerator<ProcessedData> {
    let filtered = 0;
    let passed = 0;
    
    for await (const item of input) {
      if (item.value >= minValue) {
        passed++;
        yield item;
      } else {
        filtered++;
      }
    }
    
    console.log(`Filter complete: ${passed} passed, ${filtered} filtered out`);
  }

  /**
   * Aggregate streaming data
   */
  async aggregateStream(
    input: AsyncIterable<ProcessedData>
  ): Promise<AggregationResult> {
    let count = 0;
    let sum = 0;
    let min = Infinity;
    let max = -Infinity;
    
    for await (const item of input) {
      count++;
      sum += item.value;
      min = Math.min(min, item.value);
      max = Math.max(max, item.value);
    }
    
    return {
      count,
      sum,
      average: count > 0 ? sum / count : 0,
      min: min === Infinity ? 0 : min,
      max: max === -Infinity ? 0 : max
    };
  }
}

// Type definitions for our data
interface ProcessedData {
  id: number;
  source: string;
  processedAt: string;
  value: number;
  metadata: {
    bufferSize: number;
    sequenceNumber: number;
  };
}

interface BatchResult {
  batchNumber: number;
  items: any[];
  processedCount: number;
  timestamp: number;
}

interface AggregationResult {
  count: number;
  sum: number;
  average: number;
  min: number;
  max: number;
}

/**
 * Demonstrate streaming with generated types
 */
async function demonstrateStreamingWithTypes() {
  console.log('=== GENERATED TYPES WITH STREAMING DEMO ===\n');
  
  const config: RemoteExecutorConfig = {
    host: 'localhost',
    port: 50052,
    batchSize: 5,
    pipPackages: ['numpy', 'pandas'] // Example dependencies
  };

  try {
    // Create remote proxy using generated factory
    const remoteProcessor = await createRemoteProxy(
      DataStreamProcessor,
      config,
      1000, // bufferSize
      10    // processingDelay
    );

    // Test 1: Basic streaming
    console.log('1. Basic streaming with remote execution:');
    const dataStream = await remoteProcessor.processDataStream('sensor_data', 30);
    
    let receivedCount = 0;
    for await (const data of dataStream) {
      receivedCount++;
      if (receivedCount <= 5) {
        console.log(`   Received: ${JSON.stringify(data)}`);
      }
    }
    console.log(`   Total received: ${receivedCount} items\n`);

    // Test 2: Chained streaming operations
    console.log('2. Chained streaming operations:');
    
    // Create a new stream
    const sourceStream = await remoteProcessor.processDataStream('analytics', 50);
    
    // Filter the stream
    const filteredStream = await remoteProcessor.filterTransform(sourceStream, 70);
    
    // Batch the filtered results
    const batchedStream = await remoteProcessor.batchProcess(filteredStream, 5);
    
    console.log('   Processing pipeline: source -> filter(>70) -> batch(5)');
    
    let batchCount = 0;
    for await (const batch of batchedStream) {
      batchCount++;
      console.log(`   Batch ${batch.batchNumber}: ${batch.processedCount} items`);
    }
    console.log(`   Total batches: ${batchCount}\n`);

    // Test 3: Stream aggregation
    console.log('3. Stream aggregation:');
    const aggregationStream = await remoteProcessor.processDataStream('metrics', 100);
    const result = await remoteProcessor.aggregateStream(aggregationStream);
    
    console.log('   Aggregation results:');
    console.log(`   - Count: ${result.count}`);
    console.log(`   - Average: ${result.average.toFixed(2)}`);
    console.log(`   - Min: ${result.min.toFixed(2)}`);
    console.log(`   - Max: ${result.max.toFixed(2)}`);
    console.log(`   - Sum: ${result.sum.toFixed(2)}\n`);

    // Test 4: Early termination and resource cleanup
    console.log('4. Early termination test:');
    const largeStream = await remoteProcessor.processDataStream('large_dataset', 1000);
    
    let earlyCount = 0;
    for await (const item of largeStream) {
      earlyCount++;
      if (earlyCount >= 10) {
        console.log('   Terminating early at 10 items');
        // The generator proxy will automatically clean up
        break;
      }
    }
    console.log(`   Processed only ${earlyCount} items (rest not generated)\n`);

    // Test 5: Error handling in streams
    console.log('5. Error handling in streaming:');
    try {
      // This would fail if the remote processor throws an error
      const errorStream = await remoteProcessor.processDataStream('error_source', 10);
      
      for await (const item of errorStream) {
        console.log(`   Processing: ${item.id}`);
        // Simulate error condition
        if (item.id === 5) {
          throw new Error('Simulated processing error');
        }
      }
    } catch (error) {
      console.log(`   Caught expected error: ${error.message}\n`);
    }

  } catch (error) {
    console.error('Error in demo:', error);
  }
}

/**
 * Demonstrate integration with Node.js streams
 */
async function demonstrateNodeJsIntegration() {
  console.log('=== NODE.JS STREAM INTEGRATION ===\n');
  
  const { Transform, Readable } = await import('stream');
  const { pipeline } = await import('stream/promises');
  
  // Create a custom transform stream
  const uppercaseTransform = new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      if (typeof chunk === 'string') {
        callback(null, chunk.toUpperCase());
      } else if (chunk && chunk.source) {
        callback(null, { ...chunk, source: chunk.source.toUpperCase() });
      } else {
        callback(null, chunk);
      }
    }
  });

  // Create a Node.js readable from async generator
  async function* textGenerator() {
    const messages = ['hello', 'world', 'from', 'node.js', 'streams'];
    for (const msg of messages) {
      yield msg;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }

  const readableStream = Readable.from(textGenerator());
  
  console.log('Processing text through transform stream:');
  
  // Use pipeline to connect streams
  await pipeline(
    readableStream,
    uppercaseTransform,
    async function* (source) {
      for await (const chunk of source) {
        console.log(`   Transformed: ${chunk}`);
        yield chunk;
      }
    }
  );
  
  console.log('Pipeline complete\n');
}

/**
 * Main demo function
 */
async function main() {
  console.log('RemoteMedia TypeScript Generated Types with Streaming\n');
  console.log('=' .repeat(60) + '\n');
  
  try {
    await demonstrateStreamingWithTypes();
    await demonstrateNodeJsIntegration();
    
    console.log('=' .repeat(60));
    console.log('FEATURES DEMONSTRATED:');
    console.log('✅ Auto-generated TypeScript classes with streaming support');
    console.log('✅ Transparent remote execution of generators');
    console.log('✅ Chained streaming operations');
    console.log('✅ Stream aggregation and transformation');
    console.log('✅ Early termination with automatic cleanup');
    console.log('✅ Error handling in streaming contexts');
    console.log('✅ Integration with Node.js native streams');
    console.log('✅ Type-safe remote proxy generation');
    console.log('=' .repeat(60));
  } catch (error) {
    console.error('Demo error:', error);
  }
}

// Run the demo
if (require.main === module) {
  main().catch(console.error);
}