/**
 * Demonstration of Node.js Streams and Generators with RemoteExecutionService
 * 
 * This example shows how to use:
 * - Async generators for streaming data
 * - Node.js Readable/Writable streams
 * - Batched fetching for efficiency
 * - Early termination support
 */

import { RemoteProxyClient, RemoteExecutorConfig, withRemoteProxy } from '../src/remote-proxy-client-streaming';
import { Readable, Writable, pipeline } from 'stream';
import { promisify } from 'util';

const pipelineAsync = promisify(pipeline);

/**
 * Example class with various streaming methods
 */
class StreamingDataProcessor {
  private chunkSize: number = 1024;
  
  /**
   * Synchronous generator that simulates reading file chunks
   */
  *readFileChunks(filename: string, numChunks: number = 10): Generator<string> {
    console.log(`[Local] Starting to read ${filename}`);
    for (let i = 0; i < numChunks; i++) {
      const chunk = `Chunk ${i + 1}/${numChunks} from ${filename} (${this.chunkSize} bytes)`;
      console.log(`[Local] Yielding chunk ${i + 1}`);
      yield chunk;
    }
    console.log(`[Local] Finished reading ${filename}`);
  }
  
  /**
   * Async generator that simulates streaming sensor data
   */
  async *streamSensorData(sensorId: string, count: number = 10): AsyncGenerator<any> {
    console.log(`[Local] Starting sensor stream ${sensorId}`);
    for (let i = 0; i < count; i++) {
      // Simulate async operation
      await new Promise(resolve => setTimeout(resolve, 100));
      
      const data = {
        timestamp: Date.now(),
        sensor_id: sensorId,
        value: i * 2.5,
        status: 'active'
      };
      console.log(`[Local] Streaming data point ${i + 1}`);
      yield data;
    }
    console.log(`[Local] Sensor stream complete`);
  }
  
  /**
   * Generate Fibonacci numbers as a generator
   */
  *generateFibonacci(limit: number = 100): Generator<number> {
    let a = 0, b = 1;
    while (a < limit) {
      yield a;
      [a, b] = [b, a + b];
    }
  }
  
  /**
   * Create a readable stream of random numbers
   */
  createRandomNumberStream(count: number = 100): Readable {
    let produced = 0;
    
    return new Readable({
      objectMode: true,
      read() {
        if (produced < count) {
          this.push(Math.random());
          produced++;
        } else {
          this.push(null); // End stream
        }
      }
    });
  }
  
  /**
   * Create a writable stream that accumulates values
   */
  createAccumulatorStream(): Writable & { getTotal: () => number } {
    let total = 0;
    
    const stream = new Writable({
      objectMode: true,
      write(chunk, encoding, callback) {
        total += Number(chunk);
        console.log(`[Accumulator] Received: ${chunk}, Total: ${total}`);
        callback();
      }
    });
    
    // Add custom method
    (stream as any).getTotal = () => total;
    
    return stream as Writable & { getTotal: () => number };
  }
  
  /**
   * Process streaming data with transformation
   */
  async processStreamingData(data: AsyncIterable<any>): Promise<number> {
    let count = 0;
    for await (const item of data) {
      console.log(`[Processor] Processing item:`, item);
      count++;
    }
    return count;
  }
}

/**
 * Demonstrate generator streaming
 */
async function testGeneratorStreaming() {
  console.log('=== GENERATOR STREAMING TEST ===\n');
  
  const config: RemoteExecutorConfig = {
    host: 'localhost',
    port: 50052,
    batchSize: 5 // Fetch 5 items at a time
  };
  
  await withRemoteProxy(config, async (client) => {
    const processor = new StreamingDataProcessor();
    const pythonCode = `
import asyncio
import time

class StreamingDataProcessor:
    def __init__(self):
        self.chunk_size = 1024
    
    def read_file_chunks(self, filename, num_chunks=10):
        """Synchronous generator that simulates reading file chunks"""
        print(f"[Server] Starting to read {filename}")
        for i in range(num_chunks):
            time.sleep(0.1)  # Simulate I/O
            chunk = f"Chunk {i+1}/{num_chunks} from {filename} ({self.chunk_size} bytes)"
            print(f"[Server] Yielding chunk {i+1}")
            yield chunk
        print(f"[Server] Finished reading {filename}")
    
    async def stream_sensor_data(self, sensor_id, count=10):
        """Async generator that simulates streaming sensor data"""
        print(f"[Server] Starting sensor stream {sensor_id}")
        for i in range(count):
            await asyncio.sleep(0.1)  # Simulate real-time delay
            data = {
                "timestamp": time.time(),
                "sensor_id": sensor_id,
                "value": i * 2.5,
                "status": "active"
            }
            print(f"[Server] Streaming data point {i+1}")
            yield data
        print(f"[Server] Sensor stream complete")
    
    def generate_fibonacci(self, limit=100):
        """Generate Fibonacci numbers as a generator"""
        a, b = 0, 1
        while a < limit:
            yield a
            a, b = b, a + b
`;
    
    const remote = await client.createProxy(processor, pythonCode);
    
    // Test 1: Sync generator (becomes async in Node.js)
    console.log('1. Testing sync generator streaming:');
    const fileGenerator = await remote.readFileChunks('test.dat', 5);
    
    let chunkCount = 0;
    for await (const chunk of fileGenerator) {
      chunkCount++;
      console.log(`   [Client] Received: ${chunk}`);
    }
    console.log(`   Total chunks received: ${chunkCount}\n`);
    
    // Test 2: Async generator
    console.log('2. Testing async generator streaming:');
    const sensorGenerator = await remote.streamSensorData('sensor_001', 5);
    
    let dataCount = 0;
    for await (const data of sensorGenerator) {
      dataCount++;
      console.log(`   [Client] Received:`, data);
    }
    console.log(`   Total data points received: ${dataCount}\n`);
    
    // Test 3: Early termination
    console.log('3. Testing early termination:');
    const fibGenerator = await remote.generateFibonacci(1000);
    
    let fibCount = 0;
    for await (const num of fibGenerator) {
      console.log(`   [Client] Fib[${fibCount}] = ${num}`);
      fibCount++;
      if (fibCount >= 5) {
        console.log('   [Client] Stopping early!');
        await fibGenerator.close(); // Properly close the generator
        break;
      }
    }
    console.log(`   Only received ${fibCount} numbers (generator properly closed)\n`);
  });
}

/**
 * Demonstrate Node.js stream support
 */
async function testNodeStreams() {
  console.log('=== NODE.JS STREAMS TEST ===\n');
  
  const config: RemoteExecutorConfig = {
    host: 'localhost',
    port: 50052
  };
  
  await withRemoteProxy(config, async (client) => {
    const processor = new StreamingDataProcessor();
    const pythonCode = `
import asyncio
import random

class StreamingDataProcessor:
    def create_random_number_stream(self, count=100):
        """Create a generator of random numbers"""
        for i in range(count):
            yield random.random()
    
    async def process_stream(self, stream_data):
        """Process streaming data"""
        total = 0
        count = 0
        async for item in stream_data:
            total += float(item)
            count += 1
        return {"count": count, "total": total, "average": total / count if count > 0 else 0}
`;
    
    const remote = await client.createProxy(processor, pythonCode);
    
    // Test 1: Remote generator as Node.js stream
    console.log('1. Testing remote generator as readable stream:');
    const randomStream = await remote.createRandomNumberStream(10);
    
    // Collect values
    const values: number[] = [];
    for await (const value of randomStream) {
      values.push(value);
      console.log(`   [Client] Received random: ${value}`);
    }
    console.log(`   Total values: ${values.length}\n`);
    
    // Test 2: Pipeline with streams
    console.log('2. Testing stream pipeline:');
    
    // Create a local transform stream
    const doubleTransform = new Transform({
      objectMode: true,
      transform(chunk, encoding, callback) {
        callback(null, chunk * 2);
      }
    });
    
    // Create accumulator
    const accumulator = processor.createAccumulatorStream();
    
    // This would require more complex implementation for full streaming
    // For now, demonstrate the concept
    console.log('   (Full stream piping requires bidirectional streaming support)\n');
  });
}

/**
 * Demonstrate batched processing
 */
async function testBatchedProcessing() {
  console.log('=== BATCHED PROCESSING TEST ===\n');
  
  const config: RemoteExecutorConfig = {
    host: 'localhost',
    port: 50052,
    batchSize: 10 // Fetch 10 items at a time
  };
  
  await withRemoteProxy(config, async (client) => {
    const processor = new StreamingDataProcessor();
    const pythonCode = `
class StreamingDataProcessor:
    def generate_large_dataset(self, size=1000):
        """Generate a large dataset"""
        for i in range(size):
            yield {
                "id": i,
                "value": i * 0.1,
                "category": f"cat_{i % 10}"
            }
`;
    
    const remote = await client.createProxy(processor, pythonCode);
    
    console.log('Generating large dataset with batched fetching:');
    const start = Date.now();
    const generator = await remote.generateLargeDataset(100);
    
    let count = 0;
    let firstBatchTime = 0;
    
    for await (const item of generator) {
      count++;
      if (count === 1) {
        firstBatchTime = Date.now() - start;
        console.log(`   First item received after ${firstBatchTime}ms`);
      }
      
      // Process only first 25 items
      if (count <= 25) {
        if (count % 10 === 0) {
          console.log(`   Processed ${count} items...`);
        }
      } else {
        console.log('   Stopping at 25 items (early termination)');
        break;
      }
    }
    
    const totalTime = Date.now() - start;
    console.log(`   Total: Processed ${count} items in ${totalTime}ms`);
    console.log('   Note: Only requested/generated items were transferred\n');
  });
}

/**
 * Main function to run all tests
 */
async function main() {
  console.log('RemoteMedia Node.js Streaming & Generators Demo\n');
  console.log('=' .repeat(60) + '\n');
  
  try {
    await testGeneratorStreaming();
    await testNodeStreams();
    await testBatchedProcessing();
    
    console.log('=' .repeat(60));
    console.log('SUMMARY:');
    console.log('✅ Generator streaming with async iteration');
    console.log('✅ Batched fetching for efficiency');
    console.log('✅ Early termination support');
    console.log('✅ Transparent remote execution');
    console.log('✅ Type-safe proxy generation');
    console.log('=' .repeat(60));
  } catch (error) {
    console.error('Error:', error);
  }
}

// Run the demo
if (require.main === module) {
  main().catch(console.error);
}