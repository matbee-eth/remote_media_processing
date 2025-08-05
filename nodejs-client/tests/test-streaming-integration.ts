/**
 * Integration test for streaming and generator functionality
 * This actually connects to the remote gRPC server
 */

import { RemoteProxyClient, RemoteExecutorConfig, withRemoteProxy } from './src/remote-proxy-client-streaming';

/**
 * Test class with streaming methods that will execute on the server
 */
class StreamingDataProcessor {
  private chunkSize: number = 1024;

  constructor(chunkSize: number = 1024) {
    this.chunkSize = chunkSize;
  }

  *readFileChunks(filename: string, numChunks: number = 10): Generator<string> {
    console.log(`[Local] Starting to read ${filename}`);
    for (let i = 0; i < numChunks; i++) {
      const chunk = `Chunk ${i + 1}/${numChunks} from ${filename} (${this.chunkSize} bytes)`;
      yield chunk;
    }
  }

  async *streamSensorData(sensorId: string, count: number = 10): AsyncGenerator<any> {
    for (let i = 0; i < count; i++) {
      await new Promise(resolve => setTimeout(resolve, 100));
      const data = {
        timestamp: Date.now(),
        sensor_id: sensorId,
        value: i * 2.5,
        status: 'active'
      };
      yield data;
    }
  }

  *generateFibonacci(limit: number = 100): Generator<number> {
    let a = 0, b = 1;
    while (a < limit) {
      yield a;
      [a, b] = [b, a + b];
    }
  }

  *generateLargeDataset(size: number = 50): Generator<any> {
    for (let i = 0; i < size; i++) {
      yield {
        id: i,
        value: i * 0.1,
        category: `cat_${i % 10}`
      };
    }
  }
}

/**
 * Python code that mirrors the JavaScript class
 */
const PYTHON_CODE = `
import asyncio
import time

class StreamingDataProcessor:
    def __init__(self, chunk_size=1024):
        self.chunk_size = chunk_size
    
    def read_file_chunks(self, filename, num_chunks=10):
        """Synchronous generator that simulates reading file chunks"""
        print(f"[Server] Starting to read {filename}")
        for i in range(num_chunks):
            time.sleep(0.05)  # Simulate I/O
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
                "timestamp": time.time() * 1000,  # Convert to milliseconds
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
        count = 0
        while a < limit:
            yield a
            a, b = b, a + b
            count += 1
        print(f"[Server] Generated {count} Fibonacci numbers")
`;

async function testRealStreaming() {
  console.log('=== REAL STREAMING INTEGRATION TEST ===\n');
  console.log('Connecting to gRPC server at localhost:50052...\n');

  const config: RemoteExecutorConfig = {
    host: 'localhost',
    port: 50052,
    batchSize: 5 // Fetch 5 items at a time
  };

  try {
    await withRemoteProxy(config, async (client) => {
      console.log('Connected successfully!\n');
      
      const processor = new StreamingDataProcessor(2048);
      const remote = await client.createProxy(processor, PYTHON_CODE);

      // Test 1: Sync generator streaming
      console.log('1. Testing sync generator streaming (should execute on server):');
      console.log('   Watch the server logs for "[Server]" messages\n');
      
      try {
        const fileGenerator = await remote.readFileChunks('test.dat', 5);
        
        console.log('   Client: Starting to consume chunks...');
        let chunkCount = 0;
        for await (const chunk of fileGenerator) {
          chunkCount++;
          console.log(`   [Client] Received: ${chunk}`);
        }
        console.log(`   Client: Total chunks received: ${chunkCount}\n`);
      } catch (error) {
        console.error('   Error in sync generator:', error);
      }

      // Test 2: Async generator streaming
      console.log('2. Testing async generator streaming:');
      console.log('   Watch the server logs for sensor data messages\n');
      
      try {
        const sensorGenerator = await remote.streamSensorData('sensor_001', 5);
        
        console.log('   Client: Starting to consume sensor data...');
        let dataCount = 0;
        for await (const data of sensorGenerator) {
          dataCount++;
          console.log(`   [Client] Received sensor data #${dataCount}:`, data);
        }
        console.log(`   Client: Total data points received: ${dataCount}\n`);
      } catch (error) {
        console.error('   Error in async generator:', error);
      }

      // Test 3: Early termination
      console.log('3. Testing early termination:');
      console.log('   The server should stop generating after we break\n');
      
      try {
        const fibGenerator = await remote.generateFibonacci(1000);
        
        console.log('   Client: Getting first 5 Fibonacci numbers...');
        let fibCount = 0;
        for await (const num of fibGenerator) {
          console.log(`   [Client] Fib[${fibCount}] = ${num}`);
          fibCount++;
          if (fibCount >= 5) {
            console.log('   [Client] Breaking early - server should see this and stop!');
            break;
          }
        }
        console.log(`   Client: Only received ${fibCount} numbers\n`);
      } catch (error) {
        console.error('   Error in Fibonacci generator:', error);
      }

      // Test 4: Large dataset with batching
      console.log('4. Testing batched fetching with larger dataset:');
      console.log('   Generating 50 items but fetching in batches of 5\n');
      
      const largeDatasetCode = PYTHON_CODE + `
    
    def generate_large_dataset(self, size=50):
        """Generate a large dataset"""
        print(f"[Server] Starting to generate {size} items")
        for i in range(size):
            if i > 0 and i % 10 == 0:
                print(f"[Server] Generated {i} items so far...")
            yield {
                "id": i,
                "value": i * 0.1,
                "category": f"cat_{i % 10}"
            }
        print(f"[Server] Finished generating {size} items")
`;

      const remoteWithLarge = await client.createProxy(processor, largeDatasetCode);
      
      try {
        const largeGenerator = await remoteWithLarge.generateLargeDataset(50);
        
        console.log('   Client: Starting to consume large dataset...');
        let itemCount = 0;
        const startTime = Date.now();
        
        for await (const item of largeGenerator) {
          itemCount++;
          if (itemCount <= 3 || itemCount > 47) {
            console.log(`   [Client] Item ${itemCount}:`, item);
          } else if (itemCount === 4) {
            console.log('   ... (showing only first 3 and last 3 items) ...');
          }
        }
        
        const duration = Date.now() - startTime;
        console.log(`   Client: Received all ${itemCount} items in ${duration}ms`);
        console.log(`   Client: Average time per item: ${(duration / itemCount).toFixed(2)}ms\n`);
      } catch (error) {
        console.error('   Error in large dataset:', error);
      }
    });

    console.log('✅ Integration test completed successfully!');
    console.log('Check the server logs to see the remote execution happening.\n');

  } catch (error) {
    console.error('❌ Integration test failed:', error);
    console.error('\nMake sure the gRPC server is running:');
    console.error('cd remote_service && docker-compose up');
  }
}

// Run the integration test
if (require.main === module) {
  console.log('RemoteMedia Streaming Integration Test\n');
  console.log('This test requires the gRPC server to be running.');
  console.log('Start it with: cd remote_service && docker-compose up\n');
  console.log('=' .repeat(60) + '\n');
  
  testRealStreaming().catch(console.error);
}