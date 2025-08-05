/**
 * Test script for streaming and generator functionality
 * This simulates the remote execution locally for testing
 */

import { RemoteAsyncGenerator } from './src/remote-proxy-client-streaming';

// Mock gRPC responses for testing
class MockGrpcClient {
  private generators: Map<string, any[]> = new Map();
  private generatorIndices: Map<string, number> = new Map();

  ExecuteObjectMethod(request: any, callback: Function) {
    // Simulate successful method execution
    const result = {
      status: 'EXECUTION_STATUS_SUCCESS',
      session_id: 'test-session-123',
      result_data: Buffer.from(JSON.stringify({
        __generator__: true,
        generator_id: 'gen-' + Date.now(),
        is_async: true
      }))
    };
    callback(null, result);
  }

  InitGenerator(request: any, callback: Function) {
    const generatorId = 'gen-' + Date.now();
    
    // Create mock data based on method name
    let data: any[] = [];
    if (request.method_name.includes('fibonacci')) {
      // Generate Fibonacci numbers
      let a = 0, b = 1;
      while (a < 100) {
        data.push(a);
        const temp = a;
        a = b;
        b = temp + b;
      }
    } else if (request.method_name.includes('sensor')) {
      // Generate sensor data
      for (let i = 0; i < 10; i++) {
        data.push({
          timestamp: Date.now() + i * 1000,
          sensor_id: 'sensor_001',
          value: i * 2.5,
          status: 'active'
        });
      }
    } else {
      // Default data
      for (let i = 0; i < 20; i++) {
        data.push(`Item ${i + 1}`);
      }
    }

    this.generators.set(generatorId, data);
    this.generatorIndices.set(generatorId, 0);

    callback(null, {
      status: 'EXECUTION_STATUS_SUCCESS',
      generator_id: generatorId
    });
  }

  GetNextBatch(request: any, callback: Function) {
    const { generator_id, batch_size } = request;
    const data = this.generators.get(generator_id) || [];
    const index = this.generatorIndices.get(generator_id) || 0;
    
    const batch = data.slice(index, index + batch_size);
    const newIndex = index + batch_size;
    this.generatorIndices.set(generator_id, newIndex);

    const items = batch.map(item => 
      Buffer.from(JSON.stringify(item))
    );

    callback(null, {
      status: 'EXECUTION_STATUS_SUCCESS',
      items,
      has_more: newIndex < data.length
    });
  }

  CloseGenerator(request: any, callback: Function) {
    const { generator_id } = request;
    this.generators.delete(generator_id);
    this.generatorIndices.delete(generator_id);
    
    callback(null, {
      status: 'EXECUTION_STATUS_SUCCESS'
    });
  }

  StreamObject() {
    // Return a mock stream
    const stream = {
      write: (data: any) => {},
      on: (event: string, handler: Function) => {
        if (event === 'data') {
          // Simulate some stream data
          setTimeout(() => {
            handler({ 
              payload: 'data', 
              data: Buffer.from(JSON.stringify({ value: Math.random() }))
            });
          }, 100);
        }
      },
      pause: () => {},
      resume: () => {},
      end: () => {},
      destroy: () => {}
    };
    return stream;
  }
}

// Test class
class TestStreamingDataProcessor {
  private chunkSize: number = 1024;

  *generateFibonacci(limit: number = 100): Generator<number> {
    let a = 0, b = 1;
    while (a < limit) {
      yield a;
      [a, b] = [b, a + b];
    }
  }

  async *streamSensorData(sensorId: string, count: number = 10): AsyncGenerator<any> {
    for (let i = 0; i < count; i++) {
      await new Promise(resolve => setTimeout(resolve, 50));
      yield {
        timestamp: Date.now(),
        sensor_id: sensorId,
        value: i * 2.5,
        status: 'active'
      };
    }
  }

  *readFileChunks(filename: string, numChunks: number = 10): Generator<string> {
    for (let i = 0; i < numChunks; i++) {
      yield `Chunk ${i + 1}/${numChunks} from ${filename} (${this.chunkSize} bytes)`;
    }
  }
}

// Test functions
async function testRemoteAsyncGenerator() {
  console.log('=== Testing RemoteAsyncGenerator ===\n');
  
  const mockClient = new MockGrpcClient();
  const generator = new RemoteAsyncGenerator(mockClient as any, 'gen-test', 5);
  
  console.log('Testing iteration with batch size 5:');
  let count = 0;
  for await (const item of generator) {
    count++;
    console.log(`  Received: ${item}`);
    if (count >= 10) {
      console.log('  Early termination at 10 items');
      break;
    }
  }
  console.log(`Total items received: ${count}\n`);
}

async function testGeneratorMethods() {
  console.log('=== Testing Generator Methods ===\n');
  
  const processor = new TestStreamingDataProcessor();
  
  // Test 1: Sync generator
  console.log('1. Testing sync generator (Fibonacci):');
  const fibGen = processor.generateFibonacci(100);
  const fibNumbers = [];
  for (const num of fibGen) {
    fibNumbers.push(num);
    if (fibNumbers.length >= 5) break;
  }
  console.log(`  First 5 Fibonacci numbers: ${fibNumbers.join(', ')}\n`);
  
  // Test 2: Async generator
  console.log('2. Testing async generator (Sensor data):');
  const sensorGen = processor.streamSensorData('test-sensor', 5);
  let sensorCount = 0;
  for await (const data of sensorGen) {
    sensorCount++;
    console.log(`  Sensor reading ${sensorCount}:`, data);
  }
  console.log(`  Total sensor readings: ${sensorCount}\n`);
  
  // Test 3: File chunks
  console.log('3. Testing file chunk generator:');
  const chunkGen = processor.readFileChunks('test.dat', 3);
  for (const chunk of chunkGen) {
    console.log(`  ${chunk}`);
  }
}

async function testBatchedFetching() {
  console.log('\n=== Testing Batched Fetching ===\n');
  
  const mockClient = new MockGrpcClient();
  
  // Initialize a large dataset
  await new Promise<void>((resolve) => {
    mockClient.InitGenerator({
      method_name: 'generate_large_dataset'
    }, (err: any, response: any) => {
      if (!err && response.status === 'EXECUTION_STATUS_SUCCESS') {
        const generatorId = response.generator_id;
        
        // Manually set up large dataset
        const largeData = Array.from({ length: 100 }, (_, i) => ({
          id: i,
          value: Math.random() * 100,
          timestamp: Date.now() + i
        }));
        (mockClient as any).generators.set(generatorId, largeData);
        (mockClient as any).generatorIndices.set(generatorId, 0);
        
        console.log(`Created generator with ${largeData.length} items`);
        
        // Test batched fetching
        testBatchedGenerator(mockClient, generatorId).then(resolve);
      }
    });
  });
}

async function testBatchedGenerator(client: any, generatorId: string) {
  const generator = new RemoteAsyncGenerator(client, generatorId, 10);
  
  console.log('Fetching with batch size 10:');
  let fetchCount = 0;
  let totalItems = 0;
  
  // Override GetNextBatch to count fetches
  const originalGetNextBatch = client.GetNextBatch.bind(client);
  client.GetNextBatch = function(request: any, callback: Function) {
    fetchCount++;
    console.log(`  Batch fetch #${fetchCount} (requesting ${request.batch_size} items)`);
    originalGetNextBatch(request, callback);
  };
  
  for await (const item of generator) {
    totalItems++;
    if (totalItems <= 3 || totalItems > 97) {
      console.log(`    Item ${totalItems}:`, item);
    } else if (totalItems === 4) {
      console.log('    ... (skipping middle items) ...');
    }
    
    if (totalItems >= 25) {
      console.log(`  Stopping early at ${totalItems} items`);
      break;
    }
  }
  
  console.log(`\nSummary:`);
  console.log(`  Total items received: ${totalItems}`);
  console.log(`  Number of batch fetches: ${fetchCount}`);
  console.log(`  Efficiency: ${(totalItems / (fetchCount * 10) * 100).toFixed(1)}% of fetched items used`);
}

async function testErrorHandling() {
  console.log('\n=== Testing Error Handling ===\n');
  
  const errorClient = {
    GetNextBatch(request: any, callback: Function) {
      // Simulate error after first batch
      if ((this as any).callCount === undefined) {
        (this as any).callCount = 0;
      }
      (this as any).callCount++;
      
      if ((this as any).callCount === 1) {
        // First batch succeeds
        callback(null, {
          status: 'EXECUTION_STATUS_SUCCESS',
          items: [Buffer.from(JSON.stringify('Item 1'))],
          has_more: true
        });
      } else {
        // Second batch fails
        callback(null, {
          status: 'EXECUTION_STATUS_ERROR',
          error_message: 'Simulated generator error',
          items: [],
          has_more: false
        });
      }
    },
    CloseGenerator(request: any, callback: Function) {
      callback(null, { status: 'EXECUTION_STATUS_SUCCESS' });
    }
  };
  
  const generator = new RemoteAsyncGenerator(errorClient as any, 'error-gen', 5);
  
  try {
    console.log('Iterating over generator that will fail:');
    for await (const item of generator) {
      console.log(`  Received: ${item}`);
    }
  } catch (error) {
    console.log(`  Caught expected error: ${(error as Error).message}`);
  }
}

async function runAllTests() {
  console.log('RemoteMedia Streaming Tests\n');
  console.log('=' .repeat(60) + '\n');
  
  try {
    await testGeneratorMethods();
    await testRemoteAsyncGenerator();
    await testBatchedFetching();
    await testErrorHandling();
    
    console.log('\n' + '=' .repeat(60));
    console.log('✅ All tests completed successfully!');
    console.log('=' .repeat(60));
  } catch (error) {
    console.error('\n❌ Test failed:', error);
    process.exit(1);
  }
}

// Run tests
if (require.main === module) {
  runAllTests().catch(console.error);
}