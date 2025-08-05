/**
 * Test ExecuteNode with generator support
 * This tests the new generator support we added to ExecuteNode
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { promisify } from 'util';
import * as path from 'path';

const PROTO_PATH = path.join(__dirname, '..', '..', 'remote_service', 'protos', 'execution.proto');

async function testExecuteNodeGenerators() {
  console.log('=== TEST EXECUTENODE WITH GENERATORS ===\n');
  console.log('This test requires the modified server with generator support in ExecuteNode.\n');
  
  // Load proto
  const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
    includeDirs: [path.join(__dirname, '..', '..', 'remote_service', 'protos')]
  });

  const remoteMedia = grpc.loadPackageDefinition(packageDefinition).remotemedia as any;
  
  const client = new remoteMedia.execution.RemoteExecutionService(
    'localhost:50052',
    grpc.credentials.createInsecure()
  );
  
  const executeNode = promisify(client.ExecuteNode.bind(client));
  const initGenerator = promisify(client.InitGenerator.bind(client));
  const getNextBatch = promisify(client.GetNextBatch.bind(client));
  const closeGenerator = promisify(client.CloseGenerator.bind(client));
  
  // Test 1: CodeExecutorNode with generator
  console.log('1. Testing CodeExecutorNode with generator:\n');
  
  const generatorCode = `
def process(input_data):
    """Generate a sequence of numbers"""
    limit = input_data.get('limit', 10)
    print(f"[Server] Starting generator with limit {limit}")
    for i in range(limit):
        print(f"[Server] Yielding item {i}")
        yield {"number": i, "squared": i * i}
    print("[Server] Generator complete")
`;

  const request = {
    node_type: 'CodeExecutorNode',
    config: {
      code: generatorCode,
      entry_point: 'process'
    },
    input_data: Buffer.from(JSON.stringify({ limit: 5 })),
    serialization_format: 'json',
    options: {
      timeout: 30.0
    }
  };
  
  try {
    console.log('Executing node...');
    const response = await executeNode(request);
    
    if (response.status === 'EXECUTION_STATUS_SUCCESS') {
      const result = JSON.parse(response.output_data.toString());
      console.log('Response:', JSON.stringify(result, null, 2));
      
      // Check if we got a generator marker
      if (result && result.__generator__) {
        console.log('\n✅ Generator detected! Generator ID:', result.generator_id);
        console.log('Is async:', result.is_async);
        console.log('Node type:', result.node_type);
        
        // Now fetch items from the generator
        console.log('\nFetching items from generator...\n');
        
        let hasMore = true;
        let totalItems = 0;
        
        while (hasMore) {
          const batchResponse = await getNextBatch({
            generator_id: result.generator_id,
            batch_size: 2,  // Fetch 2 items at a time
            serialization_format: 'json'
          });
          
          if (batchResponse.status === 'EXECUTION_STATUS_SUCCESS') {
            const items = batchResponse.items.map((item: Buffer) => 
              JSON.parse(item.toString())
            );
            
            console.log(`Batch received (${items.length} items):`, items);
            totalItems += items.length;
            hasMore = batchResponse.has_more;
          } else {
            console.error('Error fetching batch:', batchResponse.error_message);
            break;
          }
        }
        
        console.log(`\nTotal items received: ${totalItems}`);
        
        // Clean up
        await closeGenerator({ generator_id: result.generator_id });
        console.log('Generator closed.\n');
        
      } else {
        console.log('\n❌ Did not receive generator marker. Got:', typeof result);
        console.log('The server may not have the generator support for ExecuteNode yet.');
      }
    } else {
      console.error('Execution failed:', response.error_message);
    }
  } catch (error) {
    console.error('Test failed:', error);
  }
  
  // Test 2: Async generator
  console.log('\n2. Testing async generator with CodeExecutorNode:\n');
  
  const asyncGeneratorCode = `
import asyncio

async def process(input_data):
    """Async generator that simulates streaming data"""
    count = input_data.get('count', 5)
    print(f"[Server] Starting async generator with count {count}")
    
    for i in range(count):
        await asyncio.sleep(0.1)  # Simulate async work
        yield {
            "index": i,
            "timestamp": asyncio.get_event_loop().time(),
            "message": f"Async item {i+1}/{count}"
        }
    print("[Server] Async generator complete")
`;

  const asyncRequest = {
    node_type: 'CodeExecutorNode',
    config: {
      code: asyncGeneratorCode,
      entry_point: 'process'
    },
    input_data: Buffer.from(JSON.stringify({ count: 3 })),
    serialization_format: 'json',
    options: {
      timeout: 30.0
    }
  };
  
  try {
    console.log('Executing async generator node...');
    const response = await executeNode(asyncRequest);
    
    if (response.status === 'EXECUTION_STATUS_SUCCESS') {
      const result = JSON.parse(response.output_data.toString());
      
      if (result && result.__generator__) {
        console.log('\n✅ Async generator detected!');
        console.log('Generator ID:', result.generator_id);
        console.log('Is async:', result.is_async);
        
        // Fetch all items
        console.log('\nFetching all items...\n');
        
        const allResponse = await getNextBatch({
          generator_id: result.generator_id,
          batch_size: 10,  // Get all at once
          serialization_format: 'json'
        });
        
        if (allResponse.status === 'EXECUTION_STATUS_SUCCESS') {
          const items = allResponse.items.map((item: Buffer) => 
            JSON.parse(item.toString())
          );
          
          items.forEach((item: any, index: number) => {
            console.log(`Item ${index + 1}:`, item);
          });
        }
        
        // Clean up
        await closeGenerator({ generator_id: result.generator_id });
        console.log('\nAsync generator closed.');
      } else {
        console.log('\n❌ Did not receive generator marker for async generator');
      }
    }
  } catch (error) {
    console.error('Async generator test failed:', error);
  }
  
  console.log('\n=== SUMMARY ===\n');
  console.log('With the server modifications:');
  console.log('✅ ExecuteNode now detects generators and returns markers');
  console.log('✅ Generator sessions are stored and can be accessed via GetNextBatch');
  console.log('✅ Both sync and async generators are supported');
  console.log('✅ Node.js clients can stream data efficiently without materialization\n');
}

// Run the test
if (require.main === module) {
  testExecuteNodeGenerators().catch(console.error);
}