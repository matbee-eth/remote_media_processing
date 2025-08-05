/**
 * Test streaming with existing SDK nodes
 * Uses ExecuteNode instead of ExecuteObjectMethod
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { promisify } from 'util';
import * as path from 'path';

const PROTO_PATH = path.join(__dirname, '..', '..', 'remote_service', 'protos', 'execution.proto');

async function testStreamingWithNodes() {
  console.log('=== STREAMING TEST WITH SDK NODES ===\n');
  
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
  
  // Test 1: Execute a node that might return a generator
  console.log('1. Testing CodeExecutorNode with generator code:\n');
  
  const generatorCode = `
def process(input_data):
    """Generate a sequence of numbers"""
    limit = input_data.get('limit', 10)
    for i in range(limit):
        yield {"number": i, "squared": i * i}
`;

  const codeNodeRequest = {
    node_type: 'CodeExecutorNode',
    config: {
      code: generatorCode,
      entry_point: 'process'
    },
    input_data: Buffer.from(JSON.stringify({ limit: 20 })),
    serialization_format: 'json',
    options: {
      timeout: 30.0
    }
  };
  
  try {
    console.log('Executing generator code...');
    const response = await executeNode(codeNodeRequest);
    
    if (response.status === 'EXECUTION_STATUS_SUCCESS') {
      const result = JSON.parse(response.output_data.toString());
      console.log('Response type:', typeof result);
      console.log('Response:', JSON.stringify(result, null, 2));
      
      // Check if it's a generator marker
      if (result && result.__generator__) {
        console.log('\nDetected generator! Would need to use generator APIs to iterate.\n');
      } else {
        console.log('\nGot materialized list (not streaming).\n');
      }
    } else {
      console.error(`Error: ${response.error_message}\n`);
    }
  } catch (error) {
    console.error('Request failed:', error, '\n');
  }

  // Test 2: Use a streaming-capable node
  console.log('2. Testing with an async generator in CodeExecutorNode:\n');
  
  const asyncGeneratorCode = `
import asyncio

async def process(input_data):
    """Async generator that simulates streaming data"""
    count = input_data.get('count', 5)
    delay = input_data.get('delay', 0.1)
    
    for i in range(count):
        await asyncio.sleep(delay)
        yield {
            "index": i,
            "timestamp": str(asyncio.get_event_loop().time()),
            "message": f"Async item {i+1}/{count}"
        }
`;

  const asyncCodeRequest = {
    node_type: 'CodeExecutorNode', 
    config: {
      code: asyncGeneratorCode,
      entry_point: 'process'
    },
    input_data: Buffer.from(JSON.stringify({ count: 10, delay: 0.05 })),
    serialization_format: 'json',
    options: {
      timeout: 30.0
    }
  };
  
  try {
    console.log('Executing async generator code...');
    const response = await executeNode(asyncCodeRequest);
    
    if (response.status === 'EXECUTION_STATUS_SUCCESS') {
      const result = JSON.parse(response.output_data.toString());
      console.log('Response:', JSON.stringify(result, null, 2));
      console.log('\nNote: Generators are currently materialized into lists by ExecuteNode.\n');
    } else {
      console.error(`Error: ${response.error_message}\n`);
    }
  } catch (error) {
    console.error('Request failed:', error, '\n');
  }

  // Test 3: Direct use of SerializedClassExecutorNode
  console.log('3. Testing SerializedClassExecutorNode:\n');
  
  const classCode = `
import cloudpickle as pickle

class StreamingProcessor:
    def __init__(self):
        self.counter = 0
    
    def generate_data(self, count=5):
        for i in range(count):
            self.counter += 1
            yield {"id": self.counter, "value": i * 2}
    
    async def async_generate(self, count=5):
        import asyncio
        for i in range(count):
            await asyncio.sleep(0.05)
            yield {"async_id": i, "data": f"async_{i}"}

# Create and pickle the instance
processor = StreamingProcessor()
serialized = pickle.dumps(processor)
`;

  // First, we need to create the serialized object
  const createSerializedRequest = {
    node_type: 'CodeExecutorNode',
    config: {
      code: classCode + '\ndef process(input_data): return serialized',
      entry_point: 'process'
    },
    input_data: Buffer.from(JSON.stringify({})),
    serialization_format: 'pickle',
    options: {
      timeout: 30.0
    }
  };

  try {
    console.log('Creating serialized object...');
    const createResponse = await executeNode(createSerializedRequest);
    
    if (createResponse.status === 'EXECUTION_STATUS_SUCCESS') {
      // Now use the serialized object
      const useSerializedRequest = {
        node_type: 'SerializedClassExecutorNode',
        config: {},
        input_data: createResponse.output_data, // The pickled object
        serialization_format: 'pickle',
        options: {
          timeout: 30.0
        }
      };
      
      console.log('Executing method on serialized object...');
      const useResponse = await executeNode(useSerializedRequest);
      
      if (useResponse.status === 'EXECUTION_STATUS_SUCCESS') {
        console.log('SerializedClassExecutorNode response received');
        // This would process the data
      } else {
        console.error(`Error using serialized object: ${useResponse.error_message}\n`);
      }
    } else {
      console.error(`Error creating serialized object: ${createResponse.error_message}\n`);
    }
  } catch (error) {
    console.error('Serialized class test failed:', error, '\n');
  }

  console.log('=== SUMMARY ===\n');
  console.log('Current SDK behavior with ExecuteNode:');
  console.log('- Generators are materialized into lists (not true streaming)');
  console.log('- Both sync and async generators return complete results');
  console.log('- For true streaming, need to use StreamNode or the new generator APIs\n');
  console.log('To enable true streaming for generators:');
  console.log('1. Use ExecuteObjectMethod with proper code packaging');
  console.log('2. Use the InitGenerator/GetNextBatch/CloseGenerator APIs');
  console.log('3. Or use StreamNode for bidirectional streaming\n');
}

// Run the test
if (require.main === module) {
  testStreamingWithNodes().catch(console.error);
}