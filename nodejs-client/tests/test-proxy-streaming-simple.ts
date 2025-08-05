/**
 * Simple test of the proxy streaming functionality
 * Using the existing proxy example pattern
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { promisify } from 'util';
import * as path from 'path';

const PROTO_PATH = path.join(__dirname, '..', '..', 'remote_service', 'protos', 'execution.proto');

async function testProxyStreaming() {
  console.log('=== SIMPLE PROXY STREAMING TEST ===\n');
  
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
  
  // Test with SerializedClassExecutorNode - similar to the calculator example
  console.log('Testing generator with SerializedClassExecutorNode:\n');
  
  // First, let's create a simple Python object that has generator methods
  const pythonCode = `
# Simple data processor with generators
class DataProcessor:
    def __init__(self):
        self.name = "DataProcessor"
    
    def generate_numbers(self, limit=10):
        """Generate numbers up to limit"""
        for i in range(limit):
            yield i
    
    def process(self, data):
        """Process method that returns the generator"""
        limit = data.get('limit', 5)
        # Return the generator itself
        return self.generate_numbers(limit)

# Create instance
processor = DataProcessor()
`;

  const request = {
    node_type: 'SerializedClassExecutorNode',
    config: {
      class_name: 'DataProcessor',
      code: pythonCode,
      method_name: 'process'
    },
    input_data: Buffer.from(JSON.stringify({ limit: 10 })),
    serialization_format: 'json',
    options: {
      timeout: 30.0
    }
  };
  
  try {
    console.log('Executing SerializedClassExecutorNode...');
    const response = await executeNode(request);
    
    if (response.status === 'EXECUTION_STATUS_SUCCESS') {
      const result = JSON.parse(response.output_data.toString());
      console.log('Response:', JSON.stringify(result, null, 2));
      
      // Check what we got back
      if (Array.isArray(result)) {
        console.log(`\nReceived array with ${result.length} items (generator was materialized)`);
      } else if (result && result.__generator__) {
        console.log('\nReceived generator marker! Can use generator APIs to stream.');
        console.log('Generator ID:', result.generator_id);
      } else {
        console.log('\nReceived:', typeof result);
      }
    } else {
      console.error(`Error: ${response.error_message}`);
      if (response.error_traceback) {
        console.error('Traceback:', response.error_traceback);
      }
    }
  } catch (error) {
    console.error('Request failed:', error);
  }

  // Test 2: Try with a different approach
  console.log('\n\nTesting with CodeExecutorNode returning generator:\n');
  
  const codeExecutorRequest = {
    node_type: 'CodeExecutorNode',
    config: {
      code: `
def process(input_data):
    # Try to return a generator marker
    def gen():
        for i in range(input_data.get('count', 5)):
            yield i * 2
    
    # Return the generator function result
    return gen()
`,
      entry_point: 'process'
    },
    input_data: Buffer.from(JSON.stringify({ count: 7 })),
    serialization_format: 'json',
    options: {
      timeout: 30.0
    }
  };
  
  try {
    console.log('Executing CodeExecutorNode with generator...');
    const response = await executeNode(codeExecutorRequest);
    
    if (response.status === 'EXECUTION_STATUS_SUCCESS') {
      const result = JSON.parse(response.output_data.toString());
      console.log('Response:', JSON.stringify(result, null, 2));
    } else {
      console.error(`Error: ${response.error_message}`);
    }
  } catch (error) {
    console.error('Request failed:', error);
  }
}

// Run the test
if (require.main === module) {
  testProxyStreaming().catch(console.error);
}