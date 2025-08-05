/**
 * Test generator support by creating a proper custom node
 * This demonstrates how generator support works with the server modifications
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { promisify } from 'util';
import * as path from 'path';

const PROTO_PATH = path.join(__dirname, '..', '..', 'remote_service', 'protos', 'execution.proto');

async function testCustomGeneratorNode() {
  console.log('=== TEST CUSTOM GENERATOR NODE ===\n');
  console.log('This test creates a custom node that the server can register and execute.\n');
  
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
  const getNextBatch = promisify(client.GetNextBatch.bind(client));
  const closeGenerator = promisify(client.CloseGenerator.bind(client));
  
  // The key insight: We need to create a node instance and serialize it,
  // then use SerializedClassExecutorNode to execute it
  
  console.log('Creating a test with SerializedClassExecutorNode:\n');
  
  // Step 1: Create Python code that defines and pickles a generator node
  const createNodeCode = `
import cloudpickle
import base64

# Define a custom node class
class TestGeneratorNode:
    def __init__(self, name="TestGenerator"):
        self.name = name
        
    def process(self, data):
        """Returns a generator."""
        count = data.get('count', 5) if isinstance(data, dict) else 5
        
        def generate():
            for i in range(count):
                yield {
                    'index': i,
                    'message': f'Generated item {i}',
                    'from': self.name
                }
        
        return generate()
    
    def __getstate__(self):
        return {'name': self.name}
    
    def __setstate__(self, state):
        self.name = state['name']

# Create and serialize the node
node = TestGeneratorNode("MyTestGenerator")
serialized = cloudpickle.dumps(node)

# Since CodeExecutorNode expects a result, encode it
result = {
    'serialized_object': base64.b64encode(serialized).decode('utf-8'),
    'class_name': 'TestGeneratorNode'
}
`;

  // Execute code to create the serialized node
  const createRequest = {
    node_type: 'CodeExecutorNode',
    config: {
      code: createNodeCode,
      entry_point: 'process'
    },
    input_data: Buffer.from(JSON.stringify({})),
    serialization_format: 'json',
    options: {
      timeout: 30.0
    }
  };
  
  try {
    console.log('Step 1: Creating and serializing the generator node...');
    const createResponse = await executeNode(createRequest);
    
    if (createResponse.status === 'EXECUTION_STATUS_SUCCESS') {
      const createResult = JSON.parse(createResponse.output_data.toString());
      console.log('Node created successfully');
      console.log('Full response:', JSON.stringify(createResult, null, 2));
      
      if (!createResult.result || typeof createResult.result === 'string') {
        console.error('Unexpected result format:', createResult.result);
        return;
      }
      
      // Step 2: Prepare the serialized object for SerializedClassExecutorNode
      // const serializedObject = Buffer.from(createResult.result.serialized_object, 'base64');
      
      // Create input that includes the serialized object
      const executeInput = {
        serialized_object: createResult.result.serialized_object,  // Base64 encoded
        count: 3  // Parameters for the generator
      };
      
      console.log('\nStep 2: Executing SerializedClassExecutorNode...');
      const executeRequest = {
        node_type: 'SerializedClassExecutorNode',
        config: {},
        input_data: Buffer.from(JSON.stringify(executeInput)),
        serialization_format: 'json',
        options: {
          timeout: 30.0
        }
      };
      
      const response = await executeNode(executeRequest);
      
      if (response.status === 'EXECUTION_STATUS_SUCCESS') {
        const result = JSON.parse(response.output_data.toString());
        console.log('\nResponse:', JSON.stringify(result, null, 2));
        
        // Check if we got a generator marker
        if (result && result.__generator__) {
          console.log('\n✅ Generator detected!');
          console.log('Generator ID:', result.generator_id);
          console.log('Is async:', result.is_async);
          console.log('Node type:', result.node_type);
          
          // Fetch items from the generator
          console.log('\nFetching items from generator...\n');
          
          let hasMore = true;
          let totalItems = 0;
          
          while (hasMore) {
            const batchResponse = await getNextBatch({
              generator_id: result.generator_id,
              batch_size: 5,
              serialization_format: 'json'
            });
            
            if (batchResponse.status === 'EXECUTION_STATUS_SUCCESS') {
              const items = batchResponse.items.map((item: Buffer) => 
                JSON.parse(item.toString())
              );
              
              items.forEach((item: any) => {
                console.log(`Item:`, item);
              });
              
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
          console.log('Generator closed.');
          
        } else if (result.error) {
          console.log('\n❌ Error from SerializedClassExecutorNode:', result.error);
        } else {
          console.log('\n❌ No generator marker found in response');
          console.log('This means the server may not have the generator detection in ExecuteNode');
        }
      } else {
        console.error('Execution failed:', response.error_message);
      }
    } else {
      console.error('Failed to create node:', createResponse.error_message);
    }
  } catch (error) {
    console.error('Test failed:', error);
  }
  
  console.log('\n=== SUMMARY ===\n');
  console.log('To properly test generator support:');
  console.log('1. Create a custom node class that returns a generator from process()');
  console.log('2. Serialize it and use SerializedClassExecutorNode');
  console.log('3. With server modifications, generators are detected and can be streamed');
  console.log('4. Without modifications, the generator is materialized into a list\n');
}

// Run the test
if (require.main === module) {
  testCustomGeneratorNode().catch(console.error);
}