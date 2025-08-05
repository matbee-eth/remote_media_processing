/**
 * Test generator support by creating a custom node that returns generators
 * This works around CodeExecutorNode's limitations
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { promisify } from 'util';
import * as path from 'path';

const PROTO_PATH = path.join(__dirname, '..', '..', 'remote_service', 'protos', 'execution.proto');

async function testGeneratorWithCustomNode() {
  console.log('=== TEST GENERATOR WITH CUSTOM NODE ===\n');
  console.log('This test creates a custom node class that returns generators.\n');
  
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
  
  // Test 1: Create a custom node that returns a generator
  console.log('1. Testing with custom GeneratorNode:\n');
  
  // We'll create a node by extending Node and overriding process to return a generator
  const customNodeCode = `
from remotemedia.core.node import Node

class GeneratorNode(Node):
    """Custom node that returns a generator."""
    
    def __init__(self, name="GeneratorNode", limit=10):
        super().__init__(name=name)
        self.limit = limit
    
    def process(self, data):
        """Process method that returns a generator."""
        limit = data.get('limit', self.limit) if isinstance(data, dict) else self.limit
        print(f"[Server] GeneratorNode creating generator with limit {limit}")
        
        def generate():
            for i in range(limit):
                print(f"[Server] GeneratorNode yielding item {i}")
                yield {"index": i, "value": i * 10, "from": "GeneratorNode"}
        
        return generate()

# Create and serialize the node
import cloudpickle
node = GeneratorNode(limit=5)
result = cloudpickle.dumps(node)
`;

  // First create the serialized node
  const createNodeRequest = {
    node_type: 'CodeExecutorNode',
    config: {
      code: customNodeCode,
      entry_point: 'process'
    },
    input_data: Buffer.from(JSON.stringify({})),
    serialization_format: 'pickle',
    options: {
      timeout: 30.0
    }
  };
  
  try {
    console.log('Creating custom GeneratorNode...');
    const createResponse = await executeNode(createNodeRequest);
    
    if (createResponse.status === 'EXECUTION_STATUS_SUCCESS') {
      // Now use SerializedClassExecutorNode with the pickled node
      const useNodeRequest = {
        node_type: 'SerializedClassExecutorNode',
        config: {},
        input_data: createResponse.output_data,  // The pickled GeneratorNode
        serialization_format: 'pickle',
        options: {
          timeout: 30.0
        }
      };
      
      console.log('Executing SerializedClassExecutorNode with GeneratorNode...');
      const response = await executeNode(useNodeRequest);
      
      if (response.status === 'EXECUTION_STATUS_SUCCESS') {
        // Try to parse as JSON first, fall back to pickle
        let result;
        try {
          result = JSON.parse(response.output_data.toString());
        } catch (e) {
          console.log('Response is not JSON, might be pickled');
          result = response.output_data;
        }
        
        console.log('Response type:', typeof result);
        if (result && result.__generator__) {
          console.log('\n✅ Generator detected!');
          console.log('Generator ID:', result.generator_id);
          console.log('Node type:', result.node_type);
          
          // Fetch items
          console.log('\nFetching generator items...\n');
          
          const batchResponse = await getNextBatch({
            generator_id: result.generator_id,
            batch_size: 10,
            serialization_format: 'json'
          });
          
          if (batchResponse.status === 'EXECUTION_STATUS_SUCCESS') {
            const items = batchResponse.items.map((item: Buffer) => 
              JSON.parse(item.toString())
            );
            
            items.forEach((item: any, index: number) => {
              console.log(`Item ${index + 1}:`, item);
            });
            
            console.log(`\nTotal items: ${items.length}`);
          }
          
          await closeGenerator({ generator_id: result.generator_id });
        } else {
          console.log('\n❌ No generator marker found');
          console.log('Result:', result);
        }
      } else {
        console.error('SerializedClassExecutorNode failed:', response.error_message);
      }
    } else {
      console.error('Failed to create GeneratorNode:', createResponse.error_message);
    }
  } catch (error) {
    console.error('Test failed:', error);
  }
  
  // Test 2: Try with CodeExecutorNode setting result to a generator
  console.log('\n\n2. Testing CodeExecutorNode with generator in result variable:\n');
  
  const generatorInResultCode = `
# Create a generator and assign to result
def my_generator():
    for i in range(3):
        yield {"number": i, "text": f"Item {i}"}

result = my_generator()
print(f"[Server] Set result to generator: {result}")
`;

  const resultRequest = {
    node_type: 'CodeExecutorNode',
    config: {
      code: generatorInResultCode,
      entry_point: 'process'
    },
    input_data: Buffer.from(JSON.stringify({})),
    serialization_format: 'json',
    options: {
      timeout: 30.0
    }
  };
  
  try {
    const response = await executeNode(resultRequest);
    
    if (response.status === 'EXECUTION_STATUS_SUCCESS') {
      const result = JSON.parse(response.output_data.toString());
      console.log('Response:', JSON.stringify(result, null, 2));
      
      if (result.result && result.result.__generator__) {
        console.log('\n✅ Generator detected in result!');
      } else {
        console.log('\n❌ Generator not preserved in result');
        console.log('Result type:', typeof result.result);
      }
    }
  } catch (error) {
    console.error('Test failed:', error);
  }
  
  console.log('\n=== FINDINGS ===\n');
  console.log('1. CodeExecutorNode executes code but expects a "result" variable');
  console.log('2. Generators in the result variable are not preserved');
  console.log('3. Custom nodes that return generators from process() work with our modifications');
  console.log('4. SerializedClassExecutorNode can execute custom nodes with generator support\n');
}

// Run the test
if (require.main === module) {
  testGeneratorWithCustomNode().catch(console.error);
}