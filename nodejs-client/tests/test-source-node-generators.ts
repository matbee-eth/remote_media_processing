/**
 * Test generator support with source nodes that naturally return generators
 * MediaReaderNode and similar source nodes return AsyncGenerators
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { promisify } from 'util';
import * as path from 'path';

const PROTO_PATH = path.join(__dirname, '..', '..', 'remote_service', 'protos', 'execution.proto');

async function testSourceNodeGenerators() {
  console.log('=== TEST SOURCE NODE GENERATORS ===\n');
  console.log('Testing nodes that naturally return generators (MediaReaderNode, etc.)\n');
  
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
  
  // Test 1: Create a simple custom generator node
  console.log('1. Testing with a custom DataGeneratorNode:\n');
  
  // Let's create a simple data generator node
  const customGeneratorCode = `
from remotemedia.core.node import Node
from typing import AsyncGenerator, Any

class DataGeneratorNode(Node):
    """A simple node that generates data asynchronously."""
    
    def __init__(self, name="DataGenerator", count=10, **kwargs):
        super().__init__(name=name, **kwargs)
        self.count = count
    
    async def process(self, data: Any = None) -> AsyncGenerator[Any, None]:
        """Process returns an async generator."""
        count = data.get('count', self.count) if isinstance(data, dict) else self.count
        
        print(f"[Server] DataGeneratorNode starting with count={count}")
        
        import asyncio
        for i in range(count):
            await asyncio.sleep(0.01)  # Simulate async work
            item = {
                'index': i,
                'value': f'Item {i}',
                'timestamp': asyncio.get_event_loop().time()
            }
            print(f"[Server] Yielding item {i}")
            yield item
        
        print("[Server] DataGeneratorNode complete")

# Test the node directly
async def test_generator():
    node = DataGeneratorNode(count=3)
    gen = await node.process({'count': 3})
    async for item in gen:
        print(f"Got: {item}")

# For the test, just create the node
result = "DataGeneratorNode created"
`;

  // First, let's test if we can detect a generator from a custom node
  const request = {
    node_type: 'CodeExecutorNode',
    config: {
      code: customGeneratorCode,
      entry_point: 'process'
    },
    input_data: Buffer.from(JSON.stringify({})),
    serialization_format: 'json',
    options: {
      timeout: 30.0
    }
  };
  
  try {
    console.log('Creating custom generator node...');
    const response = await executeNode(request);
    
    if (response.status === 'EXECUTION_STATUS_SUCCESS') {
      console.log('Custom node created successfully\n');
    }
  } catch (error) {
    console.error('Failed to create custom node:', error);
  }

  // Test 2: Let's create a simpler generator test
  console.log('2. Testing with a simple generator function:\n');
  
  // Create a node that just returns a generator
  const simpleGeneratorCode = `
# Simple test to see if generators are detected
def simple_generator():
    for i in range(3):
        yield {'number': i}

# In CodeExecutorNode, we need to set result
result = list(simple_generator())  # This materializes it
print(f"Result type: {type(result)}")
print(f"Result: {result}")
`;

  const simpleRequest = {
    node_type: 'CodeExecutorNode',
    config: {
      code: simpleGeneratorCode,
      entry_point: 'process'  
    },
    input_data: Buffer.from(JSON.stringify({})),
    serialization_format: 'json',
    options: {
      timeout: 30.0
    }
  };
  
  try {
    const response = await executeNode(simpleRequest);
    
    if (response.status === 'EXECUTION_STATUS_SUCCESS') {
      const result = JSON.parse(response.output_data.toString());
      console.log('Response:', JSON.stringify(result, null, 2));
    }
  } catch (error) {
    console.error('Simple generator test failed:', error);
  }

  // Test 3: Try using a different approach - create a custom streaming node
  console.log('\n3. Testing with a streaming approach:\n');
  
  const streamingCode = `
# Let's create a simple data source that could stream
import json

# For CodeExecutorNode, we simulate streaming by returning batches
batch_size = 2
total_items = 5

items = []
for i in range(total_items):
    items.append({
        'id': i,
        'message': f'Stream item {i}',
        'batch': i // batch_size
    })

# Return first batch
result = {
    'items': items[:batch_size],
    'has_more': len(items) > batch_size,
    'total': total_items
}
print(f"Returning batch: {result}")
`;

  const streamRequest = {
    node_type: 'CodeExecutorNode',
    config: {
      code: streamingCode,
      entry_point: 'process'
    },
    input_data: Buffer.from(JSON.stringify({})),
    serialization_format: 'json',
    options: {
      timeout: 30.0
    }
  };
  
  try {
    const response = await executeNode(streamRequest);
    
    if (response.status === 'EXECUTION_STATUS_SUCCESS') {
      const result = JSON.parse(response.output_data.toString());
      console.log('Streaming simulation response:', JSON.stringify(result.result, null, 2));
    }
  } catch (error) {
    console.error('Streaming test failed:', error);
  }

  console.log('\n=== FINDINGS ===\n');
  console.log('1. CodeExecutorNode is limited - it expects a "result" variable');
  console.log('2. Source nodes like MediaReaderNode naturally return AsyncGenerators');
  console.log('3. With our server modifications:');
  console.log('   - Nodes that return generators will have them detected');
  console.log('   - Generator markers will be returned instead of materialized data');
  console.log('   - Clients can stream data using GetNextBatch');
  console.log('\n4. To test generator support properly, we need:');
  console.log('   - A custom node that extends Node and returns a generator from process()');
  console.log('   - Or use existing source nodes like MediaReaderNode');
  console.log('   - Or create a new node type specifically for testing\n');
}

// Run the test
if (require.main === module) {
  testSourceNodeGenerators().catch(console.error);
}