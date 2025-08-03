/**
 * Debug version to understand the response format
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { promisify } from 'util';
import * as path from 'path';

// Load protobuf
const PROTO_PATH = path.join(__dirname, '..', '..', 'remote_service', 'protos', 'execution.proto');
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
  includeDirs: [path.join(__dirname, '..', '..', 'remote_service', 'protos')]
});

const remoteMedia = grpc.loadPackageDefinition(packageDefinition).remotemedia as any;

async function debugSentiment() {
  const client = new remoteMedia.execution.RemoteExecutionService(
    'localhost:50052',
    grpc.credentials.createInsecure()
  );
  
  const executeNode = promisify(client.ExecuteNode.bind(client));
  
  // Simple test with CalculatorNode first
  console.log('Testing with CalculatorNode...\n');
  
  const calcRequest = {
    node_type: 'CalculatorNode',
    config: {
      operation: 'multiply',
      factor: '2'
    },
    input_data: Buffer.from(JSON.stringify({ value: 10 })),
    serialization_format: 'json',
    options: {
      timeout: 30.0
    }
  };
  
  try {
    const response = await executeNode(calcRequest);
    console.log('Full response:', JSON.stringify(response, null, 2));
    
    if (response.output_data) {
      console.log('Output data:', response.output_data.toString());
      const parsed = JSON.parse(response.output_data.toString());
      console.log('Parsed output:', parsed);
    }
  } catch (error) {
    console.error('Error:', error);
  }
  
  // Now try with CodeExecutorNode
  console.log('\n\nTesting with CodeExecutorNode...\n');
  
  const codeRequest = {
    node_type: 'CodeExecutorNode',
    config: {
      code: 'async def process(data): return {"result": data * 2, "type": type(data).__name__}',
      entry_point: 'process'
    },
    input_data: Buffer.from(JSON.stringify(42)),
    serialization_format: 'json',
    options: {
      timeout: 30.0
    }
  };
  
  try {
    const response = await executeNode(codeRequest);
    console.log('Full response:', JSON.stringify(response, null, 2));
    
    if (response.output_data) {
      console.log('Output data:', response.output_data.toString());
      const parsed = JSON.parse(response.output_data.toString());
      console.log('Parsed output:', parsed);
    }
  } catch (error) {
    console.error('Error:', error);
  }
}

// Run debug
debugSentiment().catch(console.error);