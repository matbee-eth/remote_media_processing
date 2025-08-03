/**
 * Test text generation with debug output
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { promisify } from 'util';
import * as path from 'path';

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

async function testGeneration() {
  const client = new remoteMedia.execution.RemoteExecutionService(
    'localhost:50052',
    grpc.credentials.createInsecure()
  );
  
  const executeNode = promisify(client.ExecuteNode.bind(client));
  
  const request = {
    node_type: 'TransformersPipelineNode',
    config: {
      task: 'text-generation',
      model: 'gpt2',
      model_kwargs: JSON.stringify({
        max_length: 50,
        num_return_sequences: 1,
        temperature: 0.8,
        do_sample: true,
        pad_token_id: 50256  // GPT2's EOS token
      })
    },
    input_data: Buffer.from(JSON.stringify("The future of AI is")),
    serialization_format: 'json',
    options: {
      timeout: 300.0,
      enable_gpu: true
    }
  };
  
  try {
    console.log('Sending request...');
    const response = await executeNode(request);
    
    console.log('Response status:', response.status);
    console.log('Response metrics:', response.metrics);
    
    if (response.output_data) {
      console.log('Raw output data:', response.output_data);
      console.log('Output as string:', response.output_data.toString());
      
      try {
        const parsed = JSON.parse(response.output_data.toString());
        console.log('Parsed output:', JSON.stringify(parsed, null, 2));
      } catch (e) {
        console.error('Failed to parse JSON:', e);
      }
    }
    
    if (response.error_message) {
      console.error('Error message:', response.error_message);
    }
  } catch (error) {
    console.error('Request failed:', error);
  }
}

testGeneration().catch(console.error);