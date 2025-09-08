/**
 * Simple text generation test without model_kwargs
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

async function simpleGeneration() {
  const client = new remoteMedia.execution.RemoteExecutionService(
    'localhost:50052',
    grpc.credentials.createInsecure()
  );
  
  const executeNode = promisify(client.ExecuteNode.bind(client));
  
  console.log('🤖 Simple Text Generation Test\n');
  
  const prompts = [
    "The future of artificial intelligence is",
    "Once upon a time",
    "In the year 2050"
  ];
  
  for (const prompt of prompts) {
    const request = {
      node_type: 'TransformersPipelineNode',
      config: {
        task: 'text-generation',
        model: 'gpt2'
        // Using defaults for model_kwargs
      },
      input_data: Buffer.from(JSON.stringify(prompt)),
      serialization_format: 'json',
      options: {
        timeout: 300.0,
        enable_gpu: true
      }
    };
    
    try {
      console.log(`Prompt: "${prompt}"`);
      const response = await executeNode(request);
      
      if (response.status === 'EXECUTION_STATUS_SUCCESS') {
        const result = JSON.parse(response.output_data.toString());
        console.log(`Generated: "${result[0].generated_text}"`);
        console.log(`Processing time: ${response.metrics?.duration_ms || 'N/A'}ms\n`);
      } else {
        console.error(`Error: ${response.error_message}\n`);
      }
    } catch (error) {
      console.error('Request failed:', error, '\n');
    }
  }
}

simpleGeneration().catch(console.error);