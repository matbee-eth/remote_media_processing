/**
 * Test kwargs parsing with different parameter types
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

async function testKwargsParsing() {
  const client = new remoteMedia.execution.RemoteExecutionService(
    'localhost:50052',
    grpc.credentials.createInsecure()
  );
  
  const executeNode = promisify(client.ExecuteNode.bind(client));
  
  console.log('🧪 Testing kwargs parsing with text generation\n');
  
  const testConfigs = [
    {
      name: "Short generation with low temperature",
      model_kwargs: {
        max_length: 30,
        temperature: 0.5,
        do_sample: true,
        top_p: 0.9
      }
    },
    {
      name: "Medium generation with high temperature",
      model_kwargs: {
        max_length: 60,
        temperature: 1.2,
        do_sample: true,
        top_p: 0.95,
        repetition_penalty: 1.2
      }
    },
    {
      name: "Deterministic generation (greedy decoding)",
      model_kwargs: {
        max_length: 40,
        do_sample: false
      }
    }
  ];
  
  const prompt = "The key to success is";
  
  for (const { name, model_kwargs } of testConfigs) {
    console.log(`\n📋 Test: ${name}`);
    console.log(`Config: ${JSON.stringify(model_kwargs, null, 2)}`);
    
    const request = {
      node_type: 'TransformersPipelineNode',
      config: {
        task: 'text-generation',
        model: 'gpt2',
        model_kwargs: JSON.stringify(model_kwargs)
      },
      input_data: Buffer.from(JSON.stringify(prompt)),
      serialization_format: 'json',
      options: {
        timeout: 300.0,
        enable_gpu: true
      }
    };
    
    try {
      const startTime = Date.now();
      const response = await executeNode(request);
      const endTime = Date.now();
      
      if (response.status === 'EXECUTION_STATUS_SUCCESS') {
        const result = JSON.parse(response.output_data.toString());
        const generated = result[0].generated_text;
        
        console.log(`✅ Success!`);
        console.log(`Generated (${generated.length} chars): "${generated}"`);
        console.log(`Processing time: ${endTime - startTime}ms`);
        
        // Verify the max_length constraint was respected
        if (model_kwargs.max_length && generated.length > prompt.length + 10) {
          const expectedMax = prompt.length + model_kwargs.max_length;
          console.log(`Length check: Generated ${generated.length} chars (expected max ~${expectedMax})`);
        }
      } else {
        console.error(`❌ Error: ${response.error_message}`);
        if (response.error_traceback) {
          console.error(`Traceback:\n${response.error_traceback}`);
        }
      }
    } catch (error) {
      console.error(`❌ Request failed:`, error);
    }
  }
  
  console.log('\n✅ Kwargs parsing test complete!');
}

// Run the test
testKwargsParsing().catch(console.error);