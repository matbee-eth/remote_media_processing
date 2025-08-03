/**
 * Simple Sentiment Analysis Example
 * 
 * This is a minimal example showing how to perform sentiment analysis
 * on text using a remote Hugging Face pipeline.
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

async function analyzeSentiment(text: string) {
  // Create gRPC client
  const client = new remoteMedia.execution.RemoteExecutionService(
    'localhost:50052',
    grpc.credentials.createInsecure()
  );
  
  const executeNode = promisify(client.ExecuteNode.bind(client));
  
  // Configure sentiment analysis pipeline
  const request = {
    node_type: 'TransformersPipelineNode',
    config: {
      task: 'sentiment-analysis',
      model: 'distilbert-base-uncased-finetuned-sst-2-english'
    },
    input_data: Buffer.from(JSON.stringify(text)),
    serialization_format: 'json',
    options: {
      timeout: 60.0,
      enable_gpu: true
    }
  };
  
  try {
    // Execute on remote server
    console.log(`Analyzing: "${text}"`);
    const response = await executeNode(request);
    
    if (response.status === 'EXECUTION_STATUS_SUCCESS') {
      const result = JSON.parse(response.output_data.toString());
      const sentiment = result[0];
      
      console.log(`✅ Sentiment: ${sentiment.label}`);
      console.log(`   Confidence: ${(sentiment.score * 100).toFixed(2)}%`);
      console.log(`   Processing time: ${response.metrics.duration_ms}ms`);
      
      return sentiment;
    } else {
      console.error(`❌ Error: ${response.error_message}`);
      return null;
    }
  } catch (error) {
    console.error('Failed to analyze sentiment:', error);
    return null;
  }
}

// Example usage
async function main() {
  console.log('🤖 Sentiment Analysis Example\n');
  
  const texts = [
    "I absolutely love this new feature! It's incredible!",
    "This is the worst experience I've ever had.",
    "It's okay, nothing special but it works.",
    "Amazing! Best decision I've made all year!",
    "Terrible quality, completely disappointed."
  ];
  
  for (const text of texts) {
    await analyzeSentiment(text);
    console.log(''); // Empty line between results
  }
}

// Run the example
if (require.main === module) {
  main().catch(console.error);
}