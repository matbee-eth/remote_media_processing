/**
 * Hugging Face Pipeline Remote Execution Example
 * 
 * This example demonstrates how to execute Hugging Face transformers pipelines
 * remotely using the RemoteMedia Processing SDK from Node.js.
 * 
 * Examples include:
 * - Sentiment analysis
 * - Text generation
 * - Question answering
 * - Image classification
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { promisify } from 'util';
import * as fs from 'fs/promises';
import * as path from 'path';

// Define types for Hugging Face pipeline configurations
interface HuggingFacePipelineConfig {
  task: string;
  model?: string;
  device?: string | number;
  model_kwargs?: Record<string, any>;
}

interface RemoteExecutorConfig {
  host: string;
  port: number;
  protocol?: 'grpc' | 'http';
  authToken?: string;
  timeout?: number;
  maxRetries?: number;
  sslEnabled?: boolean;
  pipPackages?: string[];
}

interface ExecutionResponse<T = any> {
  status: 'success' | 'error';
  data?: T;
  error?: {
    message: string;
    traceback?: string;
  };
  metrics?: {
    startTimestamp: number;
    endTimestamp: number;
    durationMs: number;
    memoryPeakMb?: number;
    cpuTimeMs?: number;
  };
}

// Load protobuf definitions
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

/**
 * RemoteMedia client for executing Hugging Face pipelines
 */
class HuggingFaceRemoteClient {
  private client: any;
  private config: RemoteExecutorConfig;

  constructor(config: RemoteExecutorConfig) {
    this.config = config;
    const address = `${config.host}:${config.port}`;
    
    if (config.sslEnabled) {
      const credentials = grpc.credentials.createSsl();
      this.client = new remoteMedia.execution.RemoteExecutionService(address, credentials);
    } else {
      this.client = new remoteMedia.execution.RemoteExecutionService(
        address,
        grpc.credentials.createInsecure()
      );
    }
  }

  /**
   * Execute a Hugging Face pipeline remotely
   */
  async executePipeline<T = any>(
    pipelineConfig: HuggingFacePipelineConfig,
    inputData: any
  ): Promise<ExecutionResponse<T>> {
    const executeNode = promisify(this.client.ExecuteNode.bind(this.client));
    
    // Serialize input data as JSON
    const serializedInput = Buffer.from(JSON.stringify(inputData));
    
    // Convert config to string map for gRPC
    const configMap: Record<string, string> = {
      task: pipelineConfig.task
    };
    
    if (pipelineConfig.model) {
      configMap.model = pipelineConfig.model;
    }
    
    if (pipelineConfig.device !== undefined) {
      // Device can be a string ('cpu', 'cuda') or number (0, 1, etc)
      configMap.device = String(pipelineConfig.device);
    }
    
    if (pipelineConfig.model_kwargs) {
      // Convert model_kwargs to JSON string for gRPC transport
      configMap.model_kwargs = JSON.stringify(pipelineConfig.model_kwargs);
    }
    
    const request = {
      node_type: 'TransformersPipelineNode',
      config: configMap,
      input_data: serializedInput,
      serialization_format: 'json',
      options: {
        timeout: this.config.timeout || 300.0, // Longer timeout for model loading
        enable_gpu: true
      }
    };
    
    try {
      console.log(`🚀 Executing ${pipelineConfig.task} pipeline...`);
      const response = await executeNode(request);
      
      // Deserialize output
      const outputData = JSON.parse(response.output_data.toString());
      
      return {
        status: response.status === 'EXECUTION_STATUS_SUCCESS' ? 'success' : 'error',
        data: outputData,
        error: response.error_message ? {
          message: response.error_message,
          traceback: response.error_traceback
        } : undefined,
        metrics: response.metrics ? {
          startTimestamp: parseInt(response.metrics.start_timestamp),
          endTimestamp: parseInt(response.metrics.end_timestamp),
          durationMs: parseInt(response.metrics.duration_ms),
          memoryPeakMb: response.metrics.memory_peak_mb,
          cpuTimeMs: response.metrics.cpu_time_ms
        } : undefined
      };
    } catch (error: any) {
      return {
        status: 'error',
        error: {
          message: error.message || 'Unknown error',
          traceback: error.stack
        }
      };
    }
  }

  /**
   * Stream data through a Hugging Face pipeline
   */
  streamPipeline(
    pipelineConfig: HuggingFacePipelineConfig,
    onData: (data: any) => void,
    onError?: (error: Error) => void
  ): StreamHandle {
    const stream = this.client.StreamNode();
    
    // Convert config for streaming
    const configMap: Record<string, string> = {
      task: pipelineConfig.task
    };
    
    if (pipelineConfig.model) {
      configMap.model = pipelineConfig.model;
    }
    
    if (pipelineConfig.device !== undefined) {
      // Device can be a string ('cpu', 'cuda') or number (0, 1, etc)
      configMap.device = String(pipelineConfig.device);
    }
    
    if (pipelineConfig.model_kwargs) {
      // Convert model_kwargs to JSON string for gRPC transport
      configMap.model_kwargs = JSON.stringify(pipelineConfig.model_kwargs);
    }
    
    // Send initialization
    stream.write({
      init: {
        node_type: 'TransformersPipelineNode',
        config: configMap,
        serialization_format: 'json'
      }
    });
    
    // Handle responses
    stream.on('data', (response: any) => {
      if (response.error_message) {
        if (onError) {
          onError(new Error(response.error_message));
        }
      } else if (response.data) {
        try {
          const data = JSON.parse(response.data.toString());
          onData(data);
        } catch (e) {
          if (onError) {
            onError(e as Error);
          }
        }
      }
    });
    
    stream.on('error', (error: Error) => {
      if (onError) {
        onError(error);
      }
    });
    
    return {
      send: async (data: any) => {
        const serialized = Buffer.from(JSON.stringify(data));
        stream.write({ data: serialized });
      },
      close: async () => {
        stream.end();
      },
      sessionId: 'stream-' + Date.now()
    };
  }

  async close(): Promise<void> {
    return Promise.resolve();
  }
}

interface StreamHandle {
  send(data: any): Promise<void>;
  close(): Promise<void>;
  readonly sessionId: string;
}

// Example usage functions

/**
 * Example 1: Sentiment Analysis
 */
async function sentimentAnalysisExample(client: HuggingFaceRemoteClient) {
  console.log('\n📊 Example 1: Sentiment Analysis');
  console.log('================================');
  
  const texts = [
    "I love this product! It's amazing and works perfectly.",
    "This is terrible. Completely disappointed with the quality.",
    "It's okay, nothing special but does the job.",
    "Best purchase I've made this year! Highly recommend to everyone!"
  ];
  
  const config: HuggingFacePipelineConfig = {
    task: 'sentiment-analysis',
    model: 'distilbert-base-uncased-finetuned-sst-2-english'
  };
  
  for (const text of texts) {
    console.log(`\nAnalyzing: "${text}"`);
    const result = await client.executePipeline(config, text);
    
    if (result.status === 'success') {
      const sentiment = result.data[0];
      console.log(`  Sentiment: ${sentiment.label} (confidence: ${(sentiment.score * 100).toFixed(2)}%)`);
      console.log(`  Processing time: ${result.metrics?.durationMs}ms`);
    } else {
      console.error(`  Error: ${result.error?.message}`);
    }
  }
}

/**
 * Example 2: Text Generation
 */
async function textGenerationExample(client: HuggingFaceRemoteClient) {
  console.log('\n✍️  Example 2: Text Generation');
  console.log('=============================');
  
  const prompts = [
    "The future of artificial intelligence is",
    "Once upon a time in a distant galaxy",
    "The most important invention of the 21st century is"
  ];
  
  const config: HuggingFacePipelineConfig = {
    task: 'text-generation',
    model: 'gpt2',
    model_kwargs: {
      max_length: 50,
      num_return_sequences: 1,
      temperature: 0.8
    }
  };
  
  for (const prompt of prompts) {
    console.log(`\nPrompt: "${prompt}"`);
    const result = await client.executePipeline(config, prompt);
    
    if (result.status === 'success') {
      const generated = result.data[0].generated_text;
      console.log(`  Generated: "${generated}"`);
      console.log(`  Processing time: ${result.metrics?.durationMs}ms`);
    } else {
      console.error(`  Error: ${result.error?.message}`);
    }
  }
}

/**
 * Example 3: Question Answering
 */
async function questionAnsweringExample(client: HuggingFaceRemoteClient) {
  console.log('\n❓ Example 3: Question Answering');
  console.log('================================');
  
  const context = `The RemoteMedia Processing SDK is a Python SDK for building distributed 
  audio/video/data processing pipelines with transparent remote offloading capabilities. 
  It was developed to enable developers to create complex, real-time processing applications 
  that can seamlessly offload computationally intensive tasks to remote execution services. 
  The SDK supports WebRTC integration, CloudPickle serialization, and automatic dependency packaging.`;
  
  const questions = [
    "What is RemoteMedia Processing SDK?",
    "What capabilities does the SDK have?",
    "What serialization method is supported?"
  ];
  
  const config: HuggingFacePipelineConfig = {
    task: 'question-answering',
    model: 'distilbert-base-cased-distilled-squad'
  };
  
  for (const question of questions) {
    console.log(`\nQuestion: "${question}"`);
    const result = await client.executePipeline(config, { question, context });
    
    if (result.status === 'success') {
      console.log(`  Answer: "${result.data.answer}"`);
      console.log(`  Confidence: ${(result.data.score * 100).toFixed(2)}%`);
      console.log(`  Processing time: ${result.metrics?.durationMs}ms`);
    } else {
      console.error(`  Error: ${result.error?.message}`);
    }
  }
}

/**
 * Example 4: Zero-Shot Classification
 */
async function zeroShotClassificationExample(client: HuggingFaceRemoteClient) {
  console.log('\n🎯 Example 4: Zero-Shot Classification');
  console.log('======================================');
  
  const texts = [
    "The new smartphone features a powerful camera and long battery life",
    "The stock market saw significant gains today amid positive earnings reports",
    "Scientists discovered a new species of butterfly in the Amazon rainforest"
  ];
  
  const candidate_labels = ["technology", "business", "science", "sports", "entertainment"];
  
  const config: HuggingFacePipelineConfig = {
    task: 'zero-shot-classification',
    model: 'facebook/bart-large-mnli'
  };
  
  for (const text of texts) {
    console.log(`\nClassifying: "${text.substring(0, 50)}..."`);
    const result = await client.executePipeline(config, {
      sequences: text,
      candidate_labels: candidate_labels
    });
    
    if (result.status === 'success') {
      const labels = result.data.labels;
      const scores = result.data.scores;
      
      console.log('  Classifications:');
      for (let i = 0; i < Math.min(3, labels.length); i++) {
        console.log(`    ${labels[i]}: ${(scores[i] * 100).toFixed(2)}%`);
      }
      console.log(`  Processing time: ${result.metrics?.durationMs}ms`);
    } else {
      console.error(`  Error: ${result.error?.message}`);
    }
  }
}

/**
 * Example 5: Named Entity Recognition (Streaming)
 */
async function nerStreamingExample(client: HuggingFaceRemoteClient) {
  console.log('\n🏷️  Example 5: Named Entity Recognition (Streaming)');
  console.log('=================================================');
  
  const config: HuggingFacePipelineConfig = {
    task: 'ner',
    model: 'dslim/bert-base-NER',
    model_kwargs: {
      aggregation_strategy: 'simple'
    }
  };
  
  const texts = [
    "Apple Inc. was founded by Steve Jobs in Cupertino, California.",
    "The Eiffel Tower in Paris attracts millions of tourists each year.",
    "Microsoft CEO Satya Nadella announced new AI features at the conference in Seattle."
  ];
  
  let processedCount = 0;
  
  const stream = client.streamPipeline(
    config,
    (result) => {
      processedCount++;
      console.log(`\nText ${processedCount}: Found ${result.length} entities`);
      result.forEach((entity: any) => {
        console.log(`  - ${entity.entity_group}: "${entity.word}" (confidence: ${(entity.score * 100).toFixed(2)}%)`);
      });
    },
    (error) => {
      console.error('Stream error:', error);
    }
  );
  
  // Send texts through the stream
  for (const text of texts) {
    console.log(`\nSending: "${text}"`);
    await stream.send(text);
    await new Promise(resolve => setTimeout(resolve, 500)); // Small delay between items
  }
  
  await stream.close();
  console.log('\n✅ Streaming completed');
}

/**
 * Main function to run all examples
 */
async function main() {
  // Configure the client
  const config: RemoteExecutorConfig = {
    host: process.env.REMOTE_HOST || 'localhost',
    port: parseInt(process.env.REMOTE_PORT || '50052'),
    protocol: 'grpc',
    timeout: 300, // 5 minutes for model loading
    sslEnabled: false,
    pipPackages: ['transformers', 'torch', 'torchvision'] // Ensure these are installed on remote
  };
  
  console.log('🤖 Hugging Face Pipeline Remote Execution Examples');
  console.log('==================================================');
  console.log(`Connecting to ${config.host}:${config.port}...`);
  
  const client = new HuggingFaceRemoteClient(config);
  
  try {
    // Run examples based on command line argument
    const example = process.argv[2];
    
    switch (example) {
      case 'sentiment':
        await sentimentAnalysisExample(client);
        break;
      case 'generation':
        await textGenerationExample(client);
        break;
      case 'qa':
        await questionAnsweringExample(client);
        break;
      case 'classification':
        await zeroShotClassificationExample(client);
        break;
      case 'ner':
        await nerStreamingExample(client);
        break;
      case 'all':
      default:
        // Run all examples
        await sentimentAnalysisExample(client);
        await textGenerationExample(client);
        await questionAnsweringExample(client);
        await zeroShotClassificationExample(client);
        await nerStreamingExample(client);
        break;
    }
    
    console.log('\n🎉 All examples completed successfully!');
    
  } catch (error) {
    console.error('Fatal error:', error);
  } finally {
    await client.close();
  }
}

// Run the examples
if (require.main === module) {
  console.log('\nUsage: ts-node huggingface-pipeline-client.ts [example]');
  console.log('Examples: sentiment, generation, qa, classification, ner, all\n');
  
  main().catch(console.error);
}