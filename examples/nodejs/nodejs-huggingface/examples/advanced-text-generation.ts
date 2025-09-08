/**
 * Advanced Text Generation with Custom Parameters
 * 
 * This example demonstrates how to use model_kwargs and other
 * configuration options with the Hugging Face pipeline.
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { promisify } from 'util';
import * as path from 'path';

// Type definitions
interface GenerationConfig {
  max_length?: number;
  min_length?: number;
  temperature?: number;
  top_p?: number;
  top_k?: number;
  do_sample?: boolean;
  num_return_sequences?: number;
  repetition_penalty?: number;
  length_penalty?: number;
  early_stopping?: boolean;
  pad_token_id?: number;
  eos_token_id?: number;
  no_repeat_ngram_size?: number;
}

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

class HuggingFaceGenerator {
  private client: any;
  private executeNode: any;

  constructor(host: string = 'localhost', port: number = 50052) {
    this.client = new remoteMedia.execution.RemoteExecutionService(
      `${host}:${port}`,
      grpc.credentials.createInsecure()
    );
    this.executeNode = promisify(this.client.ExecuteNode.bind(this.client));
  }

  async generate(
    prompt: string,
    model: string = 'gpt2',
    generationConfig?: GenerationConfig
  ): Promise<string[]> {
    const config: Record<string, string> = {
      task: 'text-generation',
      model: model
    };

    // Add model_kwargs if provided
    if (generationConfig) {
      config.model_kwargs = JSON.stringify(generationConfig);
    }

    const request = {
      node_type: 'TransformersPipelineNode',
      config: config,
      input_data: Buffer.from(JSON.stringify(prompt)),
      serialization_format: 'json',
      options: {
        timeout: 300.0,
        enable_gpu: true
      }
    };

    const response = await this.executeNode(request);
    
    if (response.status !== 'EXECUTION_STATUS_SUCCESS') {
      throw new Error(response.error_message || 'Generation failed');
    }

    const result = JSON.parse(response.output_data.toString());
    return result.map((r: any) => r.generated_text);
  }
}

// Example functions

async function creativeWriting() {
  console.log('📝 Creative Writing Example');
  console.log('==========================\n');

  const generator = new HuggingFaceGenerator();
  
  const prompt = "In a world where AI and humans coexist,";
  
  // High temperature for creative output
  const config: GenerationConfig = {
    max_length: 100,
    temperature: 1.2,
    top_p: 0.95,
    do_sample: true,
    num_return_sequences: 3,
    repetition_penalty: 1.2
  };

  console.log(`Prompt: "${prompt}"\n`);
  console.log('Generating 3 creative continuations...\n');
  
  const results = await generator.generate(prompt, 'gpt2', config);
  
  results.forEach((text, i) => {
    console.log(`Version ${i + 1}:`);
    console.log(text);
    console.log('---\n');
  });
}

async function technicalWriting() {
  console.log('🔧 Technical Writing Example');
  console.log('============================\n');

  const generator = new HuggingFaceGenerator();
  
  const prompt = "To implement a distributed system, you must first";
  
  // Low temperature for focused, technical output
  const config: GenerationConfig = {
    max_length: 80,
    temperature: 0.7,
    top_p: 0.9,
    do_sample: true,
    repetition_penalty: 1.1,
    no_repeat_ngram_size: 3
  };

  console.log(`Prompt: "${prompt}"\n`);
  
  const [result] = await generator.generate(prompt, 'gpt2', config);
  console.log(result);
}

async function storyGeneration() {
  console.log('📚 Story Generation Example');
  console.log('===========================\n');

  const generator = new HuggingFaceGenerator();
  
  const prompts = [
    "The ancient door creaked open, revealing",
    "She had always known she was different, but when",
    "The last human on Mars looked at Earth and"
  ];

  for (const prompt of prompts) {
    const config: GenerationConfig = {
      max_length: 150,
      min_length: 50,
      temperature: 0.9,
      top_p: 0.92,
      do_sample: true,
      repetition_penalty: 1.15,
      length_penalty: 1.0,
      early_stopping: false
    };

    console.log(`📖 Story starter: "${prompt}"\n`);
    
    const [story] = await generator.generate(prompt, 'gpt2', config);
    console.log(story);
    console.log('\n' + '='.repeat(60) + '\n');
  }
}

async function comparisonExample() {
  console.log('🔬 Parameter Comparison Example');
  console.log('===============================\n');

  const generator = new HuggingFaceGenerator();
  const prompt = "Artificial intelligence will";

  const configs = [
    {
      name: "Conservative (temp=0.5)",
      config: { max_length: 50, temperature: 0.5, do_sample: true }
    },
    {
      name: "Balanced (temp=0.8)",
      config: { max_length: 50, temperature: 0.8, do_sample: true }
    },
    {
      name: "Creative (temp=1.5)",
      config: { max_length: 50, temperature: 1.5, do_sample: true }
    },
    {
      name: "Deterministic (greedy)",
      config: { max_length: 50, do_sample: false }
    }
  ];

  console.log(`Base prompt: "${prompt}"\n`);

  for (const { name, config } of configs) {
    console.log(`${name}:`);
    const [result] = await generator.generate(prompt, 'gpt2', config);
    console.log(result);
    console.log();
  }
}

// Main function
async function main() {
  console.log('🤖 Advanced Hugging Face Text Generation\n');
  
  const example = process.argv[2] || 'all';
  
  try {
    switch (example) {
      case 'creative':
        await creativeWriting();
        break;
      case 'technical':
        await technicalWriting();
        break;
      case 'story':
        await storyGeneration();
        break;
      case 'compare':
        await comparisonExample();
        break;
      case 'all':
        await creativeWriting();
        console.log('\n' + '='.repeat(80) + '\n');
        await technicalWriting();
        console.log('\n' + '='.repeat(80) + '\n');
        await storyGeneration();
        console.log('\n' + '='.repeat(80) + '\n');
        await comparisonExample();
        break;
      default:
        console.log('Usage: ts-node advanced-generation.ts [example]');
        console.log('Examples: creative, technical, story, compare, all');
    }
  } catch (error) {
    console.error('Error:', error);
  }
}

// Run the example
if (require.main === module) {
  main().catch(console.error);
}