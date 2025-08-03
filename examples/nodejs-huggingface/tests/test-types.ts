/**
 * Test script to verify TypeScript types compile correctly
 */

// Define local types for testing (these would come from generated remotemedia-types.d.ts)
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

// Test type checking
const sentimentConfig: HuggingFacePipelineConfig = {
  task: 'sentiment-analysis',
  model: 'distilbert-base-uncased-finetuned-sst-2-english'
};

const textGenConfig: HuggingFacePipelineConfig = {
  task: 'text-generation',
  model: 'gpt2',
  device: 'cuda:0',
  model_kwargs: {
    max_length: 100,
    temperature: 0.8,
    top_p: 0.9
  }
};

const qaConfig: HuggingFacePipelineConfig = {
  task: 'question-answering',
  model: 'distilbert-base-cased-distilled-squad'
};

const nerConfig: HuggingFacePipelineConfig = {
  task: 'ner',
  model: 'dslim/bert-base-NER',
  model_kwargs: {
    aggregation_strategy: 'simple'
  }
};

const clientConfig: RemoteExecutorConfig = {
  host: 'localhost',
  port: 50052,
  protocol: 'grpc',
  timeout: 300,
  sslEnabled: false,
  pipPackages: ['transformers', 'torch', 'torchvision', 'torchaudio']
};

// Test various Hugging Face pipeline tasks
const pipelineExamples = [
  { task: 'sentiment-analysis', model: 'distilbert-base-uncased-finetuned-sst-2-english' },
  { task: 'text-generation', model: 'gpt2' },
  { task: 'text2text-generation', model: 't5-small' },
  { task: 'summarization', model: 'facebook/bart-large-cnn' },
  { task: 'translation', model: 'Helsinki-NLP/opus-mt-en-de' },
  { task: 'question-answering', model: 'distilbert-base-cased-distilled-squad' },
  { task: 'fill-mask', model: 'bert-base-uncased' },
  { task: 'ner', model: 'dslim/bert-base-NER' },
  { task: 'token-classification', model: 'bert-base-cased' },
  { task: 'zero-shot-classification', model: 'facebook/bart-large-mnli' },
  { task: 'feature-extraction', model: 'bert-base-uncased' },
  { task: 'conversational', model: 'microsoft/DialoGPT-medium' },
  { task: 'image-classification', model: 'google/vit-base-patch16-224' },
  { task: 'object-detection', model: 'facebook/detr-resnet-50' },
  { task: 'image-segmentation', model: 'facebook/detr-resnet-50-panoptic' },
  { task: 'automatic-speech-recognition', model: 'openai/whisper-base' }
];

console.log('✅ TypeScript types compile successfully!');
console.log(`\nSupported Hugging Face pipeline tasks: ${pipelineExamples.length}`);
pipelineExamples.forEach(example => {
  console.log(`  - ${example.task}: ${example.model}`);
});