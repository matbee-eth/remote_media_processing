/**
 * RemoteMedia Hugging Face Examples Index
 * 
 * This directory contains Node.js/TypeScript examples for using Hugging Face
 * transformers models with the RemoteMedia Processing SDK.
 * 
 * Directory Structure:
 * 
 * examples/
 *   - full-featured-client.ts     : Complete example with 5 different NLP tasks
 *   - sentiment-analysis.ts       : Simple sentiment analysis example
 *   - text-generation.ts          : Basic text generation with GPT-2
 *   - advanced-text-generation.ts : Advanced generation with custom parameters
 * 
 * examples/proxy/
 *   - simple-proxy-example.ts     : Python-like RemoteProxyClient API demo
 *   - calculator-proxy.ts         : Calculator example with remote execution
 * 
 * src/
 *   - remote-proxy-client.ts      : RemoteProxyClient implementation
 * 
 * tests/
 *   - test-kwargs.ts              : Tests for model_kwargs parameter parsing
 *   - test-generation.ts          : Debug text generation responses
 *   - test-types.ts               : TypeScript type checking tests
 *   - debug-response-format.ts    : Debug tool for understanding gRPC responses
 * 
 * utils/
 *   - proxy-client-example.ts     : Alternative implementation using RemoteProxyClient
 *   - sentiment_analyzer.py       : Python code for proxy client example
 * 
 * Quick Start:
 * 
 * 1. Install dependencies:
 *    npm install
 * 
 * 2. Run examples:
 *    npm run sentiment        # Run sentiment analysis
 *    npm run generation       # Run text generation
 *    npm run all             # Run all examples
 * 
 * Or use the shell script:
 *    ./run-example.sh sentiment
 * 
 * See README.md for detailed documentation.
 */

// Export types for use in other projects
export interface HuggingFacePipelineConfig {
  task: string;
  model?: string;
  device?: string | number;
  model_kwargs?: Record<string, any>;
}

export interface RemoteExecutorConfig {
  host: string;
  port: number;
  protocol?: 'grpc' | 'http';
  authToken?: string;
  timeout?: number;
  maxRetries?: number;
  sslEnabled?: boolean;
  pipPackages?: string[];
}

// Example runner
if (require.main === module) {
  console.log('📚 RemoteMedia Hugging Face Examples');
  console.log('===================================\n');
  console.log('Available examples:\n');
  console.log('1. Sentiment Analysis:');
  console.log('   npx ts-node examples/sentiment-analysis.ts\n');
  console.log('2. Text Generation:');
  console.log('   npx ts-node examples/text-generation.ts\n');
  console.log('3. Advanced Generation:');
  console.log('   npx ts-node examples/advanced-text-generation.ts\n');
  console.log('4. Full Featured Demo:');
  console.log('   npx ts-node examples/full-featured-client.ts all\n');
  console.log('Or use npm scripts: npm run sentiment, npm run generation, etc.');
}