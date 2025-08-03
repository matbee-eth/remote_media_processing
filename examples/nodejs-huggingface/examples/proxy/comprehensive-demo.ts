/**
 * Comprehensive RemoteProxyClient Demo
 * 
 * This example demonstrates the full capabilities of the RemoteProxyClient API
 * for executing server nodes remotely from Node.js/TypeScript.
 */

import { 
  RemoteProxyClient, 
  RemoteExecutorConfig, 
  withRemoteProxy,
  RemoteNodes 
} from '../../src/remote-proxy-client-v2';

async function main() {
  console.log('🎯 RemoteProxyClient Comprehensive Demo');
  console.log('=====================================\n');
  
  const config: RemoteExecutorConfig = {
    host: "localhost",
    port: 50052
  };

  // Method 1: Using async/await with manual connection management
  console.log('📌 Method 1: Manual Connection Management');
  console.log('----------------------------------------');
  
  const client = new RemoteProxyClient(config);
  await client.connect();
  
  try {
    // Create a sentiment analyzer
    const sentimentAnalyzer = await client.createNodeProxy(
      'TransformersPipelineNode',
      {
        task: 'sentiment-analysis',
        model: 'distilbert-base-uncased-finetuned-sst-2-english'
      }
    );
    
    const result = await sentimentAnalyzer.process("This RemoteProxyClient API is fantastic!");
    console.log('Sentiment Analysis Result:', result);
  } finally {
    await client.close();
  }
  
  // Method 2: Using withRemoteProxy helper (Python-style context manager)
  console.log('\n📌 Method 2: Python-style Context Manager');
  console.log('----------------------------------------');
  
  await withRemoteProxy(config, async (client) => {
    // Text generation example
    const textGen = await client.createNodeProxy(
      'TransformersPipelineNode',
      {
        task: 'text-generation',
        model: 'gpt2',
        model_kwargs: {
          max_length: 50,
          temperature: 0.7,
          do_sample: true
        }
      }
    );
    
    const generated = await textGen.process("The RemoteProxyClient enables");
    console.log('Generated Text:', generated[0].generated_text);
    
    // Calculator example
    const calculator = await client.createNodeProxy('CalculatorNode');
    const calcResult = await calculator.process({
      operation: 'multiply',
      args: [42, 3.14]
    });
    console.log('\nCalculator Result:', calcResult);
  });
  
  // Method 3: Using RemoteNodes helper class
  console.log('\n📌 Method 3: RemoteNodes Helper Class');
  console.log('------------------------------------');
  
  await withRemoteProxy(config, async (client) => {
    const nodes = new RemoteNodes(client);
    
    // Use convenient helper methods
    const pipeline = await nodes.transformersPipeline({
      task: 'question-answering',
      model: 'distilbert-base-cased-distilled-squad'
    });
    
    const qaResult = await pipeline.process({
      question: "What is the RemoteProxyClient?",
      context: "The RemoteProxyClient is a TypeScript API that allows you to execute any registered server node remotely. It provides a transparent proxy pattern similar to Python's RemoteProxyClient, making distributed computing seamless."
    });
    
    console.log('Question Answering Result:', qaResult);
  });
  
  // Method 4: Advanced usage with multiple nodes
  console.log('\n📌 Method 4: Pipeline-like Processing');
  console.log('------------------------------------');
  
  await withRemoteProxy(config, async (client) => {
    // Step 1: Process text
    const textProcessor = await client.createNodeProxy('TextProcessorNode');
    const processedText = await textProcessor.process({
      text: "hello world from remotemedia sdk",
      operations: ["uppercase"]
    });
    
    console.log('Step 1 - Processed Text:', processedText.results.uppercase);
    
    // Step 2: Analyze sentiment of processed text
    const sentimentAnalyzer = await client.createNodeProxy(
      'TransformersPipelineNode',
      {
        task: 'sentiment-analysis',
        model: 'distilbert-base-uncased-finetuned-sst-2-english'
      }
    );
    
    const sentiment = await sentimentAnalyzer.process(processedText.results.uppercase);
    console.log('Step 2 - Sentiment of Uppercase Text:', sentiment);
    
    // Step 3: Generate continuation
    const textGen = await client.createNodeProxy(
      'TransformersPipelineNode',
      {
        task: 'text-generation',
        model: 'gpt2',
        model_kwargs: {
          max_length: 30,
          temperature: 0.8
        }
      }
    );
    
    const continuation = await textGen.process(processedText.results.uppercase + " IS");
    console.log('Step 3 - Generated Continuation:', continuation[0].generated_text);
  });
  
  // Method 5: Server inspection
  console.log('\n📌 Method 5: Server Inspection');
  console.log('-----------------------------');
  
  await withRemoteProxy(config, async (client) => {
    // Get server status
    const status = await client.getStatus();
    console.log('Server Status:', status.status);
    console.log('Server Version:', status.version);
    
    // List available nodes
    const nodes = await client.listNodes();
    console.log('\nAvailable Node Types:', nodes.map(n => n.node_type).join(', '));
  });
  
  console.log('\n✅ Demo completed successfully!');
  console.log('\n💡 Key Features Demonstrated:');
  console.log('  • Execute any server node type remotely');
  console.log('  • Python-style context manager pattern');
  console.log('  • Complex configuration with model_kwargs');
  console.log('  • Helper classes for common nodes');
  console.log('  • Pipeline-like multi-step processing');
  console.log('  • Server status and node discovery');
}

// Run the demo
if (require.main === module) {
  main().catch(console.error);
}