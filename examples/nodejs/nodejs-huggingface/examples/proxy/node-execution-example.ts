/**
 * Node Execution Example
 * 
 * Demonstrates executing various server nodes using RemoteProxyClient
 */

import { 
  RemoteProxyClient, 
  RemoteExecutorConfig, 
  withRemoteProxy,
  RemoteNodes 
} from '../../src/remote-proxy-client-v2';

async function example1_sentimentAnalysis() {
  console.log('📊 Example 1: Sentiment Analysis with TransformersPipelineNode');
  console.log('==========================================================\n');
  
  const config: RemoteExecutorConfig = {
    host: "localhost",
    port: 50052
  };

  await withRemoteProxy(config, async (client) => {
    // Create a proxy for the TransformersPipelineNode
    const sentimentAnalyzer = await client.createNodeProxy(
      'TransformersPipelineNode',
      {
        task: 'sentiment-analysis',
        model: 'distilbert-base-uncased-finetuned-sst-2-english'
      }
    );
    
    // Test sentences
    const sentences = [
      "I love this new feature! It's amazing!",
      "This is terrible, I hate it.",
      "It's okay, nothing special."
    ];
    
    for (const sentence of sentences) {
      console.log(`Analyzing: "${sentence}"`);
      const result = await sentimentAnalyzer.process(sentence);
      console.log(`Result:`, result);
      console.log();
    }
  });
}

async function example2_audioProcessing() {
  console.log('🎵 Example 2: Audio Processing with AudioTransform');
  console.log('==============================================\n');
  
  await withRemoteProxy({ host: "localhost", port: 50052 }, async (client) => {
    // Create audio transform node
    const audioTransform = await client.createNodeProxy(
      'AudioTransform',
      {
        sampleRate: 16000,
        channels: 1,
        dtype: 'float32'
      }
    );
    
    // Simulate audio data
    const audioData = {
      samples: new Array(1000).fill(0).map(() => Math.random() * 2 - 1),
      sampleRate: 44100,
      channels: 2
    };
    
    console.log('Input audio:');
    console.log(`  Sample rate: ${audioData.sampleRate} Hz`);
    console.log(`  Channels: ${audioData.channels}`);
    console.log(`  Samples: ${audioData.samples.length}`);
    
    const transformed = await audioTransform.process(audioData);
    console.log('\nTransformed audio:', transformed);
  });
}

async function example3_calculator() {
  console.log('🧮 Example 3: Calculator Node');
  console.log('=============================\n');
  
  await withRemoteProxy({ host: "localhost", port: 50052 }, async (client) => {
    // Create calculator node
    const calculator = await client.createNodeProxy('CalculatorNode', {});
    
    // Test operations
    const operations = [
      { operation: 'add', args: [5, 3] },
      { operation: 'multiply', args: [4, 7] },
      { operation: 'divide', args: [20, 4] },
      { operation: 'power', args: [2, 8] }
    ];
    
    for (const op of operations) {
      console.log(`Operation: ${op.operation}(${op.args.join(', ')})`);
      const result = await calculator.process(op);
      console.log(`Result:`, result);
      console.log();
    }
  });
}

async function example4_listAvailableNodes() {
  console.log('📋 Example 4: List Available Nodes');
  console.log('==================================\n');
  
  await withRemoteProxy({ host: "localhost", port: 50052 }, async (client) => {
    // Get all available nodes
    const nodes = await client.listNodes();
    
    console.log('Available nodes on server:');
    for (const node of nodes) {
      console.log(`\n${node.node_type} (${node.category})`);
      console.log(`  ${node.description}`);
      
      if (node.parameters && node.parameters.length > 0) {
        console.log('  Parameters:');
        for (const param of node.parameters) {
          const required = param.required ? ' (required)' : '';
          const defaultVal = param.default_value ? ` = ${param.default_value}` : '';
          console.log(`    - ${param.name}: ${param.type}${defaultVal}${required}`);
          if (param.description) {
            console.log(`      ${param.description}`);
          }
        }
      }
    }
  });
}

async function example5_helperClasses() {
  console.log('🛠️  Example 5: Using Helper Classes');
  console.log('===================================\n');
  
  await withRemoteProxy({ host: "localhost", port: 50052 }, async (client) => {
    const nodes = new RemoteNodes(client);
    
    // Use convenience methods
    console.log('1. Text Generation:');
    const textGen = await nodes.transformersPipeline({
      task: 'text-generation',
      model: 'gpt2',
      model_kwargs: {
        max_length: 30,
        temperature: 0.8
      }
    });
    
    const generated = await textGen.process("The future of AI is");
    console.log('Generated:', generated);
    
    console.log('\n2. Text Processing:');
    const textProcessor = await client.createNodeProxy('TextProcessorNode');
    
    const textResult = await textProcessor.process({
      text: "Hello World! This is a test.",
      operations: ["uppercase", "word_count", "char_count"]
    });
    
    console.log('Text Processing Result:', textResult);
  });
}

async function example6_streaming() {
  console.log('🌊 Example 6: Streaming Data');
  console.log('============================\n');
  
  await withRemoteProxy({ host: "localhost", port: 50052 }, async (client) => {
    // Create a streaming node
    const calculator = await client.createNodeProxy('CalculatorNode', {
      operation: 'multiply',
      factor: 2
    });
    
    if (calculator.processStream) {
      console.log('Starting stream...');
      const stream = calculator.processStream(
        (data) => {
          console.log('Received:', data);
        },
        (error) => {
          console.error('Stream error:', error);
        }
      );
      
      // Send some data
      for (let i = 1; i <= 5; i++) {
        console.log(`Sending: ${i}`);
        await stream.send({ value: i });
        await new Promise(resolve => setTimeout(resolve, 100));
      }
      
      await stream.close();
      console.log('Stream closed');
    }
  });
}

async function example7_serverStatus() {
  console.log('📈 Example 7: Server Status');
  console.log('===========================\n');
  
  await withRemoteProxy({ host: "localhost", port: 50052 }, async (client) => {
    const status = await client.getStatus();
    
    console.log('Server Status:', status.status);
    console.log('Version:', status.version);
    console.log('Uptime:', status.uptime_seconds, 'seconds');
    
    if (status.metrics) {
      console.log('\nMetrics:');
      console.log('  Total requests:', status.metrics.total_requests);
      console.log('  Success rate:', status.metrics.success_rate);
      console.log('  Average latency:', status.metrics.average_latency_ms, 'ms');
    }
    
    if (status.active_sessions && status.active_sessions.length > 0) {
      console.log('\nActive Sessions:');
      for (const session of status.active_sessions) {
        console.log(`  ${session.session_id}: ${session.node_type}`);
      }
    }
  });
}

// Main function
async function main() {
  console.log('🚀 RemoteProxyClient Node Execution Examples');
  console.log('==========================================\n');
  console.log('This demonstrates executing various server nodes remotely.\n');
  
  try {
    // Run examples based on command line argument
    const example = process.argv[2];
    
    switch (example) {
      case 'sentiment':
        await example1_sentimentAnalysis();
        break;
      case 'audio':
        await example2_audioProcessing();
        break;
      case 'calculator':
        await example3_calculator();
        break;
      case 'list':
        await example4_listAvailableNodes();
        break;
      case 'helpers':
        await example5_helperClasses();
        break;
      case 'stream':
        await example6_streaming();
        break;
      case 'status':
        await example7_serverStatus();
        break;
      case 'all':
      default:
        await example1_sentimentAnalysis();
        await example2_audioProcessing();
        await example3_calculator();
        await example4_listAvailableNodes();
        await example5_helperClasses();
        // await example6_streaming(); // Skip streaming in 'all' mode
        await example7_serverStatus();
        break;
    }
    
    console.log('\n✅ Examples completed successfully!');
  } catch (error) {
    console.error('Fatal error:', error);
  }
}

// Run the examples
if (require.main === module) {
  console.log('\nUsage: ts-node node-execution-example.ts [example]');
  console.log('Examples: sentiment, audio, calculator, list, helpers, stream, status, all\n');
  
  main().catch(console.error);
}