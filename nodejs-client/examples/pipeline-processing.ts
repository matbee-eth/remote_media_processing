/**
 * Pipeline Processing Example
 * 
 * Demonstrates chaining multiple nodes together for complex processing.
 */

import {
  withRemoteProxy,
  RemoteNodes,
  batchProcess
} from '../src';

async function textProcessingPipeline() {
  console.log('🔗 Text Processing Pipeline Example\n');

  await withRemoteProxy(
    { host: 'localhost', port: 50052 },
    async (client) => {
      const nodes = new RemoteNodes(client);

      // Step 1: Create individual nodes
      const textProcessor = await nodes.textProcessor();
      const sentimentAnalyzer = await nodes.transformersPipeline({
        task: 'sentiment-analysis'
      });
      const textGenerator = await nodes.transformersPipeline({
        task: 'text-generation',
        model: 'gpt2',
        model_kwargs: {
          max_length: 50,
          temperature: 0.9,
          do_sample: true
        }
      });

      // Example 1: Manual pipeline
      console.log('📝 Example 1: Manual Pipeline Processing\n');

      const inputText = "the remotemedia sdk is revolutionary";
      console.log(`Input: "${inputText}"`);

      // Process through text processor
      const processed = await textProcessor.process({
        text: inputText,
        operations: ['uppercase']
      });
      console.log(`After text processing: "${processed.results.uppercase}"`);

      // Analyze sentiment
      const sentiment = await sentimentAnalyzer.process(processed.results.uppercase);
      console.log(`Sentiment analysis:`, sentiment);

      // Generate continuation
      const prompt = processed.results.uppercase + " because it";
      const generated = await textGenerator.process(prompt);
      console.log(`Generated text: "${generated[0].generated_text}"`);

      // Example 2: Using NodePipeline helper
      console.log('\n📦 Example 2: NodePipeline Helper\n');

      // Note: This works best when nodes have compatible input/output formats
      // For this example, we'll process multiple texts
      const texts = [
        "Machine learning is amazing",
        "This pipeline is terrible",
        "Natural language processing works"
      ];

      // Process each text through sentiment analysis
      for (const text of texts) {
        const result = await sentimentAnalyzer.process(text);
        console.log(`"${text}" → ${result[0].label} (${(result[0].score * 100).toFixed(1)}%)`);
      }
    }
  );
}

async function multiModalPipeline() {
  console.log('\n🎨 Multi-Modal Pipeline Example\n');

  await withRemoteProxy(
    { host: 'localhost', port: 50052 },
    async (client) => {
      // Create nodes for different modalities
      const audioTransform = await client.createNodeProxy('AudioTransform', {
        sampleRate: 16000,
        channels: 1
      });

      const textProcessor = await client.createNodeProxy('TextProcessorNode');

      // Example: Process audio metadata
      console.log('🎵 Processing audio with metadata:\n');

      const audioData = {
        samples: new Array(1000).fill(0).map(() => Math.random() * 2 - 1),
        sampleRate: 44100,
        channels: 2,
        metadata: {
          title: "test audio file",
          duration: 0.023  // ~23ms
        }
      };

      // Transform audio
      await audioTransform.process(audioData);
      console.log('Audio transformed:');
      console.log(`  Original: ${audioData.sampleRate}Hz, ${audioData.channels} channels`);
      console.log(`  Transformed: 16000Hz, 1 channel`);

      // Process metadata text
      if (audioData.metadata?.title) {
        const processedMeta = await textProcessor.process({
          text: audioData.metadata.title,
          operations: ['uppercase', 'word_count']
        });
        console.log('\nMetadata processed:');
        console.log(`  Title: "${processedMeta.results.uppercase}"`);
        console.log(`  Words: ${processedMeta.results.word_count}`);
      }
    }
  );
}

async function batchProcessingExample() {
  console.log('\n📊 Batch Processing Example\n');

  await withRemoteProxy(
    { host: 'localhost', port: 50052 },
    async (client) => {
      const nodes = new RemoteNodes(client);

      // Create a sentiment analyzer
      const analyzer = await nodes.transformersPipeline({
        task: 'sentiment-analysis'
      });

      // Large dataset to process
      const dataset = [
        "Great product, highly recommend!",
        "Worst experience ever.",
        "It's okay, nothing special.",
        "Amazing quality and fast shipping!",
        "Not worth the money.",
        "Exceeded my expectations!",
        "Disappointing purchase.",
        "Good value for the price.",
        "Terrible customer service.",
        "Would buy again!"
      ];

      console.log(`Processing ${dataset.length} items in batches...\n`);

      // Define the sentiment analysis result type
      type SentimentResult = Array<{ label: string; score: number }>;

      // Process with progress tracking
      const results = await batchProcess<string, SentimentResult>(analyzer, dataset, {
        batchSize: 3,
        parallel: true,
        onProgress: (completed, total) => {
          const percentage = ((completed / total) * 100).toFixed(0);
          console.log(`Progress: ${completed}/${total} (${percentage}%)`);
        }
      });

      // Analyze results
      console.log('\n📈 Results Summary:');
      const positive = results.filter(result => result[0].label === 'POSITIVE').length;
      const negative = results.filter(result => result[0].label === 'NEGATIVE').length;

      console.log(`  Positive: ${positive} (${(positive / dataset.length * 100).toFixed(1)}%)`);
      console.log(`  Negative: ${negative} (${(negative / dataset.length * 100).toFixed(1)}%)`);

      // Find most confident predictions
      const mostPositive = results
        .filter(result => result[0].label === 'POSITIVE')
        .sort((a, b) => b[0].score - a[0].score)[0];

      const mostNegative = results
        .filter(result => result[0].label === 'NEGATIVE')
        .sort((a, b) => b[0].score - a[0].score)[0];

      console.log('\n🎯 Most confident predictions:');
      if (mostPositive) {
        const posIndex = results.indexOf(mostPositive);
        console.log(`  Most positive: "${dataset[posIndex]}" (${(mostPositive[0].score * 100).toFixed(1)}%)`);
      }

      if (mostNegative) {
        const negIndex = results.indexOf(mostNegative);
        console.log(`  Most negative: "${dataset[negIndex]}" (${(mostNegative[0].score * 100).toFixed(1)}%)`);
      }
    }
  );
}

// Run all examples
async function main() {
  try {
    await textProcessingPipeline();
    await multiModalPipeline();
    await batchProcessingExample();
  } catch (error) {
    console.error('Error:', error);
  }
}

if (require.main === module) {
  main();
}