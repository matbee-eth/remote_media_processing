/**
 * Sentiment Analysis Example
 * 
 * Demonstrates using Hugging Face transformers for sentiment analysis.
 */

import { withRemoteProxy, RemoteNodes } from '../src';

async function analyzeSentiments() {
  const reviews = [
    "This product is absolutely amazing! Best purchase I've ever made.",
    "Terrible quality, broke after one day. Very disappointed.",
    "It's okay, nothing special but does the job.",
    "Outstanding service and fast delivery. Highly recommend!",
    "Not worth the price. Found better alternatives elsewhere."
  ];

  await withRemoteProxy(
    { host: 'localhost', port: 50052 },
    async (client) => {
      console.log('🎭 Sentiment Analysis Example\n');

      // Use the RemoteNodes helper
      const nodes = new RemoteNodes(client);

      // Create sentiment analyzer
      const analyzer = await nodes.transformersPipeline({
        task: 'sentiment-analysis',
        model: 'distilbert-base-uncased-finetuned-sst-2-english'
      });

      console.log('Analyzing customer reviews:\n');

      // Analyze each review
      for (const review of reviews) {
        const [result] = await analyzer.process(review);

        const emoji = result.label === 'POSITIVE' ? '😊' : '😞';
        const percentage = (result.score * 100).toFixed(1);

        console.log(`${emoji} ${result.label} (${percentage}% confidence)`);
        console.log(`   "${review}"\n`);
      }

      // Batch analysis example
      console.log('📊 Summary Statistics:');
      const allResults = await Promise.all(
        reviews.map(review => analyzer.process(review))
      );

      const positive = allResults.filter(([r]) => r.label === 'POSITIVE').length;
      const negative = allResults.filter(([r]) => r.label === 'NEGATIVE').length;

      console.log(`  Positive reviews: ${positive}/${reviews.length}`);
      console.log(`  Negative reviews: ${negative}/${reviews.length}`);
    }
  );
}

// Advanced example with custom model configuration
async function advancedSentimentAnalysis() {
  await withRemoteProxy(
    { host: 'localhost', port: 50052 },
    async (client) => {
      console.log('\n🔬 Advanced Sentiment Analysis\n');

      // Create analyzer with specific model
      const analyzer = await client.createNodeProxy(
        'TransformersPipelineNode',
        {
          task: 'sentiment-analysis',
          model: 'nlptown/bert-base-multilingual-uncased-sentiment',
          device: -1  // Use CPU
        }
      );

      // Analyze multilingual text
      const texts = {
        'English': "This is fantastic!",
        'Spanish': "¡Esto es fantástico!",
        'French': "C'est fantastique!",
        'German': "Das ist fantastisch!"
      };

      for (const [language, text] of Object.entries(texts)) {
        try {
          const result = await analyzer.process(text);
          console.log(`${language}: "${text}"`);
          console.log(`  Result:`, result);
        } catch (error: any) {
          console.log(`${language}: Model may not support this language`);
        }
      }
    }
  );
}

// Run examples
async function main() {
  try {
    await analyzeSentiments();
    // Uncomment to run advanced example
    await advancedSentimentAnalysis();
  } catch (error) {
    console.error('Error:', error);
  }
}

if (require.main === module) {
  main();
}