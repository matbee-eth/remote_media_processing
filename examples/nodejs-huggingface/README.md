# Hugging Face Pipeline Remote Execution Example

This example demonstrates how to execute Hugging Face transformers pipelines remotely using the RemoteMedia Processing SDK from Node.js/TypeScript.

## Overview

The example showcases various NLP tasks that can be executed on a remote server with GPU acceleration:

1. **Sentiment Analysis** - Analyze the sentiment of text (positive/negative)
2. **Text Generation** - Generate text continuations using GPT-2
3. **Question Answering** - Extract answers from context using BERT
4. **Zero-Shot Classification** - Classify text without training data
5. **Named Entity Recognition** - Extract entities from text (streaming)

## Prerequisites

1. **Remote Service Running**: Ensure the RemoteMedia remote service is running:
   ```bash
   cd ../../remote_service
   docker-compose up
   ```

2. **Node.js**: Node.js 14+ installed

3. **Dependencies**: Install npm packages:
   ```bash
   npm install
   ```

## Project Structure

```
nodejs-huggingface/
├── examples/              # Main example applications
│   ├── sentiment-analysis.ts       # Simple sentiment analysis
│   ├── text-generation.ts          # Basic text generation
│   ├── advanced-text-generation.ts # Advanced generation with parameters
│   └── full-featured-client.ts     # Complete demo with 5 NLP tasks
├── tests/                 # Test files
│   ├── test-kwargs.ts              # Parameter parsing tests
│   ├── test-generation.ts          # Generation debugging
│   ├── test-types.ts               # TypeScript type tests
│   └── debug-response-format.ts    # gRPC response debugging
├── utils/                 # Utility files
│   ├── proxy-client-example.ts     # Alternative proxy implementation
│   └── sentiment_analyzer.py       # Python code for proxy example
├── index.ts               # Main entry point and documentation
├── package.json           # Dependencies and scripts
├── tsconfig.json          # TypeScript configuration
├── README.md              # This file
└── run-example.sh         # Convenience script

```

## Running the Examples

### Quick Start
```bash
npm install                 # Install dependencies
npm start                   # Show available examples
npm run sentiment          # Run sentiment analysis
npm run generation         # Run text generation
npm run advanced           # Run advanced generation
npm run full               # Run all examples
```

### Using the Shell Script
```bash
./run-example.sh sentiment      # Run sentiment analysis
./run-example.sh generation     # Run text generation
./run-example.sh advanced       # Run advanced generation
./run-example.sh full          # Run all examples
./run-example.sh test          # Run all tests
```

### Individual Examples

```bash
# Simple Examples
npx ts-node examples/sentiment-analysis.ts
npx ts-node examples/text-generation.ts

# Advanced Examples
npx ts-node examples/advanced-text-generation.ts [creative|technical|story|compare|all]
npx ts-node examples/full-featured-client.ts [sentiment|generation|qa|classification|ner|all]

# Tests
npm run test:kwargs        # Test parameter parsing
npm run test:types         # Test TypeScript types
npm run test:debug         # Debug gRPC responses
```

### Custom Configuration

Set environment variables to connect to a different server:

```bash
export REMOTE_HOST=your-server.com
export REMOTE_PORT=50052
npm run all
```

## How It Works

1. **Client Setup**: The client connects to the remote gRPC service
2. **Pipeline Configuration**: Specifies the Hugging Face task and model
3. **Remote Execution**: The `TransformersPipelineNode` runs on the server
4. **GPU Acceleration**: Automatically uses GPU if available on server
5. **Results**: Processed results are returned with performance metrics

## Example Output

```
📊 Example 1: Sentiment Analysis
================================

Analyzing: "I love this product! It's amazing and works perfectly."
  Sentiment: POSITIVE (confidence: 99.98%)
  Processing time: 245ms

Analyzing: "This is terrible. Completely disappointed with the quality."
  Sentiment: NEGATIVE (confidence: 99.97%)
  Processing time: 23ms
```

## Supported Tasks

The `TransformersPipelineNode` supports all Hugging Face pipeline tasks:

- `sentiment-analysis`
- `text-generation`
- `text2text-generation`
- `question-answering`
- `zero-shot-classification`
- `ner` (named entity recognition)
- `summarization`
- `translation`
- `feature-extraction`
- `fill-mask`
- `token-classification`
- `conversational`
- `image-classification`
- `object-detection`
- `image-segmentation`
- `automatic-speech-recognition`
- And more...

## Customizing Models

You can use any model from the Hugging Face Hub:

```typescript
const config: HuggingFacePipelineConfig = {
  task: 'sentiment-analysis',
  model: 'nlptown/bert-base-multilingual-uncased-sentiment', // 5-star ratings
  device: 'cuda:0' // Specify GPU
};
```

## Performance Considerations

1. **Model Loading**: First execution loads the model (can take 10-30s)
2. **Caching**: Subsequent calls reuse the loaded model (fast)
3. **Batch Processing**: Consider batching inputs for better throughput
4. **Streaming**: Use streaming for processing multiple items

## Adding Custom Pipelines

To add a new pipeline task:

1. Choose a task from [Hugging Face Tasks](https://huggingface.co/tasks)
2. Find a suitable model on [Hugging Face Hub](https://huggingface.co/models)
3. Configure the pipeline:

```typescript
const config: HuggingFacePipelineConfig = {
  task: 'your-task',
  model: 'model-name',
  model_kwargs: {
    // Task-specific parameters
  }
};
```

## Troubleshooting

### Connection Refused
- Ensure the remote service is running
- Check firewall settings
- Verify host and port configuration

### Model Not Found
- Check model name spelling
- Ensure model exists on Hugging Face Hub
- Some models require authentication

### Out of Memory
- Use a smaller model
- Reduce batch size
- Ensure server has sufficient GPU memory

### Slow Performance
- First run downloads the model (normal)
- Consider using a faster/smaller model
- Check network latency to server

## Next Steps

- Try different models and tasks
- Implement batch processing for better performance
- Add authentication for private models
- Create a web UI for the examples
- Integrate with your application

## Resources

- [Hugging Face Pipelines Documentation](https://huggingface.co/docs/transformers/main_classes/pipelines)
- [Available Models](https://huggingface.co/models)
- [RemoteMedia SDK Documentation](../../README.md)
- [TypeScript Integration Guide](../../docs/TYPESCRIPT_USAGE.md)