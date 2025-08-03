#!/bin/bash

# Run Hugging Face Pipeline Examples

echo "🤖 RemoteMedia Hugging Face Pipeline Examples"
echo "============================================"
echo ""

# Check if node_modules exists
if [ ! -d "node_modules" ]; then
    echo "📦 Installing dependencies..."
    npm install
    echo ""
fi

# Check if TypeScript definitions exist
if [ ! -f "../../remotemedia-types.d.ts" ]; then
    echo "📝 Generating TypeScript definitions..."
    cd ../..
    python scripts/generate_typescript_defs.py -o remotemedia-types.d.ts
    cd examples/nodejs-huggingface
    echo ""
fi

# Display usage if no argument provided
if [ $# -eq 0 ]; then
    echo "Usage: ./run-example.sh [example]"
    echo ""
    echo "Available examples:"
    echo "  sentiment      - Sentiment analysis example"
    echo "  generation     - Text generation with GPT-2"
    echo "  advanced       - Advanced text generation with custom parameters"
    echo "  qa             - Question answering"
    echo "  classification - Zero-shot classification"
    echo "  ner            - Named entity recognition (streaming)"
    echo "  full           - Run all examples from full-featured client"
    echo "  test           - Run all tests"
    echo ""
    echo "Example: ./run-example.sh sentiment"
    exit 0
fi

# Run the requested example
case $1 in
    sentiment)
        echo "Running sentiment analysis example..."
        npx ts-node examples/sentiment-analysis.ts
        ;;
    generation)
        echo "Running text generation example..."
        npx ts-node examples/text-generation.ts
        ;;
    advanced)
        echo "Running advanced text generation example..."
        npx ts-node examples/advanced-text-generation.ts
        ;;
    qa|classification|ner)
        echo "Running $1 example..."
        npx ts-node examples/full-featured-client.ts $1
        ;;
    full|all)
        echo "Running all examples..."
        npx ts-node examples/full-featured-client.ts all
        ;;
    test)
        echo "Running tests..."
        npm run test:kwargs
        npm run test:types
        ;;
    *)
        echo "Unknown example: $1"
        echo "Run './run-example.sh' without arguments to see available examples."
        exit 1
        ;;
esac