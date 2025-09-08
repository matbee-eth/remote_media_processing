/**
 * Sentiment Analysis using RemoteProxyClient approach
 * 
 * This example shows how to execute a Hugging Face pipeline using
 * the RemoteProxyClient pattern, which doesn't require the node
 * to be pre-registered on the server.
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { promisify } from 'util';
import * as path from 'path';
import * as fs from 'fs';
import * as childProcess from 'child_process';

// First, create a Python script that will be executed remotely
const pythonCode = `
import asyncio
from typing import List, Dict, Any

class HuggingFaceSentimentAnalyzer:
    def __init__(self, model_name: str = "distilbert-base-uncased-finetuned-sst-2-english"):
        self.model_name = model_name
        self.pipeline = None
        
    async def initialize(self):
        """Load the Hugging Face pipeline."""
        print(f"Loading model: {self.model_name}")
        from transformers import pipeline
        import torch
        
        # Determine device
        if torch.cuda.is_available():
            device = "cuda:0"
        elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            device = "mps"
        else:
            device = "cpu"
            
        print(f"Using device: {device}")
        
        # Load pipeline (this may download the model on first run)
        self.pipeline = await asyncio.to_thread(
            pipeline,
            task="sentiment-analysis",
            model=self.model_name,
            device=device
        )
        print("Model loaded successfully!")
        
    async def analyze(self, text: str) -> Dict[str, Any]:
        """Analyze sentiment of a single text."""
        if not self.pipeline:
            await self.initialize()
            
        # Run inference
        result = await asyncio.to_thread(self.pipeline, text)
        return result[0]  # Return first result
        
    async def analyze_batch(self, texts: List[str]) -> List[Dict[str, Any]]:
        """Analyze sentiment of multiple texts."""
        if not self.pipeline:
            await self.initialize()
            
        # Run batch inference
        results = await asyncio.to_thread(self.pipeline, texts)
        return results
        
    def cleanup(self):
        """Clean up resources."""
        if hasattr(self, 'pipeline') and self.pipeline is not None:
            del self.pipeline
            self.pipeline = None
            
            # Clear CUDA cache if using GPU
            try:
                import torch
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
            except:
                pass

# Create the analyzer instance
analyzer = HuggingFaceSentimentAnalyzer()
`;

// Save the Python code to a file
const pythonFile = path.join(__dirname, 'sentiment_analyzer.py');
fs.writeFileSync(pythonFile, pythonCode);

// Create a package script
const packageScript = `
import os
import sys
import zipfile
import base64
import cloudpickle

# Create the analyzer object
exec(open('sentiment_analyzer.py').read())

# Package the code
with zipfile.ZipFile('package.zip', 'w') as zf:
    zf.write('sentiment_analyzer.py', 'code/sentiment_analyzer.py')

# Serialize the object
serialized_obj = base64.b64encode(cloudpickle.dumps(analyzer)).decode('utf-8')

# Write serialized object
with open('package.zip', 'rb') as f:
    package_data = f.read()
    
with zipfile.ZipFile('package.zip', 'a') as zf:
    zf.writestr('serialized_object.pkl', serialized_obj)

# Output the package
with open('package.zip', 'rb') as f:
    print(base64.b64encode(f.read()).decode('utf-8'))
`;

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

async function packagePythonCode(): Promise<Buffer> {
  // Save package script
  const packageScriptFile = path.join(__dirname, 'package_script.py');
  fs.writeFileSync(packageScriptFile, packageScript);
  
  // Run Python to create the package
  return new Promise((resolve, reject) => {
    childProcess.exec(`python ${packageScriptFile}`, { cwd: __dirname }, (error, stdout, stderr) => {
      if (error) {
        console.error('Packaging error:', stderr);
        reject(error);
      } else {
        // Clean up temporary files
        fs.unlinkSync(packageScriptFile);
        fs.unlinkSync(pythonFile);
        if (fs.existsSync(path.join(__dirname, 'package.zip'))) {
          fs.unlinkSync(path.join(__dirname, 'package.zip'));
        }
        
        // Decode the base64 package
        resolve(Buffer.from(stdout.trim(), 'base64'));
      }
    });
  });
}

async function analyzeSentimentViaProxy(texts: string[]) {
  console.log('📦 Packaging Python code...');
  const packageData = await packagePythonCode();
  
  // Create gRPC client
  const client = new remoteMedia.execution.RemoteExecutionService(
    'localhost:50052',
    grpc.credentials.createInsecure()
  );
  
  const executeMethod = promisify(client.ExecuteObjectMethod.bind(client));
  
  // First, initialize the remote object
  console.log('🚀 Initializing remote sentiment analyzer...');
  const initRequest = {
    code_package: packageData,
    config: {},
    serialization_format: 'pickle',
    method_name: 'initialize',
    method_args_data: Buffer.from(cloudpickle.dumps([])),
    method_kwargs_data: Buffer.from(cloudpickle.dumps({})),
    dependencies: ['transformers', 'torch']
  };
  
  const initResponse = await executeMethod(initRequest);
  
  if (initResponse.status !== 'EXECUTION_STATUS_SUCCESS') {
    console.error('❌ Failed to initialize:', initResponse.error_message);
    return;
  }
  
  const sessionId = initResponse.session_id;
  console.log(`✅ Initialized with session ID: ${sessionId}`);
  
  // Analyze texts
  console.log('\n📊 Analyzing sentiments...\n');
  
  for (const text of texts) {
    const analyzeRequest = {
      session_id: sessionId,
      config: {},
      serialization_format: 'pickle',
      method_name: 'analyze',
      method_args_data: Buffer.from(cloudpickle.dumps([text])),
      method_kwargs_data: Buffer.from(cloudpickle.dumps({}))
    };
    
    const response = await executeMethod(analyzeRequest);
    
    if (response.status === 'EXECUTION_STATUS_SUCCESS') {
      const result = cloudpickle.loads(response.result_data);
      console.log(`Text: "${text}"`);
      console.log(`  Sentiment: ${result.label} (confidence: ${(result.score * 100).toFixed(2)}%)\n`);
    } else {
      console.error(`Failed to analyze: ${response.error_message}`);
    }
  }
}

// Simple cloudpickle mock for the example (in real use, you'd use a proper implementation)
const cloudpickle = {
  dumps: (obj: any) => Buffer.from(JSON.stringify(obj)),
  loads: (data: Buffer) => JSON.parse(data.toString())
};

// Example usage
async function main() {
  console.log('🤖 Sentiment Analysis via RemoteProxyClient Pattern\n');
  
  const texts = [
    "I absolutely love this new feature! It's incredible!",
    "This is the worst experience I've ever had.",
    "It's okay, nothing special but it works.",
    "Amazing! Best decision I've made all year!"
  ];
  
  try {
    await analyzeSentimentViaProxy(texts);
  } catch (error) {
    console.error('Error:', error);
  }
}

// Run the example
if (require.main === module) {
  console.log('Note: This is a demonstration of the proxy pattern.');
  console.log('For a simpler approach, use the direct node execution after the server is updated.\n');
  
  // For now, let's use the simpler CodeExecutorNode approach
  simpleSentimentWithCodeExecutor().catch(console.error);
}

// Alternative: Use CodeExecutorNode for a simpler approach
async function simpleSentimentWithCodeExecutor() {
  const client = new remoteMedia.execution.RemoteExecutionService(
    'localhost:50052',
    grpc.credentials.createInsecure()
  );
  
  const executeNode = promisify(client.ExecuteNode.bind(client));
  
  const sentimentCode = `
async def process(text):
    from transformers import pipeline
    import torch
    
    # Determine device
    device = "cuda:0" if torch.cuda.is_available() else "cpu"
    
    # Load pipeline
    classifier = pipeline("sentiment-analysis", 
                         model="distilbert-base-uncased-finetuned-sst-2-english",
                         device=device)
    
    # Run inference
    result = classifier(text)
    return result[0]
`;
  
  console.log('🤖 Sentiment Analysis using CodeExecutorNode\n');
  
  const texts = [
    "I absolutely love this new feature! It's incredible!",
    "This is the worst experience I've ever had.",
    "It's okay, nothing special but it works.",
    "Amazing! Best decision I've made all year!"
  ];
  
  for (const text of texts) {
    console.log(`Analyzing: "${text}"`);
    
    const request = {
      node_type: 'CodeExecutorNode',
      config: {
        code: sentimentCode,
        entry_point: 'process'
      },
      input_data: Buffer.from(JSON.stringify(text)),
      serialization_format: 'json',
      options: {
        timeout: 60.0,
        enable_gpu: true
      }
    };
    
    try {
      const response = await executeNode(request);
      
      if (response.status === 'EXECUTION_STATUS_SUCCESS') {
        const result = JSON.parse(response.output_data.toString());
        console.log(`  ✅ Sentiment: ${result.label} (confidence: ${(result.score * 100).toFixed(2)}%)`);
        console.log(`     Processing time: ${response.metrics?.duration_ms || 'N/A'}ms\n`);
      } else {
        console.error(`  ❌ Error: ${response.error_message}`);
      }
    } catch (error) {
      console.error(`  ❌ Failed:`, error);
    }
  }
}