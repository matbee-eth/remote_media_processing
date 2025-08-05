/**
 * Simple Pipeline Execution from Node.js
 * 
 * This example shows the simplest way to execute a Python pipeline from Node.js
 * and stream data through it.
 */

import { RemoteProxyClient, RemoteExecutorConfig } from '../src/remote-proxy-client-streaming';

/**
 * Create a simple audio processing pipeline in Python
 */
class AudioPipeline {
  pipeline: any;
  
  async initialize() {
    // Import required modules
    const { Pipeline } = await import('remotemedia.core.pipeline');
    const { AudioTransform, VoiceActivityDetector } = await import('remotemedia.nodes.audio');
    const { PassThroughNode } = await import('remotemedia.nodes');
    
    // Create pipeline
    this.pipeline = new Pipeline({ name: "NodeJSAudioPipeline" });
    
    // Add nodes
    this.pipeline.add_node(new AudioTransform({
      output_sample_rate: 16000,
      output_channels: 1,
      name: "AudioTransform"
    }));
    
    this.pipeline.add_node(new VoiceActivityDetector({
      frame_duration_ms: 30,
      speech_threshold: 0.3,
      name: "VAD"
    }));
    
    this.pipeline.add_node(new PassThroughNode({
      name: "Output"
    }));
    
    // Initialize the pipeline
    await this.pipeline.initialize();
    
    console.log("[Pipeline] Initialized with nodes:", this.pipeline.nodes.map(n => n.name));
  }
  
  /**
   * Process audio data through the pipeline
   */
  async *processStream(audioFrames: AsyncIterable<any>): AsyncGenerator<any> {
    // Process each frame through the pipeline
    for await (const frame of audioFrames) {
      const results = [];
      
      // Run frame through pipeline
      for await (const result of this.pipeline.process(frame)) {
        results.push(result);
      }
      
      // Yield all results
      for (const result of results) {
        yield result;
      }
    }
  }
  
  async cleanup() {
    if (this.pipeline) {
      await this.pipeline.cleanup();
    }
  }
}

/**
 * Generate test audio frames
 */
async function* generateTestAudioFrames(count: number = 100): AsyncGenerator<any> {
  const sampleRate = 16000;
  const frameDurationMs = 20;
  const samplesPerFrame = (sampleRate * frameDurationMs) / 1000;
  
  for (let i = 0; i < count; i++) {
    // Create audio data
    const audioData = new Float32Array(samplesPerFrame);
    
    // Simulate speech in the middle section
    const isSpeech = i > 20 && i < 80;
    const amplitude = isSpeech ? 0.3 : 0.01;
    
    for (let j = 0; j < samplesPerFrame; j++) {
      audioData[j] = (Math.random() - 0.5) * amplitude;
    }
    
    yield {
      audio: audioData,
      sample_rate: sampleRate,
      channels: 1,
      metadata: {
        frame_number: i,
        timestamp: Date.now()
      }
    };
    
    // Simulate real-time
    await new Promise(resolve => setTimeout(resolve, frameDurationMs));
  }
}

/**
 * Main example using RemoteProxyClient
 */
async function runWithRemoteProxy() {
  console.log('=== Simple Pipeline Execution Example ===\n');
  
  const config: RemoteExecutorConfig = {
    host: 'localhost',
    port: 50052,
    pipPackages: ['numpy', 'scipy'] // If needed
  };
  
  const pythonCode = `
from remotemedia.core.pipeline import Pipeline
from remotemedia.nodes.audio import AudioTransform, VoiceActivityDetector
from remotemedia.nodes import PassThroughNode
import asyncio
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AudioPipeline:
    def __init__(self):
        # Create pipeline
        self.pipeline = Pipeline(name="NodeJSAudioPipeline")
        
        # Add nodes
        self.pipeline.add_node(AudioTransform(
            output_sample_rate=16000,
            output_channels=1,
            name="AudioTransform"
        ))
        
        self.pipeline.add_node(VoiceActivityDetector(
            frame_duration_ms=30,
            speech_threshold=0.3,
            name="VAD"
        ))
        
        self.pipeline.add_node(PassThroughNode(name="Output"))
        
        self.speech_detected = False
        self.frame_count = 0
        
    async def initialize(self):
        await self.pipeline.initialize()
        logger.info(f"Pipeline initialized with {len(self.pipeline.nodes)} nodes")
        
    async def process_frame(self, frame_data):
        """Process a single frame through the pipeline"""
        self.frame_count += 1
        
        results = []
        async for result in self.pipeline.process(frame_data):
            results.append(result)
            
            # Check for speech detection
            if isinstance(result, dict) and 'is_speech' in result:
                if result['is_speech'] and not self.speech_detected:
                    self.speech_detected = True
                    logger.info(f"Speech started at frame {self.frame_count}")
                elif not result['is_speech'] and self.speech_detected:
                    self.speech_detected = False
                    logger.info(f"Speech ended at frame {self.frame_count}")
        
        return {
            'frame_number': self.frame_count,
            'results': results,
            'speech_active': self.speech_detected
        }
    
    async def cleanup(self):
        await self.pipeline.cleanup()
`;

  try {
    // Create remote proxy client
    const client = new RemoteProxyClient(config);
    await client.connect();
    
    // Create remote pipeline
    console.log('Creating remote pipeline...');
    const pipeline = new AudioPipeline();
    const remotePipeline = await client.createProxy(pipeline, pythonCode);
    
    // Initialize pipeline
    console.log('Initializing pipeline...');
    await remotePipeline.initialize();
    
    // Process audio frames
    console.log('Processing audio frames...\n');
    
    const audioFrames = generateTestAudioFrames(100);
    let processedCount = 0;
    
    for await (const frame of audioFrames) {
      const result = await remotePipeline.process_frame(frame);
      processedCount++;
      
      // Log interesting events
      if (result.speech_active) {
        console.log(`📢 Frame ${result.frame_number}: Speech active`);
      }
      
      // Progress indicator
      if (processedCount % 20 === 0) {
        console.log(`Processed ${processedCount} frames...`);
      }
    }
    
    console.log(`\n✅ Processed ${processedCount} frames successfully!`);
    
    // Cleanup
    await remotePipeline.cleanup();
    await client.close();
    
  } catch (error) {
    console.error('❌ Error:', error);
  }
}

/**
 * Alternative: Direct execution without proxy
 */
async function runDirectExecution() {
  console.log('\n=== Direct Pipeline Execution ===\n');
  
  // This approach creates the pipeline locally in Node.js
  // and executes each node remotely
  
  console.log('This approach would:');
  console.log('1. Create pipeline structure in Node.js');
  console.log('2. Execute each node remotely via ExecuteNode');
  console.log('3. Handle data flow between nodes in Node.js');
  console.log('4. Useful when you need fine-grained control\n');
}

// Main
async function main() {
  // Run the remote proxy example
  await runWithRemoteProxy();
  
  // Show alternative approach
  await runDirectExecution();
}

if (require.main === module) {
  main().catch(console.error);
}