/**
 * Execute a Python Pipeline from Node.js with Streaming Input
 * 
 * This example demonstrates how to:
 * 1. Create and execute a Python pipeline remotely
 * 2. Stream data from Node.js (generator/stream) into the pipeline
 * 3. Receive processed results back as a stream
 * 
 * Similar to the WebRTC example, but using Node.js streams instead of WebRTC
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';
import { promisify } from 'util';

// Load proto definitions
const PROTO_PATH = path.join(__dirname, '../../remote_service/protos/execution.proto');

interface PipelineConfig {
  host: string;
  port: number;
  ssl?: boolean;
}

/**
 * Example: Audio stream generator that simulates microphone input
 * In a real application, this could be actual microphone data
 */
async function* generateAudioFrames(durationSeconds: number = 5): AsyncGenerator<AudioFrame> {
  const sampleRate = 16000; // 16kHz
  const channels = 1;
  const frameDurationMs = 20; // 20ms frames (like WebRTC)
  const samplesPerFrame = (sampleRate * frameDurationMs) / 1000;
  const totalFrames = (durationSeconds * 1000) / frameDurationMs;
  
  console.log(`Generating ${totalFrames} audio frames over ${durationSeconds} seconds...`);
  
  for (let i = 0; i < totalFrames; i++) {
    // Simulate audio data - in reality this would be from a microphone
    const audioData = new Float32Array(samplesPerFrame);
    
    // Generate a simple sine wave for testing
    const frequency = 440; // A4 note
    for (let j = 0; j < samplesPerFrame; j++) {
      const t = (i * samplesPerFrame + j) / sampleRate;
      audioData[j] = Math.sin(2 * Math.PI * frequency * t) * 0.3;
    }
    
    yield {
      timestamp: Date.now(),
      frameNumber: i,
      sampleRate,
      channels,
      data: audioData,
      durationMs: frameDurationMs
    };
    
    // Simulate real-time by waiting
    await new Promise(resolve => setTimeout(resolve, frameDurationMs));
  }
  
  console.log('Audio generation complete');
}

interface AudioFrame {
  timestamp: number;
  frameNumber: number;
  sampleRate: number;
  channels: number;
  data: Float32Array;
  durationMs: number;
}

/**
 * Create a Python pipeline for audio processing
 * This creates a pipeline similar to the WebRTC example
 */
async function createAudioPipeline(client: any): Promise<string> {
  const pipelineCode = `
import asyncio
from remotemedia.core.pipeline import Pipeline
from remotemedia.nodes.audio import AudioTransform, VoiceActivityDetector
from remotemedia.nodes import PassThroughNode
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create the pipeline
pipeline = Pipeline(name="NodeJSAudioPipeline")

# Add audio transform node
audio_transform = AudioTransform(
    output_sample_rate=16000,
    output_channels=1,
    name="AudioTransform"
)
pipeline.add_node(audio_transform)

# Add VAD node
vad = VoiceActivityDetector(
    frame_duration_ms=30,
    energy_threshold=0.02,
    speech_threshold=0.3,
    include_metadata=True,
    name="VAD"
)
vad.is_streaming = True
pipeline.add_node(vad)

# Add a simple processing node that logs detected speech
class SpeechLogger(PassThroughNode):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.speech_frames = 0
        self.silence_frames = 0
    
    def process(self, data):
        if isinstance(data, dict) and 'is_speech' in data:
            if data['is_speech']:
                self.speech_frames += 1
                if self.speech_frames == 1:
                    logger.info("[Pipeline] Speech started")
            else:
                self.silence_frames += 1
                if self.speech_frames > 0 and self.silence_frames > 10:
                    logger.info(f"[Pipeline] Speech ended after {self.speech_frames} frames")
                    self.speech_frames = 0
                    self.silence_frames = 0
        return data

speech_logger = SpeechLogger(name="SpeechLogger")
pipeline.add_node(speech_logger)

# Store pipeline globally so we can access it
_pipeline = pipeline
logger.info(f"Created pipeline: {pipeline}")

# Function to process a single frame
async def process_frame(frame_data):
    """Process a single audio frame through the pipeline"""
    try:
        # Initialize pipeline if needed
        if not pipeline._is_initialized:
            await pipeline.initialize()
        
        # Process the frame
        results = []
        async for result in pipeline.process(frame_data):
            results.append(result)
        
        return results
    except Exception as e:
        logger.error(f"Error processing frame: {e}")
        return {"error": str(e)}

# Return success
result = {"status": "pipeline_created", "pipeline_id": id(pipeline)}
`;

  // Execute the code to create the pipeline
  const executeNode = promisify(client.ExecuteNode.bind(client));
  
  const response = await executeNode({
    node_type: 'CodeExecutorNode',
    config: {
      code: pipelineCode,
      entry_point: 'process'
    },
    input_data: Buffer.from(JSON.stringify({})),
    serialization_format: 'json',
    options: {
      timeout: 30.0
    }
  });
  
  if (response.status === 'EXECUTION_STATUS_SUCCESS') {
    const result = JSON.parse(response.output_data.toString());
    console.log('Pipeline created:', result);
    return 'pipeline_created';
  } else {
    throw new Error(`Failed to create pipeline: ${response.error_message}`);
  }
}

/**
 * Process audio frames through the pipeline
 */
async function processAudioStream(
  client: any,
  audioGenerator: AsyncGenerator<AudioFrame>
): Promise<void> {
  const executeNode = promisify(client.ExecuteNode.bind(client));
  
  console.log('\nProcessing audio frames through pipeline...\n');
  
  let frameCount = 0;
  const startTime = Date.now();
  
  for await (const frame of audioGenerator) {
    frameCount++;
    
    // Convert audio frame to format expected by pipeline
    const pipelineInput = {
      audio_data: Array.from(frame.data), // Convert Float32Array to regular array
      sample_rate: frame.sampleRate,
      channels: frame.channels,
      timestamp: frame.timestamp,
      frame_number: frame.frameNumber
    };
    
    // Process frame through pipeline
    const processCode = `
# Process the audio frame
import json
frame_data = json.loads(input_data)

# Convert back to numpy array
import numpy as np
audio_array = np.array(frame_data['audio_data'], dtype=np.float32)

# Create audio frame format expected by pipeline
audio_frame = {
    'audio': audio_array,
    'sample_rate': frame_data['sample_rate'],
    'channels': frame_data['channels'],
    'metadata': {
        'timestamp': frame_data['timestamp'],
        'frame_number': frame_data['frame_number']
    }
}

# Process through pipeline
import asyncio
results = asyncio.run(process_frame(audio_frame))

result = {
    'frame_number': frame_data['frame_number'],
    'processed': True,
    'results': results
}
`;

    try {
      const response = await executeNode({
        node_type: 'CodeExecutorNode',
        config: {
          code: processCode,
          entry_point: 'process'
        },
        input_data: Buffer.from(JSON.stringify(pipelineInput)),
        serialization_format: 'json',
        options: {
          timeout: 1.0
        }
      });
      
      if (response.status === 'EXECUTION_STATUS_SUCCESS') {
        const result = JSON.parse(response.output_data.toString());
        
        // Log progress every 50 frames (1 second)
        if (frameCount % 50 === 0) {
          const elapsed = (Date.now() - startTime) / 1000;
          console.log(`Processed ${frameCount} frames in ${elapsed.toFixed(1)}s`);
        }
        
        // Check if speech was detected
        if (result.results && Array.isArray(result.results)) {
          const speechFrames = result.results.filter((r: any) => r?.is_speech === true);
          if (speechFrames.length > 0) {
            console.log(`  [Speech detected in frame ${frameCount}]`);
          }
        }
      } else {
        console.error(`Frame ${frameCount} processing failed:`, response.error_message);
      }
    } catch (error) {
      console.error(`Error processing frame ${frameCount}:`, error);
    }
  }
  
  const totalTime = (Date.now() - startTime) / 1000;
  console.log(`\nProcessing complete: ${frameCount} frames in ${totalTime.toFixed(1)}s`);
  console.log(`Average: ${(frameCount / totalTime).toFixed(1)} frames/second`);
}

/**
 * Main function to demonstrate pipeline streaming
 */
async function main() {
  const config: PipelineConfig = {
    host: process.env.GRPC_HOST || 'localhost',
    port: parseInt(process.env.GRPC_PORT || '50052'),
    ssl: false
  };
  
  console.log('=== Node.js Pipeline Streaming Example ===\n');
  console.log(`Connecting to gRPC server at ${config.host}:${config.port}...`);
  
  // Load proto and create client
  const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
    includeDirs: [path.dirname(PROTO_PATH)]
  });
  
  const remoteMedia = grpc.loadPackageDefinition(packageDefinition).remotemedia as any;
  const client = new remoteMedia.execution.RemoteExecutionService(
    `${config.host}:${config.port}`,
    grpc.credentials.createInsecure()
  );
  
  try {
    // Step 1: Create the pipeline on the server
    console.log('Creating audio processing pipeline...');
    await createAudioPipeline(client);
    
    // Step 2: Generate audio stream and process it
    console.log('\nStarting audio stream...');
    const audioStream = generateAudioFrames(5); // 5 seconds of audio
    
    // Step 3: Process the stream through the pipeline
    await processAudioStream(client, audioStream);
    
    console.log('\n✅ Example completed successfully!');
    
  } catch (error) {
    console.error('❌ Error:', error);
  }
}

// Alternative implementations (commented out to avoid unused variable errors):
// These show how you could also use Node.js Readable streams and Transform streams
// instead of async generators

/*
import { Readable, Transform } from 'stream';

// Example: Using Node.js Readable streams
class AudioStreamSource extends Readable {
  private frameCount = 0;
  private maxFrames = 250; // 5 seconds at 50fps
  
  constructor(options?: any) {
    super({ objectMode: true, ...options });
  }
  
  _read(): void {
    if (this.frameCount >= this.maxFrames) {
      this.push(null); // End stream
      return;
    }
    
    // Generate audio frame
    const frame: AudioFrame = {
      timestamp: Date.now(),
      frameNumber: this.frameCount++,
      sampleRate: 16000,
      channels: 1,
      data: new Float32Array(320), // 20ms at 16kHz
      durationMs: 20
    };
    
    // Add some delay to simulate real-time
    setTimeout(() => {
      this.push(frame);
    }, 20);
  }
}

// Example using transform streams
class AudioProcessor extends Transform {
  constructor(options?: any) {
    super({ objectMode: true, ...options });
  }
  
  _transform(frame: AudioFrame, encoding: string, callback: Function): void {
    // Process the frame
    console.log(`Processing frame ${frame.frameNumber}`);
    
    // Pass it along
    callback(null, frame);
  }
}
*/

// Run the example
if (require.main === module) {
  main().catch(console.error);
}