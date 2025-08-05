/**
 * Bidirectional Pipeline Streaming Example
 * 
 * This demonstrates true bidirectional streaming between Node.js and a Python pipeline
 * using the gRPC StreamNode API, similar to how WebRTC streaming works.
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';
import { EventEmitter } from 'events';

// Load proto definitions
const PROTO_PATH = path.join(__dirname, '../../remote_service/protos/execution.proto');

interface StreamConfig {
  host: string;
  port: number;
  nodeType: string;
  nodeConfig: Record<string, any>;
}

/**
 * Bidirectional streaming client for pipeline execution
 */
class PipelineStreamClient extends EventEmitter {
  private client: any;
  private stream: any;
  private isInitialized: boolean = false;
  
  constructor(private config: StreamConfig) {
    super();
  }
  
  async connect(): Promise<void> {
    const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
      includeDirs: [path.dirname(PROTO_PATH)]
    });
    
    const remoteMedia = grpc.loadPackageDefinition(packageDefinition).remotemedia as any;
    this.client = new remoteMedia.execution.RemoteExecutionService(
      `${this.config.host}:${this.config.port}`,
      grpc.credentials.createInsecure()
    );
    
    // Create bidirectional stream
    this.stream = this.client.StreamNode();
    
    // Handle incoming data from the pipeline
    this.stream.on('data', (response: any) => {
      if (response.error_message) {
        this.emit('error', new Error(response.error_message));
      } else if (response.data) {
        // Deserialize the data
        const data = this.deserializeData(response.data);
        this.emit('data', data);
      }
    });
    
    this.stream.on('end', () => {
      this.emit('end');
    });
    
    this.stream.on('error', (error: Error) => {
      this.emit('error', error);
    });
    
    // Send initialization message
    await this.initialize();
  }
  
  private async initialize(): Promise<void> {
    const initMessage = {
      init: {
        node_type: this.config.nodeType,
        config: this.config.nodeConfig,
        serialization_format: 'json'
      }
    };
    
    this.stream.write(initMessage);
    this.isInitialized = true;
    
    // Wait a bit for initialization
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  
  /**
   * Send data to the pipeline
   */
  async send(data: any): Promise<void> {
    if (!this.isInitialized) {
      throw new Error('Stream not initialized');
    }
    
    const serialized = this.serializeData(data);
    this.stream.write({ data: serialized });
  }
  
  /**
   * Close the stream
   */
  close(): void {
    if (this.stream) {
      this.stream.end();
    }
  }
  
  private serializeData(data: any): Buffer {
    return Buffer.from(JSON.stringify(data));
  }
  
  private deserializeData(buffer: Buffer): any {
    try {
      return JSON.parse(buffer.toString());
    } catch (e) {
      return buffer;
    }
  }
}

/**
 * Example: Create a custom pipeline node that processes streaming data
 */
async function createStreamingPipelineExample() {
  console.log('=== Bidirectional Pipeline Streaming Example ===\n');
  
  // Configuration for our streaming pipeline
  const config: StreamConfig = {
    host: process.env.GRPC_HOST || 'localhost',
    port: parseInt(process.env.GRPC_PORT || '50052'),
    nodeType: 'CodeExecutorNode',
    nodeConfig: {
      code: `
import asyncio
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Stateful processor that accumulates data
class StreamProcessor:
    def __init__(self):
        self.frame_count = 0
        self.speech_frames = 0
        self.buffer = []
        
    async def process(self, data):
        """Process incoming data and return results"""
        self.frame_count += 1
        
        # Parse input
        if isinstance(data, str):
            data = json.loads(data)
        
        # Simulate audio processing
        if 'audio_data' in data:
            # Simple energy calculation
            audio = data['audio_data']
            energy = sum(abs(x) for x in audio) / len(audio) if audio else 0
            
            # Detect speech (simple threshold)
            is_speech = energy > 0.01
            
            if is_speech:
                self.speech_frames += 1
                self.buffer.append(data)
                
                # If we have enough speech frames, process them
                if len(self.buffer) >= 10:  # 200ms of speech
                    result = {
                        'type': 'speech_segment',
                        'frame_count': self.frame_count,
                        'speech_frames': self.speech_frames,
                        'segment_duration_ms': len(self.buffer) * 20,
                        'message': f'Detected speech segment with {len(self.buffer)} frames'
                    }
                    self.buffer = []
                    return result
            else:
                # If we had speech and now silence, end the segment
                if self.buffer:
                    result = {
                        'type': 'speech_end',
                        'frame_count': self.frame_count,
                        'total_speech_frames': len(self.buffer),
                        'message': f'Speech ended after {len(self.buffer)} frames'
                    }
                    self.buffer = []
                    return result
        
        # Return frame acknowledgment
        return {
            'type': 'frame_processed',
            'frame_number': self.frame_count,
            'timestamp': data.get('timestamp', 0)
        }

# Global processor instance
processor = StreamProcessor()

# Entry point for streaming
async def process(input_data):
    return await processor.process(input_data)
`,
      entry_point: 'process'
    }
  };
  
  // Create streaming client
  const client = new PipelineStreamClient(config);
  
  // Set up event handlers
  client.on('data', (data) => {
    if (data.type === 'speech_segment') {
      console.log(`🎤 Speech Segment: ${data.message}`);
    } else if (data.type === 'speech_end') {
      console.log(`🔇 Speech Ended: ${data.message}`);
    } else if (data.type === 'frame_processed' && data.frame_number % 50 === 0) {
      console.log(`📊 Processed ${data.frame_number} frames`);
    }
  });
  
  client.on('error', (error) => {
    console.error('Stream error:', error);
  });
  
  client.on('end', () => {
    console.log('Stream ended');
  });
  
  try {
    // Connect to the streaming endpoint
    console.log('Connecting to streaming endpoint...');
    await client.connect();
    console.log('Connected! Starting to stream audio data...\n');
    
    // Simulate streaming audio data
    const sampleRate = 16000;
    const frameDurationMs = 20;
    const samplesPerFrame = (sampleRate * frameDurationMs) / 1000;
    const totalFrames = 250; // 5 seconds
    
    for (let i = 0; i < totalFrames; i++) {
      // Generate audio frame with varying energy
      const audioData = new Array(samplesPerFrame);
      
      // Simulate speech patterns
      const isSpeechPeriod = (i > 50 && i < 100) || (i > 150 && i < 200);
      const amplitude = isSpeechPeriod ? 0.3 : 0.01;
      
      for (let j = 0; j < samplesPerFrame; j++) {
        // Generate audio with some noise
        audioData[j] = (Math.random() - 0.5) * amplitude;
      }
      
      // Send frame to pipeline
      await client.send({
        audio_data: audioData,
        timestamp: Date.now(),
        frame_number: i,
        sample_rate: sampleRate
      });
      
      // Simulate real-time streaming
      await new Promise(resolve => setTimeout(resolve, frameDurationMs));
    }
    
    console.log('\nFinished streaming audio data');
    
    // Wait a bit for final responses
    await new Promise(resolve => setTimeout(resolve, 500));
    
    // Close the stream
    client.close();
    
  } catch (error) {
    console.error('Error:', error);
  }
}

/**
 * Example: Stream data through an actual pipeline
 */
async function streamThroughPipeline() {
  console.log('\n=== Streaming Through Actual Pipeline ===\n');
  
  // This example would create a real pipeline with AudioTransform, VAD, etc.
  // For now, we'll use a custom node that simulates pipeline behavior
  
  const config: StreamConfig = {
    host: 'localhost',
    port: 50052,
    nodeType: 'SerializedClassExecutorNode',
    nodeConfig: {} // Would include serialized pipeline
  };
  
  // Implementation would be similar to above
  console.log('This would stream through a real pipeline with AudioTransform, VAD, etc.');
}

/**
 * Helper: Create a Node.js stream that generates audio
 */
function createAudioStream() {
  const { Readable } = require('stream');
  
  let frameCount = 0;
  const maxFrames = 250;
  
  return new Readable({
    objectMode: true,
    read() {
      if (frameCount >= maxFrames) {
        this.push(null);
        return;
      }
      
      const frame = {
        id: frameCount++,
        timestamp: Date.now(),
        audio: new Float32Array(320) // 20ms at 16kHz
      };
      
      this.push(frame);
      
      // Pace the stream
      setTimeout(() => {}, 20);
    }
  });
}

// Main function
async function main() {
  try {
    // Run the bidirectional streaming example
    await createStreamingPipelineExample();
    
    // You could also run the pipeline example
    // await streamThroughPipeline();
    
    console.log('\n✅ All examples completed successfully!');
    
  } catch (error) {
    console.error('❌ Error in main:', error);
  }
}

// Run if this is the main module
if (require.main === module) {
  main().then(() => process.exit(0)).catch(() => process.exit(1));
}

export { PipelineStreamClient, createAudioStream };