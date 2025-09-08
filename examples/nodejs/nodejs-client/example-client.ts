
/**
 * Example TypeScript/Node.js client for RemoteMedia Processing SDK
 * 
 * This demonstrates how to use the generated TypeScript interfaces
 * to interact with the remote media processing service.
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { promisify } from 'util';
import type {
  RemoteExecutorConfig,
  NodeType,
  AudioTransformConfig,
  ExecutionResponse,
  RemoteMediaNode
} from '../../remotemedia-types';

// Load the protobuf definitions
const PROTO_PATH = '../../remote_service/protos/execution.proto';
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
  includeDirs: ['../../remote_service/protos']
});

// Create gRPC client
const remoteMedia = grpc.loadPackageDefinition(packageDefinition).remotemedia as any;

/**
 * RemoteMedia client implementation for Node.js/TypeScript
 */
class RemoteMediaClient {
  private client: any;
  private config: RemoteExecutorConfig;

  constructor(config: RemoteExecutorConfig) {
    this.config = config;
    const address = `${config.host}:${config.port}`;

    if (config.sslEnabled) {
      // For SSL connections
      const credentials = grpc.credentials.createSsl();
      this.client = new remoteMedia.execution.RemoteExecutionService(address, credentials);
    } else {
      // For insecure connections
      this.client = new remoteMedia.execution.RemoteExecutionService(
        address,
        grpc.credentials.createInsecure()
      );
    }
  }

  /**
   * Execute a node remotely
   */
  async executeNode<T = any>(
    nodeType: string,
    config: Record<string, any>,
    inputData: any,
    serializationFormat: 'json' | 'pickle' = 'json'
  ): Promise<ExecutionResponse<T>> {
    const executeNode = promisify(this.client.ExecuteNode.bind(this.client));

    // Serialize input data
    const serializedInput = serializationFormat === 'json'
      ? Buffer.from(JSON.stringify(inputData))
      : this.pickleSerialize(inputData);

    // Convert config to string map
    const configMap: Record<string, string> = {};
    for (const [key, value] of Object.entries(config)) {
      configMap[key] = String(value);
    }

    const request = {
      node_type: nodeType,
      config: configMap,
      input_data: serializedInput,
      serialization_format: serializationFormat,
      options: {
        timeout: this.config.timeout || 30.0,
        enable_gpu: false
      }
    };

    try {
      const response = await executeNode(request);

      // Deserialize output
      const outputData = serializationFormat === 'json'
        ? JSON.parse(response.output_data.toString())
        : this.pickleDeserialize(response.output_data);

      return {
        status: response.status === 'EXECUTION_STATUS_SUCCESS' ? 'success' : 'error',
        data: outputData,
        error: response.error_message ? {
          message: response.error_message,
          traceback: response.error_traceback
        } : undefined,
        metrics: response.metrics ? {
          startTimestamp: parseInt(response.metrics.start_timestamp),
          endTimestamp: parseInt(response.metrics.end_timestamp),
          durationMs: parseInt(response.metrics.duration_ms),
          memoryPeakMb: response.metrics.memory_peak_mb,
          cpuTimeMs: response.metrics.cpu_time_ms
        } : undefined
      };
    } catch (error: any) {
      return {
        status: 'error',
        error: {
          message: error.message || 'Unknown error',
          traceback: error.stack
        }
      };
    }
  }

  /**
   * Stream data through a node
   */
  streamNode(
    nodeType: string,
    config: Record<string, any>,
    onData: (data: any) => void,
    onError?: (error: Error) => void
  ): StreamHandle {
    const stream = this.client.StreamNode();

    // Send initialization message
    const configMap: Record<string, string> = {};
    for (const [key, value] of Object.entries(config)) {
      configMap[key] = String(value);
    }

    stream.write({
      init: {
        node_type: nodeType,
        config: configMap,
        serialization_format: 'json'
      }
    });

    // Handle responses
    stream.on('data', (response: any) => {
      if (response.error_message) {
        if (onError) {
          onError(new Error(response.error_message));
        }
      } else if (response.data) {
        try {
          const data = JSON.parse(response.data.toString());
          onData(data);
        } catch (e) {
          if (onError) {
            onError(e as Error);
          }
        }
      }
    });

    stream.on('error', (error: Error) => {
      if (onError) {
        onError(error);
      }
    });

    return {
      send: async (data: any) => {
        const serialized = Buffer.from(JSON.stringify(data));
        stream.write({ data: serialized });
      },
      close: async () => {
        stream.end();
      },
      sessionId: 'stream-' + Date.now() // Simple session ID
    };
  }

  /**
   * Close the client connection
   */
  async close(): Promise<void> {
    // gRPC client doesn't need explicit closing in Node.js
    return Promise.resolve();
  }

  // Placeholder for pickle serialization (would need a proper implementation)
  private pickleSerialize(data: any): Buffer {
    throw new Error('Pickle serialization not implemented in TypeScript. Use JSON format.');
  }

  private pickleDeserialize(data: Buffer): any {
    throw new Error('Pickle deserialization not implemented in TypeScript. Use JSON format.');
  }
}

/**
 * Stream handle for bidirectional streaming
 */
interface StreamHandle {
  send(data: any): Promise<void>;
  close(): Promise<void>;
  readonly sessionId: string;
}

// Example usage
async function main() {
  // Configure the client
  const config: RemoteExecutorConfig = {
    host: 'localhost',
    port: 50052,
    protocol: 'grpc',
    timeout: 30,
    sslEnabled: false
  };

  const client = new RemoteMediaClient(config);

  try {
    // Example 1: Execute an audio transform node
    console.log('Example 1: Audio Transform');
    const audioConfig: AudioTransformConfig = {
      sampleRate: 16000,
      channels: 1,
      dtype: 'float32'
    };

    // Simulated audio data (in real usage, this would be actual audio samples)
    const audioData = {
      samples: new Array(16000).fill(0).map(() => Math.random() * 2 - 1),
      sampleRate: 44100,
      channels: 2
    };

    const result = await client.executeNode(
      'AudioTransform',
      audioConfig,
      audioData
    );

    if (result.status === 'success') {
      console.log('✅ Audio transform completed successfully');
      console.log(`   Duration: ${result.metrics?.durationMs}ms`);
    } else {
      console.error('❌ Audio transform failed:', result.error?.message);
    }

    // Example 2: Stream data through a node
    console.log('\nExample 2: Streaming Calculator');
    const streamHandle = client.streamNode(
      'CalculatorNode',
      { operation: 'multiply', factor: 2 },
      (result) => {
        console.log('   Received result:', result);
      },
      (error) => {
        console.error('   Stream error:', error);
      }
    );

    // Send some data
    for (let i = 1; i <= 5; i++) {
      await streamHandle.send({ value: i });
      await new Promise(resolve => setTimeout(resolve, 100)); // Small delay
    }

    await streamHandle.close();
    console.log('✅ Streaming completed');

  } catch (error) {
    console.error('Error:', error);
  } finally {
    await client.close();
  }
}

// Run the example
if (require.main === module) {
  main().catch(console.error);
}