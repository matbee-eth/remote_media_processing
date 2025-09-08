/**
 * RemoteProxyClient v2 - Execute ANY server node remotely
 * 
 * This implementation allows executing any node registered on the server,
 * not just arbitrary Python code.
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { promisify } from 'util';
import * as path from 'path';

// Configuration interface
export interface RemoteExecutorConfig {
  host: string;
  port: number;
  protocol?: 'grpc' | 'http';
  timeout?: number;
  sslEnabled?: boolean;
}

// Node configuration
export interface NodeConfig {
  [key: string]: any;
}

// Execution options
export interface ExecutionOptions {
  timeout?: number;
  enable_gpu?: boolean;
  priority?: string;
}

// Node proxy that wraps the process method
export interface RemoteNodeProxy {
  process(data: any): Promise<any>;
  processStream?(onData: (data: any) => void, onError?: (error: Error) => void): StreamHandle;
}

// Stream handle for bidirectional streaming
export interface StreamHandle {
  send(data: any): Promise<void>;
  close(): Promise<void>;
  sessionId: string;
}

// Load protobuf
const PROTO_PATH = path.join(__dirname, '../../../remote_service/protos/execution.proto');

/**
 * RemoteProxyClient - Execute any server node remotely
 */
export class RemoteProxyClient {
  private client: any;
  private config: RemoteExecutorConfig;
  private executeNode: any;
  private connected: boolean = false;
  private packageDefinition: any;
  private remoteMedia: any;

  constructor(config: RemoteExecutorConfig) {
    this.config = config;
    
    // Load protobuf
    this.packageDefinition = protoLoader.loadSync(PROTO_PATH, {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
      includeDirs: [path.dirname(PROTO_PATH)]
    });
    
    this.remoteMedia = grpc.loadPackageDefinition(this.packageDefinition).remotemedia as any;
  }

  /**
   * Connect to the remote service
   */
  async connect(): Promise<void> {
    if (this.connected) return;

    const address = `${this.config.host}:${this.config.port}`;
    
    if (this.config.sslEnabled) {
      const credentials = grpc.credentials.createSsl();
      this.client = new this.remoteMedia.execution.RemoteExecutionService(address, credentials);
    } else {
      this.client = new this.remoteMedia.execution.RemoteExecutionService(
        address,
        grpc.credentials.createInsecure()
      );
    }

    this.executeNode = promisify(this.client.ExecuteNode.bind(this.client));
    this.connected = true;
  }

  /**
   * Create a proxy for a specific node type
   * 
   * @param nodeType - The type of node to create (e.g., 'TransformersPipelineNode', 'AudioTransform')
   * @param config - Node configuration parameters
   * @param options - Execution options
   */
  async createNodeProxy(
    nodeType: string,
    config: NodeConfig = {},
    options: ExecutionOptions = {}
  ): Promise<RemoteNodeProxy> {
    if (!this.connected) {
      await this.connect();
    }

    // Convert config to string map for gRPC
    const configMap: Record<string, string> = {};
    for (const [key, value] of Object.entries(config)) {
      if (typeof value === 'object') {
        configMap[key] = JSON.stringify(value);
      } else {
        configMap[key] = String(value);
      }
    }

    // Create proxy object
    const proxy: RemoteNodeProxy = {
      process: async (data: any): Promise<any> => {
        const request = {
          node_type: nodeType,
          config: configMap,
          input_data: Buffer.from(JSON.stringify(data)),
          serialization_format: 'json',
          options: {
            timeout: options.timeout || this.config.timeout || 30.0,
            enable_gpu: options.enable_gpu || false,
            priority: options.priority || 'normal'
          }
        };

        const response = await this.executeNode(request);
        
        if (response.status === 'EXECUTION_STATUS_SUCCESS') {
          return JSON.parse(response.output_data.toString());
        } else {
          throw new Error(`Node execution failed: ${response.error_message}`);
        }
      },

      processStream: (onData: (data: any) => void, onError?: (error: Error) => void): StreamHandle => {
        const stream = this.client.StreamNode();
        
        // Send initialization
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
          sessionId: 'stream-' + Date.now()
        };
      }
    };
    
    return proxy;
  }

  /**
   * Create a proxy for a custom Python object (using SerializedClassExecutorNode)
   */
  async createObjectProxy<T extends object>(
    obj: T,
    dependencies?: string[]
  ): Promise<any> {
    // This would use SerializedClassExecutorNode or similar
    // For now, throw not implemented
    throw new Error('Object proxy not yet implemented. Use createNodeProxy for registered nodes.');
  }

  /**
   * Get list of available nodes
   */
  async listNodes(category?: string): Promise<any[]> {
    if (!this.connected) {
      await this.connect();
    }

    const listNodes = promisify(this.client.ListNodes.bind(this.client));
    const response = await listNodes({ category: category || '' });
    
    return response.available_nodes;
  }

  /**
   * Get server status
   */
  async getStatus(): Promise<any> {
    if (!this.connected) {
      await this.connect();
    }

    const getStatus = promisify(this.client.GetStatus.bind(this.client));
    const response = await getStatus({
      include_metrics: true,
      include_sessions: true
    });
    
    return response;
  }

  /**
   * Close the connection
   */
  async close(): Promise<void> {
    this.connected = false;
    // grpc-js doesn't require explicit closing
  }

  /**
   * Python-style context manager support
   */
  async __aenter__(): Promise<RemoteProxyClient> {
    await this.connect();
    return this;
  }

  async __aexit__(): Promise<void> {
    await this.close();
  }
}

/**
 * Helper function for Python-style async with pattern
 */
export async function withRemoteProxy<T>(
  config: RemoteExecutorConfig,
  callback: (client: RemoteProxyClient) => Promise<T>
): Promise<T> {
  const client = new RemoteProxyClient(config);
  try {
    await client.connect();
    return await callback(client);
  } finally {
    await client.close();
  }
}

/**
 * Convenience functions for common nodes
 */
export class RemoteNodes {
  constructor(private client: RemoteProxyClient) {}

  async audioTransform(config: {
    sampleRate?: number;
    channels?: number;
    dtype?: string;
  } = {}) {
    return this.client.createNodeProxy('AudioTransform', config);
  }

  async transformersPipeline(config: {
    task: string;
    model?: string;
    device?: string | number;
    model_kwargs?: Record<string, any>;
  }) {
    return this.client.createNodeProxy('TransformersPipelineNode', config);
  }

  async textProcessor(config: {} = {}) {
    return this.client.createNodeProxy('TextProcessorNode', config);
  }

  async calculator(config: {
    operation?: string;
    factor?: number;
  } = {}) {
    return this.client.createNodeProxy('CalculatorNode', config);
  }

  async codeExecutor(config: {
    code: string;
    entry_point?: string;
  }) {
    return this.client.createNodeProxy('CodeExecutorNode', config);
  }
}