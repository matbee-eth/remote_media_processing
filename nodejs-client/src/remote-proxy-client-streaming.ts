/**
 * RemoteProxyClient with Stream and Generator Support for Node.js/TypeScript
 * 
 * Provides transparent remote execution with support for:
 * - Node.js Readable/Writable/Transform streams
 * - Async generators and iterators
 * - Batched fetching for efficiency
 * - Automatic stream/generator detection
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { promisify } from 'util';
import * as path from 'path';
import { Readable, Writable, Transform, pipeline } from 'stream';
import { promisify as utilPromisify } from 'util';

const pipelineAsync = utilPromisify(pipeline);

// Configuration interface
export interface RemoteExecutorConfig {
  host: string;
  port: number;
  protocol?: 'grpc' | 'http';
  timeout?: number;
  sslEnabled?: boolean;
  pipPackages?: string[];
  batchSize?: number; // For generator batching
}

// Remote proxy type with stream support
export type RemoteProxy<T> = {
  [K in keyof T]: T[K] extends (...args: any[]) => any
    ? ReturnType<T[K]> extends AsyncGenerator<infer Y, any, any>
      ? (...args: Parameters<T[K]>) => RemoteAsyncGenerator<Y>
      : ReturnType<T[K]> extends Generator<infer Y, any, any>
        ? (...args: Parameters<T[K]>) => RemoteAsyncGenerator<Y>
        : ReturnType<T[K]> extends NodeJS.ReadableStream
          ? (...args: Parameters<T[K]>) => Promise<RemoteReadableStream>
          : ReturnType<T[K]> extends NodeJS.WritableStream
            ? (...args: Parameters<T[K]>) => Promise<RemoteWritableStream>
            : (...args: Parameters<T[K]>) => Promise<Awaited<ReturnType<T[K]>>>
    : never;
};

// Generator detection interface
interface GeneratorMarker {
  __generator__: boolean;
  generator_id: string;
  is_async?: boolean;
}

// Stream detection interface
interface StreamMarker {
  __stream__: boolean;
  stream_id: string;
  stream_type: 'readable' | 'writable' | 'transform';
}

// Load protobuf
const PROTO_PATH = path.join(__dirname, '../../../remote_service/protos/execution.proto');

/**
 * Remote async generator that fetches items from the server
 */
export class RemoteAsyncGenerator<T> implements AsyncIterable<T> {
  private client: any;
  private generatorId: string;
  private batchSize: number;
  private buffer: T[] = [];
  private exhausted: boolean = false;
  private closed: boolean = false;

  constructor(client: any, generatorId: string, batchSize: number = 10) {
    this.client = client;
    this.generatorId = generatorId;
    this.batchSize = batchSize;
  }

  async *[Symbol.asyncIterator](): AsyncIterator<T> {
    try {
      while (!this.exhausted && !this.closed) {
        // Fetch next batch if buffer is empty
        if (this.buffer.length === 0) {
          const getNextBatch = promisify(this.client.GetNextBatch.bind(this.client));
          const response = await getNextBatch({
            generator_id: this.generatorId,
            batch_size: this.batchSize,
            serialization_format: 'json'
          });

          if (response.status !== 'EXECUTION_STATUS_SUCCESS') {
            throw new Error(`Generator error: ${response.error_message}`);
          }

          if (response.items && response.items.length > 0) {
            // Deserialize items
            this.buffer = response.items.map((item: Buffer) => 
              JSON.parse(item.toString())
            );
          }

          if (!response.has_more) {
            this.exhausted = true;
          }
        }

        // Yield items from buffer
        while (this.buffer.length > 0) {
          yield this.buffer.shift()!;
        }
      }
    } finally {
      await this.close();
    }
  }

  async close(): Promise<void> {
    if (!this.closed) {
      this.closed = true;
      try {
        const closeGenerator = promisify(this.client.CloseGenerator.bind(this.client));
        await closeGenerator({ generator_id: this.generatorId });
      } catch (error) {
        console.warn('Error closing generator:', error);
      }
    }
  }
}

/**
 * Remote readable stream that receives data from the server
 */
export class RemoteReadableStream extends Readable {
  private client: any;
  private streamId: string;
  private _closed: boolean = false;

  constructor(client: any, streamId: string) {
    super({ objectMode: true });
    this.client = client;
    this.streamId = streamId;
    this.startStreaming();
  }

  private async startStreaming() {
    try {
      // Create bidirectional stream
      const stream = this.client.StreamObject();
      
      // Send init message
      stream.write({
        init: {
          session_id: this.streamId,
          serialization_format: 'json'
        }
      });

      // Handle incoming data
      stream.on('data', (response: any) => {
        if (response.payload === 'data' && response.data) {
          const data = JSON.parse(response.data.toString());
          if (!this.push(data)) {
            // Backpressure - pause the stream
            stream.pause();
          }
        } else if (response.payload === 'error') {
          this.destroy(new Error(response.error));
        }
      });

      stream.on('end', () => {
        this.push(null); // Signal end of stream
      });

      stream.on('error', (error: Error) => {
        this.destroy(error);
      });

      // Handle backpressure
      this.on('drain', () => {
        stream.resume();
      });

    } catch (error) {
      this.destroy(error as Error);
    }
  }

  _read(size: number): void {
    // Reading is handled by the streaming connection
  }

  _destroy(error: Error | null, callback: (error?: Error | null) => void): void {
    this._closed = true;
    // Clean up stream resources
    callback(error);
  }
}

/**
 * Remote writable stream that sends data to the server
 */
export class RemoteWritableStream extends Writable {
  private client: any;
  private streamId: string;
  private stream: any;
  private initialized: boolean = false;

  constructor(client: any, streamId: string) {
    super({ objectMode: true });
    this.client = client;
    this.streamId = streamId;
    this.initStream();
  }

  private async initStream() {
    this.stream = this.client.StreamObject();
    
    // Send init message
    this.stream.write({
      init: {
        session_id: this.streamId,
        serialization_format: 'json'
      }
    });

    // Handle responses
    this.stream.on('data', (response: any) => {
      if (response.payload === 'error') {
        this.destroy(new Error(response.error));
      }
    });

    this.stream.on('error', (error: Error) => {
      this.destroy(error);
    });

    this.initialized = true;
    this.emit('ready');
  }

  async _write(chunk: any, encoding: string, callback: (error?: Error | null) => void): Promise<void> {
    if (!this.initialized) {
      await new Promise(resolve => this.once('ready', resolve));
    }

    try {
      const data = Buffer.from(JSON.stringify(chunk));
      this.stream.write({ data }, callback);
    } catch (error) {
      callback(error as Error);
    }
  }

  _final(callback: (error?: Error | null) => void): void {
    if (this.stream) {
      this.stream.end(callback);
    } else {
      callback();
    }
  }

  _destroy(error: Error | null, callback: (error?: Error | null) => void): void {
    if (this.stream) {
      this.stream.destroy();
    }
    callback(error);
  }
}

/**
 * RemoteProxyClient - Execute Python code remotely with stream support
 */
export class RemoteProxyClient {
  private client: any;
  private config: RemoteExecutorConfig;
  private executeNode: any;
  private executeObjectMethod: any;
  private initGenerator: any;
  private connected: boolean = false;
  private sessions: Map<string, string> = new Map(); // object -> session_id mapping

  constructor(config: RemoteExecutorConfig) {
    this.config = config;
  }

  /**
   * Connect to the remote service
   */
  async connect(): Promise<void> {
    if (this.connected) return;

    const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
      includeDirs: [path.dirname(PROTO_PATH)]
    });

    const remoteMedia = grpc.loadPackageDefinition(packageDefinition).remotemedia as any;
    const address = `${this.config.host}:${this.config.port}`;
    
    if (this.config.sslEnabled) {
      const credentials = grpc.credentials.createSsl();
      this.client = new remoteMedia.execution.RemoteExecutionService(address, credentials);
    } else {
      this.client = new remoteMedia.execution.RemoteExecutionService(
        address,
        grpc.credentials.createInsecure()
      );
    }

    this.executeNode = promisify(this.client.ExecuteNode.bind(this.client));
    this.executeObjectMethod = promisify(this.client.ExecuteObjectMethod.bind(this.client));
    this.initGenerator = promisify(this.client.InitGenerator.bind(this.client));
    this.connected = true;
  }

  /**
   * Create a proxy for a JavaScript object that executes methods remotely
   */
  async createProxy<T extends object>(
    obj: T,
    pythonCode?: string,
    dependencies?: string[]
  ): Promise<RemoteProxy<T>> {
    if (!this.connected) {
      await this.connect();
    }

    // Generate Python code if not provided
    const code = pythonCode || this.generatePythonCode(obj);
    
    // Create proxy object
    const proxy: any = {};
    const objId = this.generateObjectId();
    
    // Get all methods from the object
    const methods = this.getMethods(obj);
    
    for (const methodName of methods) {
      proxy[methodName] = async (...args: any[]) => {
        const result = await this.executeRemoteMethod(
          code, 
          methodName, 
          args, 
          dependencies,
          objId
        );

        // Check if result is a generator marker
        if (this.isGeneratorMarker(result)) {
          return new RemoteAsyncGenerator(
            this.client,
            result.generator_id,
            this.config.batchSize || 10
          );
        }

        // Check if result is a stream marker
        if (this.isStreamMarker(result)) {
          switch (result.stream_type) {
            case 'readable':
              return new RemoteReadableStream(this.client, result.stream_id);
            case 'writable':
              return new RemoteWritableStream(this.client, result.stream_id);
            case 'transform':
              // Transform streams can be implemented as a combination
              throw new Error('Transform streams not yet implemented');
            default:
              throw new Error(`Unknown stream type: ${result.stream_type}`);
          }
        }

        return result;
      };
    }
    
    return proxy as RemoteProxy<T>;
  }

  /**
   * Create a proxy for a Node.js Stream
   */
  async createStreamProxy(
    stream: NodeJS.ReadableStream | NodeJS.WritableStream,
    pythonCode?: string
  ): Promise<RemoteReadableStream | RemoteWritableStream> {
    if (!this.connected) {
      await this.connect();
    }

    const streamId = this.generateObjectId();
    
    if (stream instanceof Readable) {
      return new RemoteReadableStream(this.client, streamId);
    } else if (stream instanceof Writable) {
      return new RemoteWritableStream(this.client, streamId);
    } else {
      throw new Error('Unsupported stream type');
    }
  }

  /**
   * Execute a remote method
   */
  private async executeRemoteMethod(
    classCode: string,
    methodName: string,
    args: any[],
    dependencies?: string[],
    objId?: string
  ): Promise<any> {
    // Check if this method might return a generator
    const isGeneratorMethod = methodName.includes('generate') || 
                            methodName.includes('stream') || 
                            methodName.includes('iter');

    const sessionId = this.sessions.get(objId || '');

    // First request - include code package
    const request: any = {
      config: {},
      serialization_format: 'json',
      method_name: methodName,
      method_args_data: Buffer.from(JSON.stringify(args)),
      method_kwargs_data: Buffer.from(JSON.stringify({})),
      dependencies: dependencies || []
    };

    if (sessionId) {
      request.session_id = sessionId;
    } else {
      // Create the code package for first call
      const executionCode = `
${classCode}

# Global instance storage
_instance = None

def get_or_create_instance():
    global _instance
    if _instance is None:
        _instance = ${this.getClassName(classCode)}()
    return _instance

def execute_method(method_name, args, kwargs):
    instance = get_or_create_instance()
    method = getattr(instance, method_name)
    result = method(*args, **kwargs)
    
    # Handle generators
    import inspect
    if inspect.isgenerator(result) or inspect.isasyncgen(result):
        return {"__generator__": True, "generator_id": "temp_id", "is_async": inspect.isasyncgen(result)}
    
    # Handle async methods
    import asyncio
    if asyncio.iscoroutine(result):
        result = asyncio.run(result)
    
    return result
`;
      request.code_package = Buffer.from(executionCode);
    }

    const response = await this.executeObjectMethod(request);
    
    if (response.status === 'EXECUTION_STATUS_SUCCESS') {
      // Store session ID for future calls
      if (response.session_id && objId) {
        this.sessions.set(objId, response.session_id);
      }

      const result = JSON.parse(response.result_data.toString());
      
      // If it's a generator marker from the initial method call,
      // we need to properly initialize it
      if (this.isGeneratorMarker(result) && isGeneratorMethod) {
        // Initialize the actual generator
        const genResponse = await this.initGenerator({
          session_id: response.session_id || sessionId,
          method_name: methodName,
          method_args_data: Buffer.from(JSON.stringify(args)),
          serialization_format: 'json',
          method_kwargs_data: Buffer.from(JSON.stringify({}))
        });

        if (genResponse.status === 'EXECUTION_STATUS_SUCCESS') {
          return {
            __generator__: true,
            generator_id: genResponse.generator_id,
            is_async: result.is_async
          };
        } else {
          throw new Error(`Failed to initialize generator: ${genResponse.error_message}`);
        }
      }

      return result;
    } else {
      throw new Error(`Remote execution failed: ${response.error_message}`);
    }
  }

  /**
   * Generate Python code from JavaScript object
   */
  private generatePythonCode(obj: any): string {
    const className = obj.constructor.name;
    const methods = this.getMethods(obj);
    
    let code = `class ${className}:\n`;
    code += `    def __init__(self):\n`;
    
    // Add properties
    for (const [key, value] of Object.entries(obj)) {
      if (typeof value !== 'function') {
        code += `        self.${key} = ${JSON.stringify(value)}\n`;
      }
    }
    
    // Add placeholder for methods
    for (const method of methods) {
      code += `\n    def ${method}(self, *args, **kwargs):\n`;
      code += `        raise NotImplementedError("Method ${method} not implemented")\n`;
    }
    
    return code;
  }

  /**
   * Get all methods from an object
   */
  private getMethods(obj: any): string[] {
    const methods: string[] = [];
    const proto = Object.getPrototypeOf(obj);
    
    // Get instance methods
    for (const name of Object.getOwnPropertyNames(obj)) {
      if (typeof obj[name] === 'function') {
        methods.push(name);
      }
    }
    
    // Get prototype methods
    for (const name of Object.getOwnPropertyNames(proto)) {
      if (name !== 'constructor' && typeof proto[name] === 'function') {
        methods.push(name);
      }
    }
    
    return [...new Set(methods)];
  }

  /**
   * Extract class name from Python code
   */
  private getClassName(code: string): string {
    const match = code.match(/class\s+(\w+)/);
    return match ? match[1] : 'UnknownClass';
  }

  /**
   * Generate a unique object ID
   */
  private generateObjectId(): string {
    return `obj_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  /**
   * Check if result is a generator marker
   */
  private isGeneratorMarker(result: any): result is GeneratorMarker {
    return result && typeof result === 'object' && result.__generator__ === true;
  }

  /**
   * Check if result is a stream marker
   */
  private isStreamMarker(result: any): result is StreamMarker {
    return result && typeof result === 'object' && result.__stream__ === true;
  }

  /**
   * Close the client
   */
  async close(): Promise<void> {
    this.connected = false;
    this.sessions.clear();
  }
}

/**
 * Helper function for async with pattern
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