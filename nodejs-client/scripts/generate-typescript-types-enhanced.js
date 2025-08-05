#!/usr/bin/env node

/**
 * Enhanced TypeScript Type Generator for RemoteMedia Processing SDK
 * 
 * This script generates TypeScript classes with full streaming support:
 * - Concrete classes for each node type
 * - Proxy classes for transparent remote execution
 * - Stream and Generator support
 * - Automatic method detection and type inference
 */

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const fs = require('fs').promises;
const path = require('path');

// Configuration
const GRPC_HOST = process.env.GRPC_HOST || 'localhost';
const GRPC_PORT = process.env.GRPC_PORT || 50052;
const OUTPUT_DIR = process.env.OUTPUT_DIR || './generated-types';

// Proto file paths
const PROTO_PATH = path.join(__dirname, '../remote_service/protos/execution.proto');

class EnhancedTypeScriptGenerator {
  constructor() {
    this.client = null;
    this.nodeDefinitions = null;
  }

  async initialize() {
    try {
      // Load proto definitions
      const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
      });

      const proto = grpc.loadPackageDefinition(packageDefinition);

      // Create gRPC client
      this.client = new proto.remotemedia.execution.RemoteExecutionService(
        `${GRPC_HOST}:${GRPC_PORT}`,
        grpc.credentials.createInsecure()
      );

      console.log(`Connected to gRPC service at ${GRPC_HOST}:${GRPC_PORT}`);
    } catch (error) {
      console.error('Failed to initialize gRPC client:', error);
      throw error;
    }
  }

  async fetchNodeDefinitions() {
    return new Promise((resolve, reject) => {
      this.client.ExportTypeScriptDefinitions({}, (error, response) => {
        if (error) {
          reject(error);
          return;
        }

        if (response.status !== 'EXECUTION_STATUS_SUCCESS') {
          reject(new Error(`Server error: ${response.error_message}`));
          return;
        }

        try {
          this.nodeDefinitions = JSON.parse(response.typescript_definitions);
          console.log(`Fetched ${this.nodeDefinitions.nodes.length} node definitions`);
          resolve(this.nodeDefinitions);
        } catch (parseError) {
          reject(new Error(`Failed to parse node definitions: ${parseError.message}`));
        }
      });
    });
  }

  async generateTypes() {
    await fs.mkdir(OUTPUT_DIR, { recursive: true });

    // Generate base types with streaming support
    await this.generateBaseTypes();

    // Generate concrete node classes
    await this.generateNodeClasses();

    // Generate proxy factory
    await this.generateProxyFactory();

    // Generate index file
    await this.generateIndexFile();

    console.log(`TypeScript definitions generated in ${OUTPUT_DIR}`);
  }

  async generateBaseTypes() {
    const baseTypes = `/**
 * Enhanced Base TypeScript interfaces for RemoteMedia Processing SDK
 * Generated at: ${this.nodeDefinitions.generated_at}
 * Service version: ${this.nodeDefinitions.service_version}
 */

import { Readable, Writable, Transform } from 'stream';

export interface RemoteMediaNode {
  name?: string;
  config?: Record<string, any>;
  process(data: any): any | Promise<any>;
  initialize?(): Promise<void>;
  cleanup?(): Promise<void>;
  flush?(): any | Promise<any>;
}

export interface RemoteExecutorConfig {
  host: string;
  port: number;
  protocol?: 'grpc' | 'http';
  authToken?: string;
  timeout?: number;
  maxRetries?: number;
  sslEnabled?: boolean;
  pipPackages?: string[];
  batchSize?: number;
}

export interface ExecutionOptions {
  timeout?: number;
  maxMemoryMb?: number;
  cpuLimit?: number;
  enableGpu?: boolean;
  priority?: 'low' | 'normal' | 'high';
}

export interface ExecutionResponse<T = any> {
  status: 'success' | 'error';
  data?: T;
  error?: {
    message: string;
    traceback?: string;
  };
  metrics?: {
    startTimestamp: number;
    endTimestamp: number;
    durationMs: number;
    memoryPeakMb?: number;
    cpuTimeMs?: number;
  };
}

export interface StreamHandle {
  send(data: any): Promise<void>;
  close(): Promise<void>;
  readonly sessionId: string;
}

export interface NodeInfo {
  node_type: string;
  category: string;
  description: string;
  parameters: NodeParameter[];
}

export interface NodeParameter {
  name: string;
  type: string;
  required: boolean;
  default_value?: any;
  description?: string;
}

export type SerializationFormat = 'json' | 'pickle';

// Stream and Generator types
export interface RemoteAsyncGenerator<T> extends AsyncIterable<T> {
  close(): Promise<void>;
}

export interface RemoteReadableStream extends Readable {
  readonly streamId: string;
}

export interface RemoteWritableStream extends Writable {
  readonly streamId: string;
}

export interface RemoteTransformStream extends Transform {
  readonly streamId: string;
}

// Method return type detection
export type RemoteMethodReturn<T> = 
  T extends AsyncGenerator<infer Y, any, any> ? RemoteAsyncGenerator<Y> :
  T extends Generator<infer Y, any, any> ? RemoteAsyncGenerator<Y> :
  T extends NodeJS.ReadableStream ? RemoteReadableStream :
  T extends NodeJS.WritableStream ? RemoteWritableStream :
  T extends NodeJS.TransformStream ? RemoteTransformStream :
  T extends Promise<infer U> ? Promise<U> :
  Promise<T>;

// Proxy type transformer
export type RemoteProxy<T> = {
  [K in keyof T]: T[K] extends (...args: any[]) => any
    ? (...args: Parameters<T[K]>) => RemoteMethodReturn<ReturnType<T[K]>>
    : never;
};
`;

    await fs.writeFile(path.join(OUTPUT_DIR, 'base.ts'), baseTypes);
  }

  async generateNodeClasses() {
    const nodes = this.nodeDefinitions.nodes;
    
    for (const node of nodes) {
      const classContent = this.generateNodeClass(node);
      const filename = `${this.kebabCase(node.node_type)}.ts`;
      await fs.writeFile(path.join(OUTPUT_DIR, filename), classContent);
    }
  }

  generateNodeClass(node) {
    const { node_type, types = [], parameters = [], description = '' } = node;

    let content = `/**
 * ${node_type} - Concrete implementation with remote execution support
 * ${description}
 * 
 * Auto-generated from Python node definition
 */

import { RemoteMediaNode, RemoteAsyncGenerator, RemoteReadableStream, RemoteWritableStream } from './base';
import { RemoteExecutor } from './proxy-factory';

`;

    // Generate TypedDict interfaces
    types.forEach(typedDict => {
      const { name, description, fields = [] } = typedDict;
      const uniqueName = name.startsWith(node_type) ? name : `${node_type}${name}`;

      content += `/**
 * ${description}
 */
export interface ${uniqueName} {\n`;

      fields.forEach(field => {
        const { name: fieldName, type, required = true } = field;
        const tsType = this.pythonToTypeScriptType(type);
        const optional = required ? '' : '?';
        content += `  ${fieldName}${optional}: ${tsType};\n`;
      });

      content += '}\n\n';
    });

    // Generate the main class
    content += `/**
 * ${node_type} implementation
 */
export class ${node_type} implements RemoteMediaNode {
`;

    // Constructor with parameters
    if (parameters.length > 0) {
      content += '  constructor(\n';
      parameters.forEach((param, index) => {
        const { name, type, required = true, description = '', default_value } = param;
        const tsType = this.pythonToTypeScriptType(type);
        const optional = required ? '' : '?';
        const isLast = index === parameters.length - 1;
        
        if (description) {
          content += `    /** ${description}`;
          if (default_value !== undefined && !required) {
            content += ` (default: ${JSON.stringify(default_value)})`;
          }
          content += ' */\n';
        }
        
        content += `    public ${name}${optional}: ${tsType}`;
        if (!isLast) content += ',';
        content += '\n';
      });
      content += '  ) {}\n\n';
    } else {
      content += '  constructor() {}\n\n';
    }

    // Generate process method
    const inputType = types.find(t => t.name.includes('Input'));
    const outputType = types.find(t => t.name.includes('Output'));
    
    if (inputType && outputType) {
      const inputTypeName = inputType.name.startsWith(node_type) ? inputType.name : `${node_type}${inputType.name}`;
      const outputTypeName = outputType.name.startsWith(node_type) ? outputType.name : `${node_type}${outputType.name}`;
      
      content += `  async process(data: ${inputTypeName}): Promise<${outputTypeName}> {\n`;
    } else {
      content += '  async process(data: any): Promise<any> {\n';
    }
    
    content += '    // This is a placeholder implementation\n';
    content += '    // In actual usage, this will be executed remotely\n';
    content += '    throw new Error("Direct execution not implemented. Use RemoteExecutor.create() for remote execution.");\n';
    content += '  }\n\n';

    // Add other standard methods
    content += `  async initialize(): Promise<void> {
    // Initialization logic
  }

  async cleanup(): Promise<void> {
    // Cleanup logic
  }

  get_config(): Record<string, any> {
    return {`;

    parameters.forEach((param, index) => {
      const comma = index < parameters.length - 1 ? ',' : '';
      content += `\n      ${param.name}: this.${param.name}${comma}`;
    });

    content += `
    };
  }
}

/**
 * Create a remote proxy for ${node_type}
 * 
 * @example
 * const remote${node_type} = await Remote${node_type}.create(config);
 * const result = await remote${node_type}.process(inputData);
 */
export class Remote${node_type} extends ${node_type} {
  private executor: RemoteExecutor;

  private constructor(executor: RemoteExecutor, ...args: ConstructorParameters<typeof ${node_type}>) {
    super(...args);
    this.executor = executor;
  }

  static async create(
    config: import('./base').RemoteExecutorConfig,
    ...args: ConstructorParameters<typeof ${node_type}>
  ): Promise<Remote${node_type}> {
    const executor = new RemoteExecutor(config);
    await executor.connect();
    const instance = new Remote${node_type}(executor, ...args);
    await executor.createProxy(instance);
    return instance;
  }

  async process(data: Parameters<${node_type}['process']>[0]): ReturnType<${node_type}['process']> {
    return this.executor.executeMethod(this, 'process', [data]);
  }
`;

    // Add streaming methods if detected
    if (this.hasStreamingMethods(node)) {
      content += `
  // Streaming methods
  async *generateStream(data: any): AsyncGenerator<any> {
    const generator = await this.executor.executeMethod(this, 'generateStream', [data]);
    yield* generator as RemoteAsyncGenerator<any>;
  }

  createReadStream(): RemoteReadableStream {
    return this.executor.executeMethod(this, 'createReadStream', []) as any;
  }

  createWriteStream(): RemoteWritableStream {
    return this.executor.executeMethod(this, 'createWriteStream', []) as any;
  }
`;
    }

    content += '}\n';

    return content;
  }

  async generateProxyFactory() {
    const proxyFactory = `/**
 * Proxy Factory for creating remote executors
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { promisify } from 'util';
import * as path from 'path';
import { 
  RemoteExecutorConfig, 
  RemoteAsyncGenerator,
  RemoteReadableStream,
  RemoteWritableStream,
  RemoteProxy
} from './base';

const PROTO_PATH = path.join(__dirname, '../../../remote_service/protos/execution.proto');

export class RemoteExecutor {
  private client: any;
  private config: RemoteExecutorConfig;
  private connected: boolean = false;
  private sessions: Map<any, string> = new WeakMap();

  constructor(config: RemoteExecutorConfig) {
    this.config = config;
  }

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
    const address = \`\${this.config.host}:\${this.config.port}\`;
    
    if (this.config.sslEnabled) {
      const credentials = grpc.credentials.createSsl();
      this.client = new remoteMedia.execution.RemoteExecutionService(address, credentials);
    } else {
      this.client = new remoteMedia.execution.RemoteExecutionService(
        address,
        grpc.credentials.createInsecure()
      );
    }

    this.connected = true;
  }

  async createProxy<T extends object>(instance: T): Promise<void> {
    // Initialize remote session for this instance
    const className = instance.constructor.name;
    const config = (instance as any).get_config ? (instance as any).get_config() : {};
    
    // Store session ID for this instance
    // Implementation would create remote session and store ID
  }

  async executeMethod(instance: any, methodName: string, args: any[]): Promise<any> {
    // Execute method remotely
    const executeObjectMethod = promisify(this.client.ExecuteObjectMethod.bind(this.client));
    
    // Get or create session for this instance
    const sessionId = this.sessions.get(instance);
    
    const request: any = {
      method_name: methodName,
      method_args_data: Buffer.from(JSON.stringify(args)),
      method_kwargs_data: Buffer.from(JSON.stringify({})),
      serialization_format: 'json'
    };

    if (sessionId) {
      request.session_id = sessionId;
    } else {
      // First call - include instance information
      request.config = (instance as any).get_config ? (instance as any).get_config() : {};
    }

    const response = await executeObjectMethod(request);
    
    if (response.status === 'EXECUTION_STATUS_SUCCESS') {
      if (response.session_id && !sessionId) {
        this.sessions.set(instance, response.session_id);
      }
      
      const result = JSON.parse(response.result_data.toString());
      
      // Handle generators
      if (result && result.__generator__) {
        return this.createGeneratorProxy(result.generator_id);
      }
      
      // Handle streams
      if (result && result.__stream__) {
        return this.createStreamProxy(result.stream_id, result.stream_type);
      }
      
      return result;
    } else {
      throw new Error(\`Remote execution failed: \${response.error_message}\`);
    }
  }

  private createGeneratorProxy(generatorId: string): RemoteAsyncGenerator<any> {
    // Return a generator proxy that fetches items from remote
    const client = this.client;
    const batchSize = this.config.batchSize || 10;
    
    return {
      [Symbol.asyncIterator]: async function* () {
        const getNextBatch = promisify(client.GetNextBatch.bind(client));
        let exhausted = false;
        
        while (!exhausted) {
          const response = await getNextBatch({
            generator_id: generatorId,
            batch_size: batchSize,
            serialization_format: 'json'
          });
          
          if (response.status !== 'EXECUTION_STATUS_SUCCESS') {
            throw new Error(\`Generator error: \${response.error_message}\`);
          }
          
          for (const item of response.items || []) {
            yield JSON.parse(item.toString());
          }
          
          exhausted = !response.has_more;
        }
      },
      
      close: async () => {
        const closeGenerator = promisify(client.CloseGenerator.bind(client));
        await closeGenerator({ generator_id: generatorId });
      }
    };
  }

  private createStreamProxy(streamId: string, streamType: string): any {
    // Implementation would return appropriate stream proxy
    throw new Error('Stream proxies not yet implemented');
  }

  async close(): Promise<void> {
    this.connected = false;
  }
}

/**
 * Create a remote proxy for any class
 */
export async function createRemoteProxy<T extends object>(
  ClassConstructor: new (...args: any[]) => T,
  config: RemoteExecutorConfig,
  ...args: ConstructorParameters<typeof ClassConstructor>
): Promise<RemoteProxy<T>> {
  const executor = new RemoteExecutor(config);
  await executor.connect();
  
  const instance = new ClassConstructor(...args);
  await executor.createProxy(instance);
  
  // Create proxy that intercepts all method calls
  return new Proxy(instance, {
    get(target, prop) {
      const value = (target as any)[prop];
      if (typeof value === 'function') {
        return async (...methodArgs: any[]) => {
          return executor.executeMethod(target, prop as string, methodArgs);
        };
      }
      return value;
    }
  }) as RemoteProxy<T>;
}
`;

    await fs.writeFile(path.join(OUTPUT_DIR, 'proxy-factory.ts'), proxyFactory);
  }

  async generateIndexFile() {
    const nodes = this.nodeDefinitions.nodes;

    let content = `/**
 * RemoteMedia Processing SDK TypeScript Definitions with Streaming Support
 * Generated at: ${this.nodeDefinitions.generated_at}
 * Service version: ${this.nodeDefinitions.service_version}
 */

// Base interfaces and types
export * from './base';

// Proxy factory and remote execution
export * from './proxy-factory';

// Node implementations
`;

    nodes.forEach(node => {
      const filename = this.kebabCase(node.node_type);
      content += `export * from './${filename}';\n`;
    });

    content += `
// Convenience re-exports
export { RemoteExecutor, createRemoteProxy } from './proxy-factory';
export type { 
  RemoteAsyncGenerator,
  RemoteReadableStream,
  RemoteWritableStream,
  RemoteTransformStream,
  RemoteProxy
} from './base';
`;

    await fs.writeFile(path.join(OUTPUT_DIR, 'index.ts'), content);
  }

  // Utility methods
  pythonToTypeScriptType(pythonType) {
    const typeMap = {
      'str': 'string',
      'int': 'number',
      'float': 'number',
      'bool': 'boolean',
      'list': 'any[]',
      'dict': 'Record<string, any>',
      'Any': 'any',
      'Generator': 'AsyncGenerator<any>',
      'AsyncGenerator': 'AsyncGenerator<any>',
      'Iterator': 'AsyncIterator<any>',
      'AsyncIterator': 'AsyncIterator<any>',
      'Stream': 'NodeJS.ReadableStream | NodeJS.WritableStream'
    };

    return typeMap[pythonType] || 'any';
  }

  hasStreamingMethods(node) {
    // Detect if node has streaming capabilities based on method names or types
    const streamingKeywords = ['stream', 'generate', 'iter', 'read_stream', 'write_stream'];
    return node.parameters.some(p => 
      streamingKeywords.some(keyword => p.name.toLowerCase().includes(keyword))
    );
  }

  kebabCase(str) {
    return str.replace(/([a-z])([A-Z])/g, '$1-$2').toLowerCase();
  }

  async cleanup() {
    if (this.client) {
      this.client.close();
    }
  }
}

// Main execution
async function main() {
  const generator = new EnhancedTypeScriptGenerator();

  try {
    await generator.initialize();
    await generator.fetchNodeDefinitions();
    await generator.generateTypes();

    console.log('✅ Enhanced TypeScript definitions generated successfully!');
    console.log(`📁 Output directory: ${OUTPUT_DIR}`);
    console.log('🔧 Import with: import { createRemoteProxy, RemoteCalculatorNode } from "./generated-types"');

  } catch (error) {
    console.error('❌ Error generating TypeScript definitions:', error);
    process.exit(1);
  } finally {
    await generator.cleanup();
  }
}

if (require.main === module) {
  main();
}

module.exports = { EnhancedTypeScriptGenerator };