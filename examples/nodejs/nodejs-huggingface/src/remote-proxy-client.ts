/**
 * RemoteProxyClient for Node.js/TypeScript
 * 
 * Provides a Python-like API for transparent remote execution
 * using the RemoteMedia Processing SDK.
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
  pipPackages?: string[];
}

// Remote proxy type
export type RemoteProxy<T> = {
  [K in keyof T]: T[K] extends (...args: any[]) => any
    ? (...args: Parameters<T[K]>) => Promise<Awaited<ReturnType<T[K]>>>
    : never;
};

// Load protobuf
const PROTO_PATH = path.join(__dirname, '../../../remote_service/protos/execution.proto');

/**
 * RemoteProxyClient - Execute Python code remotely with a proxy-like interface
 */
export class RemoteProxyClient {
  private client: any;
  private config: RemoteExecutorConfig;
  private executeNode: any;
  private connected: boolean = false;

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
    
    // Get all methods from the object
    const methods = this.getMethods(obj);
    
    for (const methodName of methods) {
      proxy[methodName] = async (...args: any[]) => {
        return this.executeRemoteMethod(code, methodName, args, dependencies);
      };
    }
    
    return proxy as RemoteProxy<T>;
  }

  /**
   * Execute a remote method
   */
  private async executeRemoteMethod(
    classCode: string,
    methodName: string,
    args: any[],
    dependencies?: string[]
  ): Promise<any> {
    const executionCode = `
${classCode}

async def process(input_data):
    # Create instance
    instance = ${this.getClassName(classCode)}()
    
    # Call method
    method = getattr(instance, input_data['method'])
    args = input_data.get('args', [])
    kwargs = input_data.get('kwargs', {})
    
    result = method(*args, **kwargs)
    
    # Handle async methods
    import asyncio
    if asyncio.iscoroutine(result):
        result = await result
    
    return result
`;

    const request = {
      node_type: 'CodeExecutorNode',
      config: {
        code: executionCode,
        entry_point: 'process',
        pip_packages: dependencies || []
      },
      input_data: Buffer.from(JSON.stringify({
        method: methodName,
        args: args,
        kwargs: {}
      })),
      serialization_format: 'json',
      options: {
        timeout: this.config.timeout || 30.0,
        enable_gpu: true
      }
    };

    const response = await this.executeNode(request);
    
    if (response.status === 'EXECUTION_STATUS_SUCCESS') {
      return JSON.parse(response.output_data.toString());
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
    
    // This is a simplified version - in practice, you'd need more sophisticated translation
    let code = `class ${className}:\n`;
    code += `    def __init__(self):\n`;
    
    // Add properties
    for (const [key, value] of Object.entries(obj)) {
      if (typeof value !== 'function') {
        code += `        self.${key} = ${JSON.stringify(value)}\n`;
      }
    }
    
    // Add placeholder for methods (user should provide Python implementation)
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
    
    return [...new Set(methods)]; // Remove duplicates
  }

  /**
   * Extract class name from Python code
   */
  private getClassName(code: string): string {
    const match = code.match(/class\s+(\w+)/);
    return match ? match[1] : 'UnknownClass';
  }

  /**
   * Close the client
   */
  async close(): Promise<void> {
    this.connected = false;
    // grpc-js doesn't require explicit closing
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