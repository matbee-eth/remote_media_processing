/**
 * RemoteProxyClient for Node.js/TypeScript
 * 
 * Provides a Python-like API for transparent remote object execution
 * using the RemoteMedia Processing SDK.
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { promisify } from 'util';
import * as path from 'path';
import * as fs from 'fs';
import * as crypto from 'crypto';
import { execSync } from 'child_process';

// Configuration interfaces
export interface RemoteExecutorConfig {
  host: string;
  port: number;
  protocol?: 'grpc' | 'http';
  authToken?: string;
  timeout?: number;
  maxRetries?: number;
  sslEnabled?: boolean;
  pipPackages?: string[];
}

// Proxy type that wraps all methods to return promises
export type RemoteProxy<T> = {
  [K in keyof T]: T[K] extends (...args: any[]) => any
    ? (...args: Parameters<T[K]>) => Promise<Awaited<ReturnType<T[K]>>>
    : Promise<T[K]>;
} & {
  _cleanup?: () => Promise<void>;
};

// Load protobuf definitions
const PROTO_PATH = path.join(__dirname, '..', '..', 'remote_service', 'protos', 'execution.proto');

/**
 * RemoteProxyClient - Main client class for remote object execution
 */
export class RemoteProxyClient {
  private client: any;
  private config: RemoteExecutorConfig;
  private activeSessions: Map<string, string> = new Map();
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
      includeDirs: [path.join(__dirname, '..', '..', 'remote_service', 'protos')]
    });
    
    this.remoteMedia = grpc.loadPackageDefinition(this.packageDefinition).remotemedia as any;
  }

  /**
   * Connect to the remote service
   */
  async connect(): Promise<void> {
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
  }

  /**
   * Create a remote proxy for an object
   */
  async createProxy<T extends object>(
    obj: T,
    dependencies?: string[]
  ): Promise<RemoteProxy<T>> {
    if (!this.client) {
      await this.connect();
    }

    // Package the object (simplified - in real implementation would use proper serialization)
    const packageData = await this.packageObject(obj, dependencies);
    
    // Create initial session
    const sessionId = await this.initializeRemoteObject(packageData, dependencies);
    this.activeSessions.set(obj.constructor.name, sessionId);
    
    // Create proxy object
    return this.createProxyObject<T>(obj, sessionId);
  }

  /**
   * Package an object for remote execution
   */
  private async packageObject(obj: any, dependencies?: string[]): Promise<Buffer> {
    // Create a Python script that recreates the object
    const className = obj.constructor.name;
    const pythonCode = `
import cloudpickle
import base64

class ${className}:
${this.generatePythonClass(obj)}

# Create instance
instance = ${className}()
${this.generateInitialState(obj)}

# Serialize
serialized = base64.b64encode(cloudpickle.dumps(instance)).decode('utf-8')
print(serialized)
`;

    // Save to temporary file and execute Python to serialize
    const tempFile = path.join(__dirname, `temp_${Date.now()}.py`);
    fs.writeFileSync(tempFile, pythonCode);
    
    try {
      const output = execSync(`python ${tempFile}`, { encoding: 'utf-8' });
      fs.unlinkSync(tempFile);
      
      // Create a zip package with the serialized object
      const zipData = await this.createZipPackage(output.trim());
      return zipData;
    } catch (error) {
      fs.unlinkSync(tempFile);
      throw new Error(`Failed to package object: ${error}`);
    }
  }

  /**
   * Generate Python class definition from JavaScript object
   */
  private generatePythonClass(obj: any): string {
    const methods: string[] = [];
    
    // Get all methods from the object
    const proto = Object.getPrototypeOf(obj);
    const methodNames = Object.getOwnPropertyNames(proto)
      .filter(name => name !== 'constructor' && typeof proto[name] === 'function');
    
    for (const method of methodNames) {
      // Generate Python method (simplified)
      methods.push(`    def ${method}(self, *args, **kwargs):
        # Placeholder implementation
        return f"Called ${method} with args={args}, kwargs={kwargs}"`);
    }
    
    return methods.join('\n');
  }

  /**
   * Generate initial state setup
   */
  private generateInitialState(obj: any): string {
    const statements: string[] = [];
    
    // Set properties
    for (const [key, value] of Object.entries(obj)) {
      if (typeof value !== 'function') {
        statements.push(`instance.${key} = ${JSON.stringify(value)}`);
      }
    }
    
    return statements.join('\n');
  }

  /**
   * Create a zip package with the serialized object
   */
  private async createZipPackage(serializedObject: string): Promise<Buffer> {
    // Simplified - in real implementation would create proper zip
    const packageData = {
      serialized_object: serializedObject,
      timestamp: Date.now()
    };
    
    return Buffer.from(JSON.stringify(packageData));
  }

  /**
   * Initialize remote object and get session ID
   */
  private async initializeRemoteObject(
    packageData: Buffer,
    dependencies?: string[]
  ): Promise<string> {
    const executeMethod = promisify(this.client.ExecuteObjectMethod.bind(this.client));
    
    const request = {
      code_package: packageData,
      config: {},
      serialization_format: 'pickle',
      method_name: '__init__',
      method_args_data: Buffer.from('[]'),
      method_kwargs_data: Buffer.from('{}'),
      dependencies: dependencies || []
    };
    
    const response = await executeMethod(request);
    
    if (response.status !== 'EXECUTION_STATUS_SUCCESS') {
      throw new Error(`Failed to initialize remote object: ${response.error_message}`);
    }
    
    return response.session_id;
  }

  /**
   * Create proxy object with method wrappers
   */
  private createProxyObject<T>(originalObj: T, sessionId: string): RemoteProxy<T> {
    const proxy: any = {};
    const executeMethod = promisify(this.client.ExecuteObjectMethod.bind(this.client));
    
    // Get all properties and methods
    const proto = Object.getPrototypeOf(originalObj);
    const allProps = [
      ...Object.getOwnPropertyNames(originalObj),
      ...Object.getOwnPropertyNames(proto)
    ].filter(name => name !== 'constructor');
    
    for (const prop of allProps) {
      const value = (originalObj as any)[prop] || (proto as any)[prop];
      
      if (typeof value === 'function') {
        // Wrap methods to execute remotely
        proxy[prop] = async (...args: any[]) => {
          const request = {
            session_id: sessionId,
            config: {},
            serialization_format: 'json',
            method_name: prop,
            method_args_data: Buffer.from(JSON.stringify(args)),
            method_kwargs_data: Buffer.from(JSON.stringify({}))
          };
          
          const response = await executeMethod(request);
          
          if (response.status === 'EXECUTION_STATUS_SUCCESS') {
            return JSON.parse(response.result_data.toString());
          } else {
            throw new Error(`Remote method ${prop} failed: ${response.error_message}`);
          }
        };
      } else {
        // Wrap properties as async getters
        proxy[prop] = (async () => {
          const request = {
            session_id: sessionId,
            config: {},
            serialization_format: 'json',
            method_name: '__getattribute__',
            method_args_data: Buffer.from(JSON.stringify([prop])),
            method_kwargs_data: Buffer.from(JSON.stringify({}))
          };
          
          const response = await executeMethod(request);
          
          if (response.status === 'EXECUTION_STATUS_SUCCESS') {
            return JSON.parse(response.result_data.toString());
          } else {
            throw new Error(`Failed to get property ${prop}: ${response.error_message}`);
          }
        })();
      }
    }
    
    // Add cleanup method
    proxy._cleanup = async () => {
      // Clean up remote session
      this.activeSessions.delete(originalObj.constructor.name);
    };
    
    return proxy as RemoteProxy<T>;
  }

  /**
   * Close all connections and clean up
   */
  async close(): Promise<void> {
    // Clean up all active sessions
    for (const [_, sessionId] of this.activeSessions) {
      // Could send cleanup request to server here
    }
    this.activeSessions.clear();
    
    // Client doesn't need explicit closing in grpc-js
  }

  /**
   * Use with async/await pattern similar to Python
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
 * Helper function to use with async/await pattern
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

// Example usage similar to Python
if (require.main === module) {
  async function example() {
    // Define a sample calculator class
    class Calculator {
      name: string = "SimpleCalculator";
      
      add(a: number, b: number): number {
        return a + b;
      }
      
      multiply(a: number, b: number): number {
        return a * b;
      }
      
      divide(a: number, b: number): number {
        if (b === 0) throw new Error("Division by zero");
        return a / b;
      }
    }
    
    // Use the RemoteProxyClient similar to Python
    const config: RemoteExecutorConfig = {
      host: "localhost",
      port: 50052
    };
    
    // Method 1: Using the client directly
    const client = new RemoteProxyClient(config);
    await client.connect();
    
    const calculator = new Calculator();
    const remoteCalc = await client.createProxy(calculator);
    
    // Use it exactly like a local object (just add await)
    const sum = await remoteCalc.add(5, 3);
    console.log(`5 + 3 = ${sum}`);
    
    const product = await remoteCalc.multiply(4, 7);
    console.log(`4 * 7 = ${product}`);
    
    await client.close();
    
    // Method 2: Using the helper function (similar to Python's async with)
    await withRemoteProxy(config, async (client) => {
      const calc = new Calculator();
      const remote = await client.createProxy(calc);
      
      const result = await remote.divide(10, 2);
      console.log(`10 / 2 = ${result}`);
    });
  }
  
  example().catch(console.error);
}