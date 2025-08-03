#!/usr/bin/env node

/**
 * TypeScript Type Generator for RemoteMedia Processing SDK
 * 
 * This script connects to the gRPC service, fetches all registered nodes,
 * and generates TypeScript definition files natively.
 */

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const fs = require('fs').promises;
const path = require('path');

// Configuration
const GRPC_HOST = process.env.GRPC_HOST || 'localhost';
const GRPC_PORT = process.env.GRPC_PORT || 50052;
const OUTPUT_DIR = process.env.OUTPUT_DIR || './generated-types';

// Proto file paths - adjust these to your actual proto file locations
const PROTO_PATH = path.join(__dirname, '../remote_service/protos/execution.proto');

class TypeScriptGenerator {
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
          // The server now returns JSON instead of TypeScript strings
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

    // Generate base types
    await this.generateBaseTypes();

    // Generate node-specific types
    await this.generateNodeTypes();

    // Generate index file
    await this.generateIndexFile();

    console.log(`TypeScript definitions generated in ${OUTPUT_DIR}`);
  }

  async generateBaseTypes() {
    const baseTypes = `/**
 * Base TypeScript interfaces for RemoteMedia Processing SDK
 * Generated at: ${this.nodeDefinitions.generated_at}
 * Service version: ${this.nodeDefinitions.service_version}
 */

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
`;

    await fs.writeFile(path.join(OUTPUT_DIR, 'base.ts'), baseTypes);
  }

  async generateNodeTypes() {
    const nodes = this.nodeDefinitions.nodes;

    // Group nodes by category
    const nodesByCategory = {};
    nodes.forEach(node => {
      const category = node.category || 'base';
      if (!nodesByCategory[category]) {
        nodesByCategory[category] = [];
      }
      nodesByCategory[category].push(node);
    });

    // Generate NodeType enum
    const nodeTypeEnum = this.generateNodeTypeEnum(nodesByCategory);
    await fs.writeFile(path.join(OUTPUT_DIR, 'node-types.ts'), nodeTypeEnum);

    // Generate configuration interfaces for each node
    for (const node of nodes) {
      const configInterface = this.generateNodeConfigInterface(node);
      const filename = `${this.kebabCase(node.node_type)}-config.ts`;
      await fs.writeFile(path.join(OUTPUT_DIR, filename), configInterface);
    }

    // Generate TypedDict interfaces for each node
    for (const node of nodes) {
      if (node.types && node.types.length > 0) {
        const typedDictInterfaces = this.generateTypedDictInterfaces(node);
        const filename = `${this.kebabCase(node.node_type)}.ts`;
        await fs.writeFile(path.join(OUTPUT_DIR, filename), typedDictInterfaces);
      }
    }

    // Generate unified config types
    const configTypes = this.generateConfigTypes(nodes);
    await fs.writeFile(path.join(OUTPUT_DIR, 'config-types.ts'), configTypes);

    // Generate client interface
    const clientInterface = this.generateClientInterface();
    await fs.writeFile(path.join(OUTPUT_DIR, 'client.ts'), clientInterface);
  }

  generateNodeTypeEnum(nodesByCategory) {
    let enumContent = `/**
 * All available node types
 */
export enum NodeType {
`;

    Object.entries(nodesByCategory).forEach(([category, nodes], categoryIndex) => {
      enumContent += `  // ${this.capitalize(category)} nodes\n`;

      nodes.forEach((node, nodeIndex) => {
        const isLast = categoryIndex === Object.keys(nodesByCategory).length - 1 &&
          nodeIndex === nodes.length - 1;
        const comma = isLast ? '' : ',';
        enumContent += `  ${node.node_type} = '${node.node_type}'${comma}\n`;
      });

      if (categoryIndex < Object.keys(nodesByCategory).length - 1) {
        enumContent += '\n';
      }
    });

    enumContent += '}\n';
    return enumContent;
  }

  generateNodeConfigInterface(node) {
    const { node_type, description, parameters = [] } = node;

    let content = `/**
 * Configuration interface for ${node_type}
 * ${description || `Configuration for ${node_type} node`}
 */
export interface ${node_type}Config {
`;

    if (parameters.length === 0) {
      content += '  // No configuration parameters\n';
    } else {
      parameters.forEach(param => {
        const { name, type, required = true, description = '', default_value } = param;
        const tsType = this.pythonToTypeScriptType(type);
        const optional = required ? '' : '?';

        // Add JSDoc comment for parameter
        if (description) {
          content += `  /** ${description}`;
          if (default_value !== undefined && !required) {
            content += ` (default: ${JSON.stringify(default_value)})`;
          }
          content += ' */\n';
        }

        content += `  ${name}${optional}: ${tsType};\n`;
      });
    }

    content += '}\n';
    return content;
  }

  generateTypedDictInterfaces(node) {
    const { node_type, types = [] } = node;

    let content = `/**
 * TypeScript interfaces for ${node_type}
 * Auto-generated from Python TypedDict classes
 */

`;

    types.forEach(typedDict => {
      const { name, description, fields = [] } = typedDict;

      // Generate interface
      content += `/**
 * ${description}
 */
export interface ${name} {
`;

      if (fields.length === 0) {
        content += '  // No fields defined\n';
      } else {
        fields.forEach(field => {
          const { name: fieldName, type, required = true } = field;
          const tsType = this.pythonToTypeScriptType(type);
          const optional = required ? '' : '?';

          content += `  ${fieldName}${optional}: ${tsType};\n`;
        });
      }

      content += '}\n\n';
    });

    // Interfaces are already exported with 'export interface', no need for additional exports

    return content;
  }

  generateConfigTypes(nodes) {
    let content = `import { NodeType } from './node-types';\n`;

    // Import all config interfaces
    nodes.forEach(node => {
      const filename = this.kebabCase(node.node_type);
      content += `import { ${node.node_type}Config } from './${filename}-config';\n`;
    });

    content += '\n';

    // Generate union type
    content += 'export type NodeConfig = \n';
    nodes.forEach((node, index) => {
      const pipe = index === 0 ? '  ' : '  | ';
      content += `${pipe}${node.node_type}Config\n`;
    });
    content += ';\n\n';

    // Generate mapping interface
    content += 'export interface NodeConfigMap {\n';
    nodes.forEach(node => {
      content += `  [NodeType.${node.node_type}]: ${node.node_type}Config;\n`;
    });
    content += '}\n';

    return content;
  }

  generateClientInterface() {
    return `import { ExecutionResponse, ExecutionOptions, StreamHandle, NodeInfo } from './base';
import { NodeConfigMap } from './config-types';
import { NodeType } from './node-types';

/**
 * RemoteMedia Processing Client Interface
 */
export interface RemoteExecutionClient {
  /**
   * Execute a node with type-safe configuration
   */
  executeNode<T extends NodeType>(
    nodeType: T,
    config: NodeConfigMap[T],
    inputData: any,
    options?: ExecutionOptions
  ): Promise<ExecutionResponse>;

  /**
   * List all available nodes
   */
  listAvailableNodes(): Promise<NodeInfo[]>;

  /**
   * Stream data through a node
   */
  streamNode<T extends NodeType>(
    nodeType: T,
    config: NodeConfigMap[T],
    onData: (data: any) => void,
    onError?: (error: Error) => void
  ): StreamHandle;

  /**
   * Close the client connection
   */
  close(): Promise<void>;
}
`;
  }

  async generateIndexFile() {
    const nodes = this.nodeDefinitions.nodes;

    let content = `/**
 * RemoteMedia Processing SDK TypeScript Definitions
 * Generated at: ${this.nodeDefinitions.generated_at}
 * Service version: ${this.nodeDefinitions.service_version}
 */

// Base interfaces
export * from './base';

// Node types and configurations
export * from './node-types';
export * from './config-types';

// Individual node configurations
`;

    nodes.forEach(node => {
      const filename = this.kebabCase(node.node_type);
      content += `export * from './${filename}-config';\n`;

      // Export TypedDict interfaces if they exist
      if (node.types && node.types.length > 0) {
        content += `export * from './${filename}';\n`;
      }
    });

    content += `
// Client interface
export * from './client';
`;

    await fs.writeFile(path.join(OUTPUT_DIR, 'index.ts'), content);
  }

  // Utility methods
  pythonToTypeScriptType(pythonType) {
    const typeMap = {
      'str': 'string',
      'string': 'string',
      'int': 'number',
      'number': 'number',
      'float': 'number',
      'bool': 'boolean',
      'boolean': 'boolean',
      'null': 'null',
      'list': 'any[]',
      'List': 'any[]',
      'Array<any>': 'any[]',
      'dict': 'Record<string, any>',
      'Dict': 'Record<string, any>',
      'Record<string, any>': 'Record<string, any>',
      'Any': 'any',
      'any': 'any',
      'Optional': 'any',
      'Union': 'any'
    };

    // Handle array types
    if (pythonType.includes('Array<')) {
      return pythonType;
    }

    // Handle union types (e.g., "string | null")
    if (pythonType.includes(' | ')) {
      return pythonType;
    }

    // Handle TypedDict references
    if (pythonType.includes('TypedDict<')) {
      // Extract the TypedDict name
      const match = pythonType.match(/TypedDict<(.+)>/);
      if (match) {
        return match[1]; // Return just the type name
      }
    }

    return typeMap[pythonType] || pythonType || 'any';
  }

  kebabCase(str) {
    return str.replace(/([a-z])([A-Z])/g, '$1-$2').toLowerCase();
  }

  capitalize(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
  }

  async cleanup() {
    if (this.client) {
      this.client.close();
    }
  }
}

// Main execution
async function main() {
  const generator = new TypeScriptGenerator();

  try {
    await generator.initialize();
    await generator.fetchNodeDefinitions();
    await generator.generateTypes();

    console.log('✅ TypeScript definitions generated successfully!');
    console.log(`📁 Output directory: ${OUTPUT_DIR}`);
    console.log('🔧 Import with: import { NodeType, RemoteExecutionClient } from "./generated-types"');

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

module.exports = { TypeScriptGenerator };