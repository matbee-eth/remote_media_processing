/**
 * RemoteMedia Node.js Client
 * 
 * A TypeScript/JavaScript client for executing remote nodes on a RemoteMedia Processing server.
 * 
 * @packageDocumentation
 */

// Export main client
export { RemoteProxyClient } from './client';

// Export enhanced streaming client
export { 
  RemoteProxyClient as RemoteProxyClientStreaming,
  RemoteAsyncGenerator,
  RemoteReadableStream,
  RemoteWritableStream,
  RemoteExecutorConfig 
} from './remote-proxy-client-streaming';

// Export helper functions and classes
export {
  withRemoteProxy,
  withRemoteExecutor,
  ExecuteFunction,
  RemoteNodes,
  NodePipeline,
  batchProcess,
  retryOperation
} from './helpers';

// Export all types
export * from './types';

// Export generated types for convenience
export { NodeType, NodeMap } from '../generated-types';

// Version information
export const VERSION = '0.1.0';