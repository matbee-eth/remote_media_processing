/**
 * TypeScript interface definitions for RemoteMedia Processing SDK
 * Generated from Python server definitions
 */

// Core Node interface
export interface RemoteMediaNode {
  name?: string;
  config?: Record<string, any>;
  process(data: any): any | Promise<any>;
  initialize?(): Promise<void>;
  cleanup?(): Promise<void>;
  flush?(): any | Promise<any>;
}

// Session State interfaces
export interface SessionState {
  sessionId: string;
  data: Record<string, any>;
  createdAt: Date;
  lastAccessed: Date;
  metadata: Record<string, any>;
}

// Remote Executor Configuration
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

// Execution Options
export interface ExecutionOptions {
  timeout?: number;
  maxMemoryMb?: number;
  cpuLimit?: number;
  enableGpu?: boolean;
  priority?: 'low' | 'normal' | 'high';
}

// Remote Execution Client interfaces
export interface RemoteExecutionClient {
  executeNode(
    nodeType: string,
    config: Record<string, any>,
    inputData: any,
    options?: ExecutionOptions
  ): Promise<any>;
  
  executeCustomTask(
    codePackage: Uint8Array,
    entryPoint: string,
    inputData: any,
    dependencies?: string[],
    options?: ExecutionOptions
  ): Promise<any>;
  
  streamNode(
    nodeType: string,
    config: Record<string, any>,
    onData: (data: any) => void,
    onError?: (error: Error) => void
  ): StreamHandle;
  
  close(): Promise<void>;
}

// Stream Handle for bidirectional streaming
export interface StreamHandle {
  send(data: any): Promise<void>;
  close(): Promise<void>;
  readonly sessionId: string;
}

// Remote Proxy Client for transparent object execution
export interface RemoteProxyClient {
  createProxy<T extends object>(
    obj: T,
    dependencies?: string[]
  ): Promise<RemoteProxy<T>>;
  
  close(): Promise<void>;
}

// Remote Proxy type wrapper
export type RemoteProxy<T> = {
  [K in keyof T]: T[K] extends (...args: any[]) => any
    ? (...args: Parameters<T[K]>) => Promise<Awaited<ReturnType<T[K]>>>
    : Promise<T[K]>;
};

// Generator support interfaces
export interface RemoteGenerator<T> {
  next(): Promise<{ value: T; done: boolean }>;
  close(): Promise<void>;
  [Symbol.asyncIterator](): AsyncIterator<T>;
}

// Common built-in node types
export enum NodeType {
  // Audio nodes
  AudioTransform = 'AudioTransform',
  AudioBuffer = 'AudioBuffer',
  VoiceActivityDetector = 'VoiceActivityDetector',
  VADTriggeredBuffer = 'VADTriggeredBuffer',
  
  // ML nodes
  UltravoxNode = 'UltravoxNode',
  KokoroTTSNode = 'KokoroTTSNode',
  
  // Transform nodes
  TransformNode = 'TransformNode',
  FilterNode = 'FilterNode',
  BatchNode = 'BatchNode',
  
  // Utility nodes
  CalculatorNode = 'CalculatorNode',
  TextProcessorNode = 'TextProcessorNode',
  CodeExecutorNode = 'CodeExecutorNode',
}

// Node configuration interfaces
export interface AudioTransformConfig {
  sampleRate?: number;
  channels?: number;
  dtype?: 'int16' | 'float32';
}

export interface VoiceActivityDetectorConfig {
  sampleRate?: number;
  frameLength?: number;
  frameLengthMs?: number;
  vadMode?: 0 | 1 | 2 | 3;
}

export interface VADTriggeredBufferConfig {
  sampleRate?: number;
  minSpeechDurationMs?: number;
  minSilenceDurationMs?: number;
  preSpeechBufferMs?: number;
}

// Serialization formats
export type SerializationFormat = 'json' | 'pickle';

// Response types
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

// Example usage:
/*
import { RemoteExecutionClient, NodeType, AudioTransformConfig } from './remotemedia-types';

const config: RemoteExecutorConfig = {
  host: 'localhost',
  port: 50052,
  protocol: 'grpc'
};

const client = new RemoteExecutionClient(config);

// Execute a node
const audioConfig: AudioTransformConfig = {
  sampleRate: 16000,
  channels: 1
};

const result = await client.executeNode(
  NodeType.AudioTransform,
  audioConfig,
  audioData
);
*/