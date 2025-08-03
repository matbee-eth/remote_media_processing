/**
 * TypeScript interfaces for BufferNode
 * Auto-generated from Python TypedDict classes
 */

/**
 * Error output structure for BufferNode.
 */
export interface BufferError {
  error: string;
  input: any;
  processed_by: string;
}

/**
 * Output data structure for BufferNode.
 */
export interface BufferOutput {
  buffer: Array<any>;
  count: number;
  processed_by: string;
}

/**
 * Error output structure for PassThroughNode.
 */
export interface PassThroughError {
  error: string;
  input: any;
  processed_by: string;
}


/**
 * BufferNode Interface
 * 
 * A node that buffers data for batch processing.
 */
export interface BufferNode {
  // Configuration properties (constructor arguments)
  /** Maximum number of items to buffer (default: 10) */
  buffer_size?: number;
  args?: any;
  /** Whether to enable state management (default: True) (default: true) */
  enable_state?: boolean;
  /** Maximum number of concurrent sessions (default: None/unlimited) */
  max_sessions?: number;
  /** Optional name for the node (defaults to class name) */
  name?: string;
  /** Time-to-live for session states (default: 24 hours) */
  state_ttl?: any;

  // Available methods
  /** Clean up resources used by the node. */
  cleanup(): null;
  /** Extract session ID from input data. */
  extract_session_id(data: any): string | null;
  /** Flush the current buffer and return its contents. */
  flush(): Array<any>;
  /** Get the node configuration. */
  get_config(): Record<string, any>;
  /** Get the current session ID. */
  get_session_id(): string | null;
  /** Get the session state for the given session ID. */
  get_session_state(session_id?: string | null): any | null;
  /** Initialize the node before processing. */
  initialize(): null;
  /** Merge processed data with metadata. */
  merge_data_metadata(data: any, metadata: Record<string, any> | null): any;
  /** Buffer data and return when buffer is full. */
  process(data: any): Array<any> | null | BufferError;
  /** Set the current session ID for state management. */
  set_session_id(session_id: string): null;
  /** Split data into content and metadata components. */
  split_data_metadata(data: any): any | any;
}
