/**
 * SerializedClassExecutorNode Interface
 * 
 * 
    Serialized Class Executor node - executes cloudpickle-serialized Python classes.
    
    This node implements the Phase 3 requirement for executing user-defined Python
    classes with local dependencies using cloudpickle serialization.
    
    Expects input data in the format:
    {
        "serialized_object": "base64_encoded_cloudpickle_data",
        "method_name": "method_to_call",
        "method_args": [args],
        "method_kwargs": {kwargs}
    }
    
 */
export interface SerializedClassExecutorNode {
  // Configuration properties (constructor arguments)
  /** Whether to enable state management (default: True) (default: true) */
  enable_state?: boolean;
  /** Maximum number of concurrent sessions (default: None/unlimited) */
  max_sessions?: number;
  /** Optional name for the node (defaults to class name) */
  name?: string;
  /** Time-to-live for session states (default: 24 hours) */
  state_ttl?: any;
  args?: any;

  // Available methods
  /** Clean up resources used by the node. */
  cleanup(): null;
  /** Extract session ID from input data. */
  extract_session_id(data: any): string | null;
  /** Get the node configuration. */
  get_config(): Record<string, any>;
  /** Get information about security settings. */
  get_security_info(): Record<string, any>;
  /** Get the current session ID. */
  get_session_id(): string | null;
  /** Get the session state for the given session ID. */
  get_session_state(session_id?: string | null): any | null;
  /** Initialize the node before processing. */
  initialize(): null;
  /** Merge processed data with metadata. */
  merge_data_metadata(data: any, metadata: Record<string, any> | null): any;
  /** Execute a method on a cloudpickle-serialized Python object. */
  process(data: any): any;
  /** Set the current session ID for state management. */
  set_session_id(session_id: string): null;
  /** Split data into content and metadata components. */
  split_data_metadata(data: any): any | any;
}
