/**
 * Configuration interface for SerializedClassExecutorNode
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
export interface SerializedClassExecutorNodeConfig {
  /** Whether to enable state management (default: True) (default: true) */
  enable_state?: boolean;
  /** Maximum number of concurrent sessions (default: None/unlimited) */
  max_sessions?: number;
  /** Optional name for the node (defaults to class name) */
  name?: string;
  /** Time-to-live for session states (default: 24 hours) */
  state_ttl?: any;
  args: any;
}
