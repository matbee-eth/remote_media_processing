/**
 * Configuration interface for CodeExecutorNode
 * 
    Code Executor node - executes arbitrary Python code.
    
    WARNING: This is INSECURE and should only be used in trusted environments!
    
    Expects input data in the format:
    {
        "code": "python_code_string",
        "input": optional_input_data
    }
    
 */
export interface CodeExecutorNodeConfig {
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
