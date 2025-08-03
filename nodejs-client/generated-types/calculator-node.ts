/**
 * CalculatorNode Interface
 * 
 * 
    Calculator node - performs mathematical operations.
    
    Expects input data in the format:
    {
        "operation": "add|multiply|subtract|divide|power|modulo",
        "args": [number1, number2, ...]
    }
    
 */
export interface CalculatorNode {
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
  /** Get the current session ID. */
  get_session_id(): string | null;
  /** Get the session state for the given session ID. */
  get_session_state(session_id?: string | null): any | null;
  /** Get list of supported operations. */
  get_supported_operations(): any | "multiply" | "subtract" | "divide" | "power" | any;
  /** Initialize the node before processing. */
  initialize(): null;
  /** Merge processed data with metadata. */
  merge_data_metadata(data: any, metadata: Record<string, any> | null): any;
  /** Perform mathematical operations on input data. */
  process(data: { operation: "add" | "multiply" | "subtract" | "divide" | "power" | "modulo"; args: Array<number> } | any): { operation: "add" | "multiply" | "subtract" | "divide" | "power" | "modulo"; args: Array<number>; result: number; processed_by: string; node_config: Record<string, any> } | { error: string; operation?: string; args?: Array<number>; processed_by: string };
  /** Set the current session ID for state management. */
  set_session_id(session_id: string): null;
  /** Split data into content and metadata components. */
  split_data_metadata(data: any): any | any;
}
