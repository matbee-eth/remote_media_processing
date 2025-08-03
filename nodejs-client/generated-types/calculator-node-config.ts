/**
 * Configuration interface for CalculatorNode
 * 
    Calculator node - performs mathematical operations.
    
    Expects input data in the format:
    {
        "operation": "add|multiply|subtract|divide|power|modulo",
        "args": [number1, number2, ...]
    }
    
 */
export interface CalculatorNodeConfig {
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
