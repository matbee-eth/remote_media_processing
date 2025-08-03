/**
 * Configuration interface for DataTransform
 * Generic data transformation node.
 */
export interface DataTransformConfig {
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
