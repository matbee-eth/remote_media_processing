/**
 * Configuration interface for BufferNode
 * A node that buffers data for batch processing.
 */
export interface BufferNodeConfig {
  /** Maximum number of items to buffer (default: 10) */
  buffer_size?: number;
  args: any;
  /** Whether to enable state management (default: True) (default: true) */
  enable_state?: boolean;
  /** Maximum number of concurrent sessions (default: None/unlimited) */
  max_sessions?: number;
  /** Optional name for the node (defaults to class name) */
  name?: string;
  /** Time-to-live for session states (default: 24 hours) */
  state_ttl?: any;
}
