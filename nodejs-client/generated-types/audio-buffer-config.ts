/**
 * Configuration interface for AudioBuffer
 * 
    Audio buffering node that accumulates audio data until a target size is reached.
    
 */
export interface AudioBufferConfig {
  /** The number of samples to buffer before outputting. */
  buffer_size_samples: number;
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
