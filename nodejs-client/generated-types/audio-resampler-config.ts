/**
 * Configuration interface for AudioResampler
 * Audio resampling node.
 */
export interface AudioResamplerConfig {
  target_sample_rate?: number;
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
