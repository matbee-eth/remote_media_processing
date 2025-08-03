/**
 * Configuration interface for AudioTransform
 * 
    Audio transformation node that supports resampling and channel conversion.
    
 */
export interface AudioTransformConfig {
  /** The target number of channels for the audio. (default: 2) */
  output_channels?: number;
  /** The target sample rate for the audio. (default: 44100) */
  output_sample_rate?: number;
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
