/**
 * Configuration interface for TextProcessorNode
 * 
    Text Processor node - performs various text processing operations.
    
    Expects input data in the format:
    {
        "text": "string_to_process",
        "operations": ["uppercase", "lowercase", "reverse", "word_count", "char_count"]
    }
    
 */
export interface TextProcessorNodeConfig {
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
