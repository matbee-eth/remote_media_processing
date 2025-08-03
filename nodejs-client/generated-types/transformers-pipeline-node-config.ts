/**
 * Configuration interface for TransformersPipelineNode
 * 
    A generic node that wraps a Hugging Face Transformers pipeline.

    This node can be configured to run various tasks like text-classification,
    automatic-speech-recognition, etc., by leveraging the `transformers.pipeline`
    factory.

    See: https://huggingface.co/docs/transformers/main_classes/pipelines
    
 */
export interface TransformersPipelineNodeConfig {
  /** The device to run the model on (e.g., "cpu", "cuda", 0). */
  device?: any;
  /** The model identifier from the Hugging Face Hub. */
  model?: string;
  /** Extra keyword arguments for the model. */
  model_kwargs?: Record<string, any>;
  /** The task for the pipeline (e.g., "text-classification"). */
  task: string;
  /** The torch dtype to use (e.g., "float16", "bfloat16"). */
  torch_dtype?: string;
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
