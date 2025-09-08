# Remote Execution Examples

This directory contains examples demonstrating remote execution capabilities of the RemoteMedia SDK.

## Examples

- **simple_remote_test.py** - Basic connection test and simple node execution on remote service
- **remote_streaming_pipeline.py** - Streaming pipeline with remote execution nodes
- **remote_object_streaming_audio.py** - Stream custom objects for real-time audio processing remotely
- **test_streaming_generators.py** - Test streaming generator support for remote execution

## Key Concepts

These examples demonstrate:
- Connecting to remote gRPC services
- Remote node execution with RemoteExecutionClient
- Streaming data to/from remote services
- CloudPickle serialization for custom objects
- Generator-based streaming for real-time processing