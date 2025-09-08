# RemoteMedia SDK Examples

This directory contains example applications demonstrating various features and capabilities of the RemoteMedia SDK. Examples are organized by feature area for easy navigation.

## Directory Structure

### 📁 [pipeline/](pipeline/)
Core pipeline functionality examples including basic setup, export, and media processing.

### 📁 [remote/](remote/)
Remote execution examples demonstrating gRPC communication, streaming, and CloudPickle serialization.

### 📁 [ml-models/](ml-models/)
Machine learning model integration examples including Whisper, Ultravox, Qwen, and generic Transformers pipelines.

### 📁 [vad/](vad/)
Voice Activity Detection examples for speech processing, buffering, and utterance collection.

### 📁 [webrtc/](webrtc/)
WebRTC integration examples for real-time audio/video streaming and conversation management.

### 📁 [state/](state/)
State management examples for multi-user scenarios and session persistence.

### 📁 [nodejs/](nodejs/)
JavaScript/Node.js client examples for integrating with the RemoteMedia SDK from JavaScript.

### 📁 [media-files/](media-files/)
Sample media files (audio, video) used by various examples.

## Getting Started

### Prerequisites

1. **Install Dependencies**:
```bash
# From the project root
pip install -r requirements.txt
pip install -r requirements-dev.txt

# For ML examples
pip install -r requirements-ml.txt
```

2. **Start the Remote Service** (for remote execution examples):
```bash
# From the project root
cd remote_service
./scripts/run.sh
```

## Quick Start Examples

### Basic Pipeline
```bash
python examples/pipeline/basic_pipeline.py
```

### Simple Remote Test
```bash
python examples/remote/simple_remote_test.py
```

### Voice Activity Detection
```bash
python examples/vad/test_vad_simple.py
```

### ML Model (Whisper)
```bash
python examples/ml-models/whisper_transcription.py
```

## Feature Highlights

### Remote Execution
The RemoteMedia SDK enables transparent remote execution of any Python object:

```python
from remotemedia.remote import RemoteProxyClient
from remotemedia.core.node import RemoteExecutorConfig

config = RemoteExecutorConfig(host="localhost", port=50052)
async with RemoteProxyClient(config) as client:
    # ANY object becomes remote with one line!
    obj = MyComplexObject()
    remote_obj = await client.create_proxy(obj)
    
    # Use exactly like the local object (just add await)
    result = await remote_obj.process_data(input_data)
```

### Pipeline Export
Export pipeline definitions for use by JavaScript clients:

```python
from remotemedia import Pipeline

pipeline = Pipeline(name="MyPipeline")
# ... add nodes ...
definition = pipeline.export_definition()
# Register with gRPC service for JavaScript access
```

### WebRTC Integration
Real-time audio/video processing with WebRTC:

```python
from remotemedia.webrtc import WebRTCServer

server = WebRTCServer()
# Add pipeline for speech-to-speech processing
await server.start()
```

## Advanced Features

### Streaming Generators
True streaming support with on-demand fetching:
- Generators return proxy objects that stream data
- Batched fetching for performance
- Early termination support
- Memory efficient

### State Management
Session-specific state for multi-user scenarios:
- Automatic session isolation
- State persistence across calls
- Built-in node state management

### ML Model Integration
Support for popular ML frameworks:
- Whisper (speech recognition)
- Ultravox (speech-to-text)
- Qwen (multimodal)
- Transformers pipelines
- Remote ML inference

## Troubleshooting

### Connection Issues
- Ensure remote service is running: `cd remote_service && ./scripts/run.sh`
- Check port availability (default: 50052)
- Verify network connectivity

### Import Errors
- Install required dependencies: `pip install -r requirements.txt`
- For ML models: `pip install -r requirements-ml.txt`
- Run from project root directory

### Media File Errors
- Check media files exist in `media-files/` directory
- Some examples generate dummy files if missing
- You can provide your own media files

## Learn More

- See individual directory README files for detailed documentation
- Check `tests/` directory for comprehensive test coverage
- Review SDK documentation in the main README