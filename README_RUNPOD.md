# RunPod Integration for RemoteMedia Processing SDK

This document describes how to use the RemoteMedia Processing SDK with RunPod GPU infrastructure for remote execution of audio processing tasks.

## Overview

The RunPod integration allows you to execute any RemoteMedia Node or custom Python classes on RunPod GPU infrastructure. It automatically deploys a gRPC service as a RunPod Pod and connects to it seamlessly.

## Features

- **Automatic Pod Deployment**: Deploys gRPC service as RunPod Pod with GPU support
- **Transparent Integration**: Works with existing RemoteExecutorConfig patterns
- **CloudPickle Serialization**: Supports custom Python classes and complex objects
- **Auto-discovery**: Automatically finds pod connection details
- **Cleanup Management**: Configurable pod termination

## Quick Start

### 1. Install Dependencies

```bash
pip install runpod>=1.7.0
```

### 2. Build and Push Docker Image

```bash
# Build the optimized RunPod image
./remote_service/scripts/build_runpod.sh --push
```

### 3. Set API Key

```bash
export RUNPOD_API_KEY="your-runpod-api-key"
```

### 4. Run Example

```bash
python examples/runpod_pod_vad_example.py --gpu-type "NVIDIA A40"
```

## Usage Examples

### Basic Remote Execution

```python
from remotemedia.remote.runpod_pod_client import RunPodPodRemoteExecutorConfig
from remotemedia.nodes.remote import RemoteObjectExecutionNode
from remotemedia.nodes.audio import VoiceActivityDetector

# Configure RunPod Pod
runpod_config = RunPodPodRemoteExecutorConfig(
    api_key="your-api-key",
    pod_name="my-audio-pod",
    gpu_type="NVIDIA A40",
    image="acidhax/remotemedia-service:latest",
    auto_terminate=True
)

# Create local node
local_vad = VoiceActivityDetector(
    energy_threshold=0.01,
    speech_threshold=0.2
)

# Wrap for remote execution
remote_vad = RemoteObjectExecutionNode(
    obj_to_execute=local_vad,
    remote_config=runpod_config,
    name="VAD_RunPod"
)

# Use in pipeline
pipeline.add_node(remote_vad)
```

### Convenience Function

```python
from remotemedia.remote import create_runpod_pod_client

# Quick client creation
client = await create_runpod_pod_client(
    api_key="your-api-key",
    pod_name="my-pod",
    gpu_type="NVIDIA A40"
)

# Use client directly
result = await client.execute_object_method(my_object, "process", data)
```

## Configuration

### RunPodPodConfig Options

- `api_key`: RunPod API key (required)
- `pod_name`: Name for the pod (default: "remotemedia-grpc")
- `gpu_type`: GPU type (default: "RTX A4000")
- `image`: Docker image (default: "acidhax/remotemedia-service:latest") 
- `volume_size`: Storage size in GB (default: 20)
- `deploy_timeout`: Pod deployment timeout (default: 300s)
- `auto_terminate`: Auto-terminate pod on cleanup (default: False)
- `environment_vars`: Additional environment variables

### Available GPU Types

Common options include:
- "NVIDIA A40"
- "NVIDIA A100 80GB PCIe" 
- "NVIDIA GeForce RTX 3080"
- "NVIDIA GeForce RTX 3090"

Check availability with:
```python
import runpod
runpod.api_key = "your-key"
gpus = runpod.get_gpus()
```

## Docker Image

The RunPod integration uses `remote_service/Dockerfile.simple` which includes:

- Python 3.11 slim base
- RemoteMedia SDK with all dependencies
- ML libraries (torch, transformers, librosa, etc.)
- Bubblewrap for sandboxing
- gRPC server on port 50051

### Building Custom Images

```bash
# Build with custom tag
TAG=my-custom-tag ./remote_service/scripts/build_runpod.sh --push

# Use in config
runpod_config = RunPodPodRemoteExecutorConfig(
    image="your-registry/your-image:your-tag",
    # ... other config
)
```

## Development

### Testing Integration

```bash
# Test pod deployment and connection
python examples/runpod_pod_vad_example.py --test-connection

# Run full VAD example
python examples/runpod_pod_vad_example.py
```

### Debugging

Enable debug logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

Pod logs are available in the RunPod dashboard or via API.

## Limitations

- Pod deployment takes 2-5 minutes
- GPU availability varies by region and time
- Pods are billed per minute of runtime
- Network latency for data transfer

## Best Practices

1. **Batch Processing**: Process multiple items per session to amortize deployment time
2. **GPU Selection**: Use appropriate GPU types for your workload
3. **Auto-termination**: Enable for development, disable for production workloads
4. **Error Handling**: Implement retry logic for pod deployment failures
5. **Cost Management**: Monitor pod usage and implement timeouts