# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RemoteMedia Processing SDK - A Python SDK for building distributed audio/video/data processing pipelines with transparent remote offloading capabilities. The SDK enables real-time processing applications that can seamlessly offload computationally intensive tasks to remote execution services.

## Key Commands

### Development Setup
```bash
# Install core SDK and dependencies
pip install -e .

# Install development dependencies
pip install -e ".[dev]"

# Install ML dependencies (for advanced processing nodes)
pip install -e ".[ml]"
```

### Testing
```bash
# Run all tests with coverage
pytest

# Run specific test file
pytest tests/test_pipeline.py

# Run tests matching pattern
pytest -k "test_remote"

# Run integration tests with remote service
python tests/test_remote_execution.py --manual
```

### Code Quality
```bash
# Format code with Black (line length 79)
black remotemedia tests

# Type checking
mypy remotemedia

# Linting
flake8 remotemedia tests
```

### Remote Service Operations
```bash
# Generate gRPC code (from remote_service directory)
cd remote_service
python -m grpc_tools.protoc --proto_path=protos --python_out=src --grpc_python_out=src protos/*.proto

# Run remote service locally
cd remote_service
./scripts/run.sh

# Run with Docker
docker-compose up remote-service

# Run tests for remote service
cd remote_service
./scripts/test.sh
```

## Architecture Overview

### Core Components

1. **Pipeline System** (`remotemedia/core/pipeline.py`): Manages sequences of processing nodes with support for both local and remote execution.

2. **Node Framework** (`remotemedia/core/node.py`): Base Node class and RemoteExecutorConfig for all processing units. Nodes can be:
   - Local SDK nodes (audio, video, transform)
   - Remote SDK nodes (executed on remote service)
   - User-defined Python classes (serialized with CloudPickle)
   - Custom code strings (executed remotely)

3. **Remote Execution** (`remotemedia/remote/client.py`): gRPC-based client for offloading node execution to remote services. Supports:
   - Async execution with streaming
   - CloudPickle serialization for user objects
   - Automatic dependency packaging

4. **Code Packaging** (`remotemedia/packaging/`): AST-based analysis and packaging system that:
   - Detects local Python file dependencies
   - Creates deployable archives with dependencies
   - Integrates with CloudPickle for object serialization

### Remote Service Architecture

The remote execution service (`remote_service/`) is a gRPC server that:
- Executes SDK nodes and user-defined code in sandboxed environments
- Handles CloudPickle deserialization and execution
- Supports streaming for real-time processing
- Provides health checking and metrics endpoints

### Key Design Patterns

1. **Transparent Remote Offloading**: Nodes can be executed locally or remotely with minimal code changes using RemoteExecutorConfig.

2. **Serialization Strategy**: 
   - JSON for simple data types
   - CloudPickle for complex Python objects and user-defined classes
   - AST analysis for dependency detection

3. **Security Model**: Remote execution uses restricted globals and sandboxed environments (configurable via SANDBOX_ENABLED).

4. **Streaming Support**: Built on gRPC bidirectional streaming for real-time audio/video processing.

## Development Workflow

### Adding New Processing Nodes

1. Create node class inheriting from `Node` in `remotemedia/nodes/`
2. Implement `process()` method for data transformation
3. Add to `__init__.py` exports
4. Create tests in `tests/test_<node_name>.py`
5. Update examples if applicable

### Testing Remote Execution

1. Start remote service: `cd remote_service && ./scripts/run.sh`
2. Run integration tests: `pytest tests/test_remote_execution.py`
3. Check logs in `remote_service/logs/`

### Debugging Tips

- Set `LOG_LEVEL=DEBUG` for verbose logging
- Use `SANDBOX_ENABLED=false` for easier debugging (development only)
- Check gRPC connection with health check: `python remote_service/src/health_check.py`
- View streaming data flow with examples in `examples/remote_streaming_pipeline.py`

## Important Files

- `remotemedia/nodes/serialized_class_executor.py`: Executes CloudPickle-serialized user classes
- `remotemedia/packaging/dependency_analyzer.py`: AST-based import detection
- `remote_service/src/server.py`: Main gRPC server implementation
- `tests/test_cloudpickle_execution.py`: CloudPickle execution test scenarios
- `examples/remote_object_streaming_audio.py`: Real-world streaming example