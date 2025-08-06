# Pipeline Registry Integration Documentation

This document describes the integration of the PipelineRegistry system with the WebRTC pipeline server, enabling automatic pipeline registration, discovery, and cross-language access.

## Overview

The WebRTC pipeline server (`webrtc_pipeline_server.py`) has been enhanced to automatically register its sophisticated speech-to-speech pipeline with the global PipelineRegistry. This enables:

- **Pipeline Discovery**: JavaScript clients can discover and list available pipelines
- **Cross-Language Execution**: Execute Python-defined pipelines from JavaScript/TypeScript
- **Persistent Storage**: Pipelines survive server restarts through database persistence
- **Template System**: Clone and customize registered pipelines
- **Metadata Rich**: Complete information about pipeline capabilities and requirements

## Changes Made

### 1. WebRTC Server Integration (`webrtc_pipeline_server.py`)

#### New Imports Added
```python
from remotemedia.core.pipeline_registry import PipelineRegistry
from remotemedia.persistence import AccessLevel
```

#### New Function: `register_webrtc_pipeline()`
```python
async def register_webrtc_pipeline(registry: PipelineRegistry, remote_host: str = "127.0.0.1") -> str:
    """
    Register the WebRTC pipeline with the global registry for reuse and discovery.
    
    Args:
        registry: Pipeline registry instance
        remote_host: Host for remote execution
        
    Returns:
        Pipeline ID for the registered pipeline
    """
```

**Features:**
- Creates pipeline instance and exports definition
- Registers with comprehensive metadata including:
  - Name: `webrtc_speech_to_speech`
  - Category: `audio`
  - Tags: `webrtc`, `speech`, `vad`, `ultravox`, `tts`, `kokoro`, `realtime`
  - Features list: VAD, speech-to-text, TTS, remote execution, streaming
  - Available tools: time, weather, calculator, web search, reminders
  - Requirements: Dependencies needed for execution
  - Template flag for easy cloning
- Sets public access level for discoverability
- Enables persistence by default

#### Enhanced Main Function
```python
async def main():
    # ... existing code ...
    
    # Initialize pipeline registry with persistence
    registry = PipelineRegistry(
        db_path="webrtc_pipelines.db" if ENABLE_PERSISTENCE else None,
        enable_persistence=ENABLE_PERSISTENCE
    )
    await registry.initialize()
    
    # Register the WebRTC pipeline for discovery and reuse
    pipeline_id = await register_webrtc_pipeline(registry, REMOTE_HOST)
```

#### New Environment Variables
- `ENABLE_PERSISTENCE=true/false` (default: true) - Controls database persistence
- Existing variables remain unchanged

#### Enhanced Logging
- Pipeline registry statistics
- Registered pipeline details with tags
- Persistence status information
- JavaScript client access instructions
- Database location and status

#### Graceful Shutdown Enhancement
```python
except KeyboardInterrupt:
    logger.info("Shutting down server...")
    
    # Save any in-memory pipelines before shutdown
    if ENABLE_PERSISTENCE and registry.pipelines:
        logger.info("💾 Ensuring all pipelines are persisted...")
        for pid, registered in registry.pipelines.items():
            # ... save each pipeline ...
```

### 2. JavaScript Client Discovery Example

#### New File: `nodejs-client/examples/discover-webrtc-pipeline.js`

A comprehensive example demonstrating:

```javascript
async function discoverWebRTCPipeline() {
    const client = new PipelineClient({
        host: 'localhost',
        port: 50052
    });
    
    // 1. List all available pipelines
    const pipelines = await client.listPipelines();
    
    // 2. Find the WebRTC pipeline
    const webrtcPipeline = pipelines.find(p => 
        p.name.includes('webrtc') || 
        p.metadata?.tags?.includes('webrtc')
    );
    
    // 3. Get detailed information
    const info = await client.getPipelineInfo(webrtcPipeline.id);
    
    // 4. Display pipeline structure, features, tools, requirements
    
    // 5. Example execution (with proper setup notes)
    
    // 6. Show cloning example
}
```

**Features:**
- Pipeline listing and discovery
- Detailed information display
- Pipeline structure analysis
- Feature and capability overview
- Mock execution example
- Cloning instructions
- Error handling with helpful hints

### 3. Updated Documentation

#### Enhanced Server Documentation
- Added Pipeline Registry integration to feature list
- Updated usage instructions with environment variables
- Added JavaScript client discovery example
- Enhanced error handling guidance

#### Server Startup Messages
```
=== WebRTC Pipeline Server ===
Server: 0.0.0.0:8080
ML Pipeline: Enabled
Remote Host: 127.0.0.1
Persistence: Enabled

📋 Registered WebRTC pipeline: pipeline_webrtc_speech_to_speech_1704123456789
   🎯 Pipeline features:
   • Voice Activity Detection (VAD)
   • Ultravox speech recognition with tools
   • Kokoro TTS speech synthesis
   • Real-time WebRTC streaming
   • Remote execution support

✅ WebRTC pipeline registered and available for clients
📊 Registry contains 1 pipeline(s)
💾 Pipeline persisted to database for future sessions

🗂️  Pipeline Registry:
   • Registered pipelines: 1
   • Database: webrtc_pipelines.db
   • Persistence: Enabled
   • webrtc_speech_to_speech (pipeline_webr...)
     Tags: webrtc, speech, vad, ultravox, tts, kokoro, realtime
```

## Usage Examples

### Starting the WebRTC Server

#### With Persistence (Default)
```bash
python webrtc_examples/webrtc_pipeline_server.py
```

#### Without Persistence
```bash
ENABLE_PERSISTENCE=false python webrtc_examples/webrtc_pipeline_server.py
```

#### With Custom Configuration
```bash
ENABLE_PERSISTENCE=true \
REMOTE_HOST=remote-server.com \
SERVER_PORT=9090 \
python webrtc_examples/webrtc_pipeline_server.py
```

### JavaScript Client Usage

#### Discovery Example
```bash
node nodejs-client/examples/discover-webrtc-pipeline.js
```

#### Programmatic Usage
```javascript
import { PipelineClient } from '@remote_media_processing/nodejs-client';

// Connect to server
const client = new PipelineClient({
    host: 'localhost',
    port: 50052
});

// Discover WebRTC pipeline
const pipelines = await client.listPipelines();
const webrtcPipeline = pipelines.find(p => 
    p.metadata?.tags?.includes('webrtc')
);

// Get detailed information
const info = await client.getPipelineInfo(webrtcPipeline.id);
console.log('Pipeline features:', info.metadata.features);
console.log('Available tools:', info.metadata.tools);

// Execute with audio data
const audioData = {
    samples: new Float32Array(16000), // 1 second at 16kHz
    sampleRate: 16000,
    channels: 1,
    timestamp: Date.now()
};

const result = await client.executePipeline(webrtcPipeline.id, audioData);

// Clone for customization
const clonedId = await client.clonePipeline(
    webrtcPipeline.id,
    'my-user-id',
    {
        newName: 'my-custom-webrtc-pipeline',
        cloneNodes: true
    }
);
```

## Pipeline Metadata Structure

The registered WebRTC pipeline includes the following metadata:

```javascript
{
    "description": "Complete WebRTC speech-to-speech pipeline with VAD, Ultravox, and Kokoro TTS",
    "category": "audio",
    "version": "1.0.0",
    "author": "RemoteMedia WebRTC Example",
    "tags": ["webrtc", "speech", "vad", "ultravox", "tts", "kokoro", "realtime"],
    "features": [
        "Voice Activity Detection with buffering",
        "Ultravox speech-to-text with tool calling",
        "Kokoro TTS synthesis",
        "Remote execution support",
        "Real-time WebRTC streaming"
    ],
    "tools": [
        "get_current_time",
        "get_weather", 
        "calculate",
        "search_web",
        "set_reminder"
    ],
    "requirements": [
        "aiortc",
        "ultravox",
        "kokoro-tts",
        "torch",
        "transformers"
    ],
    "remote_host": "127.0.0.1",
    "is_template": true,
    "use_case": "Real-time voice conversations with AI assistant capabilities"
}
```

## Database Schema

When persistence is enabled, the following database file is created:

- **File**: `webrtc_pipelines.db`
- **Location**: Same directory as the server script
- **Schema**: SQLite database with pipeline and node storage tables
- **Migration**: Automatic schema updates via migration system

## Error Handling

### Server-Side Error Handling
```python
try:
    pipeline_id = await register_webrtc_pipeline(registry, REMOTE_HOST)
    logger.info(f"✅ WebRTC pipeline registered and available for clients")
except Exception as e:
    logger.error(f"❌ Failed to register WebRTC pipeline: {e}")
    # Continue without registry - server will still work
```

### Client-Side Error Handling
```javascript
try {
    const pipelines = await client.listPipelines();
    // ... process pipelines ...
} catch (error) {
    console.error('❌ Error discovering pipeline:', error.message);
    if (error.code === 'ECONNREFUSED') {
        console.log('💡 Make sure the remote execution server is running');
    }
}
```

## Benefits

### For Developers
1. **Cross-Language Integration**: Python pipelines accessible from JavaScript
2. **Automatic Discovery**: No manual configuration needed
3. **Rich Metadata**: Complete information about capabilities
4. **Template System**: Easy customization through cloning
5. **Persistent Storage**: Pipelines survive server restarts

### For Applications
1. **Reusable Components**: Share complex pipelines across projects
2. **Dynamic Discovery**: Runtime pipeline discovery and execution
3. **Version Management**: Track pipeline versions and changes
4. **Access Control**: Control who can access and modify pipelines
5. **Audit Trail**: Complete logging of pipeline usage

### For Ecosystem
1. **Standardized Interface**: Consistent pipeline access across languages
2. **Metadata Standards**: Rich description format for pipeline capabilities
3. **Tooling Integration**: Easy integration with development tools
4. **Scalable Architecture**: Support for large numbers of registered pipelines

## Migration Path

### From Existing WebRTC Server
1. Update imports to include PipelineRegistry
2. Add registry initialization in main()
3. Call register_webrtc_pipeline() after registry setup
4. Optionally configure persistence via environment variables
5. No breaking changes to existing WebRTC functionality

### For JavaScript Clients
1. Use existing PipelineClient for discovery
2. Replace hardcoded pipeline creation with discovery
3. Benefit from rich metadata for better UX
4. Enable dynamic pipeline selection

## Future Enhancements

### Planned Features
1. **Pipeline Versioning**: Support for multiple versions of the same pipeline
2. **Performance Metrics**: Track execution statistics and performance
3. **Pipeline Composition**: Combine multiple registered pipelines
4. **Real-time Updates**: Live updates when new pipelines are registered
5. **Advanced Filtering**: More sophisticated pipeline discovery options

### Integration Opportunities
1. **CI/CD Integration**: Automatic pipeline registration in deployment
2. **Monitoring Integration**: Pipeline execution monitoring and alerting
3. **Documentation Generation**: Automatic API documentation from metadata
4. **Testing Framework**: Automated testing of registered pipelines

## Troubleshooting

### Common Issues

#### Pipeline Not Registered
- Check server logs for registration errors
- Verify persistence is enabled if needed
- Ensure database permissions are correct

#### JavaScript Client Cannot Discover
- Verify remote execution server is running on port 50052
- Check network connectivity between client and server
- Confirm pipeline registration completed successfully

#### Persistence Errors
- Check file system permissions for database creation
- Verify SQLite is available on the system
- Check disk space for database file

#### Pipeline Execution Fails
- Ensure all required dependencies are installed
- Verify remote execution service is running
- Check pipeline metadata for requirements

### Debug Commands

#### Server-Side Debugging
```bash
# Enable debug logging
DEBUG=true python webrtc_examples/webrtc_pipeline_server.py

# Disable persistence for testing
ENABLE_PERSISTENCE=false python webrtc_examples/webrtc_pipeline_server.py
```

#### Client-Side Debugging
```bash
# Run discovery with verbose output
DEBUG=pipeline:* node nodejs-client/examples/discover-webrtc-pipeline.js
```

## Conclusion

The PipelineRegistry integration transforms the WebRTC server from a standalone service into a contributing member of the broader RemoteMedia ecosystem. By automatically registering its sophisticated speech-to-speech pipeline, it enables:

- **Ecosystem Growth**: Other services can discover and reuse the WebRTC pipeline
- **Developer Productivity**: JavaScript developers can easily access advanced ML capabilities
- **System Integration**: Seamless integration between different parts of the RemoteMedia platform
- **Future Scalability**: Foundation for more complex pipeline orchestration scenarios

This integration demonstrates the power of the PipelineRegistry system and provides a template for how other services can contribute their pipelines to the shared ecosystem.