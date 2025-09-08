#!/usr/bin/env python3
"""
WebRTC Pipeline Registration Example

This example shows how the webrtc_pipeline_server.py can register its 
speech-to-speech pipeline with the gRPC service, making it available 
for JavaScript clients to discover and use.
"""

import asyncio
import sys
import os
from pathlib import Path

# Add paths
sys.path.insert(0, str(Path(__file__).parent.parent))

from remotemedia.core.pipeline import Pipeline
from remotemedia.core.pipeline_registry import get_global_registry
from remotemedia.nodes.audio import AudioTransform, VoiceActivityDetector
from remotemedia.nodes.io_nodes import DataSourceNode, DataSinkNode
from remotemedia.nodes import PassThroughNode


async def create_webrtc_speech_pipeline():
    """
    Create the same speech-to-speech pipeline as in webrtc_pipeline_server.py
    but register it for JavaScript client access.
    """
    print("🎙️ Creating WebRTC Speech-to-Speech Pipeline")
    
    pipeline = Pipeline(name="WebRTCSpeechToSpeechPipeline")
    
    # Audio input source (from WebRTC)
    pipeline.add_node(DataSourceNode(
        buffer_size=1000,
        timeout_seconds=60,
        name="webrtc_audio_input"
    ))
    
    # Audio preprocessing - resample for VAD
    pipeline.add_node(AudioTransform(
        output_sample_rate=16000,
        output_channels=1,
        name="audio_transform"
    ))
    
    # Voice Activity Detection with metadata
    vad = VoiceActivityDetector(
        frame_duration_ms=30,
        energy_threshold=0.02,
        speech_threshold=0.3,
        filter_mode=False,  # Keep metadata for buffering
        include_metadata=True,
        name="vad"
    )
    # Mark as streaming
    vad.is_streaming = True
    pipeline.add_node(vad)
    
    # Simulated speech recognition (would be UltravoxNode in real setup)
    pipeline.add_node(PassThroughNode(name="speech_recognition"))
    
    # Simulated TTS (would be KokoroTTSNode in real setup)  
    tts = PassThroughNode(name="text_to_speech")
    tts.is_streaming = True
    pipeline.add_node(tts)
    
    # Audio output sink (to WebRTC)
    pipeline.add_node(DataSinkNode(
        buffer_output=False,
        name="webrtc_audio_output"
    ))
    
    return pipeline


async def register_webrtc_pipeline():
    """Register the WebRTC pipeline with the service."""
    print("\n📝 Registering WebRTC Pipeline with gRPC Service")
    
    # Create the pipeline
    pipeline = await create_webrtc_speech_pipeline()
    
    # Export its definition
    definition = pipeline.export_definition()
    
    print(f"📊 Pipeline Definition Summary:")
    print(f"  - Name: {definition['name']}")
    print(f"  - Nodes: {len(definition['nodes'])}")
    print(f"  - Connections: {len(definition['connections'])}")
    print(f"  - Dependencies: {definition['dependencies']}")
    
    # Get the global registry
    registry = get_global_registry()
    
    # Register the pipeline
    pipeline_id = await registry.register_pipeline(
        name="webrtc_speech_to_speech",
        definition=definition,
        metadata={
            "category": "webrtc",
            "description": "Real-time speech-to-speech processing for WebRTC clients",
            "webrtc_enabled": "true",
            "supports_streaming": "true",
            "input_type": "audio",
            "output_type": "audio",
            "use_case": "voice_conversation"
        },
        dependencies=definition.get('dependencies', []) + [
            'aiortc',
            'aiohttp',
            'transformers',
            'torch'
        ],
        category="webrtc"
    )
    
    print(f"\n✅ Pipeline registered successfully!")
    print(f"   Pipeline ID: {pipeline_id}")
    print(f"   Available at: gRPC service localhost:50052")
    
    return pipeline_id, registry


async def demonstrate_javascript_usage():
    """Show how JavaScript clients would discover and use this pipeline."""
    print("\n🌐 JavaScript Client Usage Example")
    
    pipeline_id, registry = await register_webrtc_pipeline()
    
    # Show how to list pipelines
    print("\n1️⃣ Discovering WebRTC pipelines:")
    webrtc_pipelines = registry.list_pipelines(category="webrtc")
    for p in webrtc_pipelines:
        print(f"   - {p['name']}: {p['description']}")
        print(f"     ID: {p['pipeline_id']}")
        print(f"     Metadata: {p.get('metadata', {})}")
    
    # Show pipeline details
    print("\n2️⃣ Getting pipeline details:")
    info = registry.get_pipeline_info(pipeline_id, include_definition=True)
    if info:
        print(f"   - Nodes: {[n['node_type'] for n in info['definition']['nodes']]}")
        print(f"   - Has sources: {any(n['is_source'] for n in info['definition']['nodes'])}")
        print(f"   - Has sinks: {any(n['is_sink'] for n in info['definition']['nodes'])}")
    
    print("\n3️⃣ JavaScript client code example:")
    print("""
    // Connect to the service
    const client = new PipelineClient('localhost', 50052);
    await client.connect();
    
    // Discover WebRTC pipelines
    const webrtcPipelines = await client.listPipelines('webrtc');
    const speechPipeline = webrtcPipelines.find(p => 
        p.name === 'webrtc_speech_to_speech'
    );
    
    // Stream audio through the pipeline
    const stream = client.streamPipeline(speechPipeline.pipelineId, {
        bidirectional: true,
        runtimeConfig: {
            sample_rate: '16000',
            channels: '1'
        }
    });
    
    // Handle audio output
    stream.on('data', (audioData) => {
        // Send audio data to WebRTC peer
        sendAudioToWebRTC(audioData);
    });
    
    // Send audio input from WebRTC
    webrtcPeer.ontrack = (event) => {
        const audioTrack = event.streams[0].getAudioTracks()[0];
        // Process audio chunks and send to pipeline
        processAudioChunks(audioTrack, (chunk) => {
            stream.send(chunk);
        });
    };
    """)
    
    # Clean up
    await registry.unregister_pipeline(pipeline_id)
    print("\n🗑️ Pipeline unregistered (cleanup)")


async def show_integration_with_webrtc_server():
    """Show how this integrates with the existing WebRTC server."""
    print("\n🔗 Integration with WebRTC Server")
    
    print("""
When the webrtc_pipeline_server.py starts up, it can now:

1. Create its speech-to-speech pipeline
2. Register it with the gRPC service  
3. Make it available to JavaScript clients
4. Continue serving WebRTC connections as before

Modified webrtc_pipeline_server.py would include:

```python
async def main():
    # Create pipeline
    pipeline = create_speech_to_speech_pipeline()
    
    # Register with gRPC service (NEW)
    from remotemedia.core.pipeline_registry import get_global_registry
    registry = get_global_registry()
    
    pipeline_id = await registry.register_pipeline(
        name="webrtc_speech_to_speech",
        definition=pipeline.export_definition(),
        metadata={"category": "webrtc", "webrtc_enabled": "true"}
    )
    
    print(f"Pipeline registered: {pipeline_id}")
    print("JavaScript clients can now discover and use this pipeline!")
    
    # Start WebRTC server as before
    server = WebRTCServer(config=config, pipeline_factory=lambda: pipeline)
    await server.start()
    # ...
```

Benefits:
✅ JavaScript clients can discover available pipelines
✅ Multiple clients can share the same pipeline definition  
✅ Pipeline configurations can be modified remotely
✅ Centralized pipeline management and monitoring
✅ Type-safe pipeline usage from TypeScript
    """)


async def main():
    """Main demonstration function."""
    print("🚀 WebRTC Pipeline Registration Demo")
    print("=" * 50)
    
    try:
        # Demonstrate pipeline creation and registration
        await demonstrate_javascript_usage()
        
        # Show integration patterns
        await show_integration_with_webrtc_server()
        
        print("\n✅ Demo completed successfully!")
        print("\nNext steps:")
        print("1. Start the gRPC service: python remote_service/src/server.py")
        print("2. Register pipelines using the registry")
        print("3. Connect JavaScript clients to discover and use pipelines")
        print("4. Build rich WebRTC applications with pipeline integration!")
        
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)