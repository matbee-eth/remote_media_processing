#!/usr/bin/env python3
"""
RunPod Pod VAD Example - RemoteMedia Processing SDK

This example demonstrates how to execute Voice Activity Detection (VAD) 
on RunPod Pod infrastructure using our existing gRPC remote execution service.

This approach:
- Deploys our full gRPC service as a RunPod Pod
- Automatically discovers the connection details
- Uses our standard RemoteExecutionClient interface
- Leverages all existing remote execution features (sessions, streaming, etc.)
"""

import asyncio
import logging
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from remotemedia.core.pipeline import Pipeline
from remotemedia.nodes.source import MediaReaderNode, AudioTrackSource
from remotemedia.nodes.audio import AudioTransform, VoiceActivityDetector
from remotemedia.nodes.remote import RemoteObjectExecutionNode
from remotemedia.remote.runpod_pod_client import RunPodPodRemoteExecutorConfig
from remotemedia.nodes import PassThroughNode

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class VADDebugger(PassThroughNode):
    """Debug node to analyze VAD output."""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.is_streaming = True
        self.chunk_count = 0
        self.speech_chunks = 0
        self.total_samples = 0
        self.speech_samples = 0
        
    async def process(self, data_stream):
        """Log detailed VAD analysis."""
        logger.info("VADDebugger: Starting analysis...")
        
        async for data in data_stream:
            self.chunk_count += 1
            
            if isinstance(data, tuple) and len(data) == 2 and isinstance(data[1], dict):
                (audio_data, sample_rate), metadata = data
                
                is_speech = metadata.get("is_speech", False)
                speech_ratio = metadata.get("speech_ratio", 0.0)
                avg_energy = metadata.get("avg_energy", 0.0)
                
                samples = audio_data.size if hasattr(audio_data, 'size') else len(audio_data)
                duration_ms = (samples / sample_rate) * 1000
                
                self.total_samples += samples
                if is_speech:
                    self.speech_chunks += 1
                    self.speech_samples += samples
                
                logger.info(
                    f"Chunk {self.chunk_count}: "
                    f"SPEECH={is_speech}, "
                    f"ratio={speech_ratio:.2f}, "
                    f"energy={avg_energy:.4f}, "
                    f"duration={duration_ms:.0f}ms, "
                    f"samples={samples}"
                )
                
                yield data
            else:
                logger.warning(f"Chunk {self.chunk_count}: Unexpected format {type(data)}")
                yield data
        
        total_duration_s = self.total_samples / 16000
        speech_duration_s = self.speech_samples / 16000
        
        logger.info("=== RUNPOD POD VAD ANALYSIS SUMMARY ===")
        logger.info(f"Total chunks: {self.chunk_count}")
        logger.info(f"Speech chunks: {self.speech_chunks} ({100*self.speech_chunks/max(self.chunk_count,1):.1f}%)")
        logger.info(f"Total duration: {total_duration_s:.2f}s")
        logger.info(f"Speech duration: {speech_duration_s:.2f}s ({100*speech_duration_s/max(total_duration_s,1):.1f}%)")


async def test_runpod_pod_connection(api_key: str, gpu_type: str = "RTX A4000"):
    """Test RunPod Pod deployment and gRPC connection."""
    logger.info("🧪 Testing RunPod Pod deployment and gRPC connection...")
    
    from remotemedia.remote.runpod_pod_client import create_runpod_pod_client
    
    try:
        # Deploy and connect
        client = await create_runpod_pod_client(
            api_key=api_key,
            pod_name="remotemedia-test",
            gpu_type=gpu_type,
            auto_terminate=True  # Clean up after test
        )
        
        logger.info("✅ RunPod Pod deployed and gRPC connection established!")
        
        # Test basic functionality
        logger.info("Testing gRPC status check...")
        response = await client.get_status()
        logger.info(f"Status response: {response}")
        
        # Cleanup
        await client.disconnect()
        logger.info("✅ Connection test successful and pod cleaned up")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Connection test failed: {e}")
        return False


async def main(api_key: str = None, gpu_type: str = "RTX A4000"):
    """Run VAD detection on RunPod Pod infrastructure."""
    
    # Check for RunPod API key
    api_key = api_key or os.environ.get("RUNPOD_API_KEY")
    
    if not api_key:
        logger.error("RunPod API key required!")
        logger.error("Provide via command line: --api-key YOUR_KEY")
        logger.error("Or set environment variable: export RUNPOD_API_KEY='your-api-key'")
        return False
    
    logger.info("=== RunPod Pod VAD Detection Example ===")
    logger.info(f"API Key: {api_key[:8]}...")
    logger.info(f"GPU Type: {gpu_type}")
    
    # Configure RunPod Pod
    runpod_config = RunPodPodRemoteExecutorConfig(
        api_key=api_key,
        pod_name="remotemedia-vad",
        gpu_type=gpu_type,
        image="acidhax/remotemedia-service:latest",  # Built from Dockerfile.simple with ML deps
        auto_terminate=True,  # Clean up after processing
        deploy_timeout=600,   # 10 minutes for deployment
        timeout=120.0         # 2 minutes for individual operations
    )
    
    logger.info(f"RunPod Config: {runpod_config.provider}, GPU: {runpod_config.runpod_config.gpu_type}")
    
    try:
        # Create pipeline
        pipeline = Pipeline()
        
        # Source - read audio file
        pipeline.add_node(MediaReaderNode(
            path="examples/transcribe_demo.wav",
            chunk_size=4096,
            name="MediaReader"
        ))
        
        pipeline.add_node(AudioTrackSource(name="AudioTrackSource"))
        
        # Transform to 16kHz (locally)
        pipeline.add_node(AudioTransform(
            output_sample_rate=16000,
            output_channels=1,
            name="AudioTransform"
        ))
        
        # VAD - Execute on RunPod Pod via gRPC!
        local_vad = VoiceActivityDetector(
            frame_duration_ms=30,
            energy_threshold=0.01,  # More sensitive
            speech_threshold=0.2,   # Lower ratio needed
            filter_mode=False,
            include_metadata=True,
            name="VAD_Local"
        )
        
        # Wrap VAD for remote execution on RunPod Pod
        remote_vad = RemoteObjectExecutionNode(
            obj_to_execute=local_vad,
            remote_config=runpod_config,
            name="VAD_RunPodPod"
        )
        
        pipeline.add_node(remote_vad)
        
        # Debug analyzer (local)
        pipeline.add_node(VADDebugger(name="VADDebugger"))
        
        # Execute pipeline
        logger.info("Starting pipeline with RunPod Pod gRPC execution...")
        
        async with pipeline.managed_execution():
            chunk_count = 0
            async for result in pipeline.process():
                chunk_count += 1
            
            logger.info(f"Pipeline processed {chunk_count} final chunks")
            logger.info("✅ RunPod Pod VAD execution completed successfully!")
            return True
            
    except Exception as e:
        logger.error(f"❌ Pipeline execution failed: {e}")
        logger.error("This could be due to:")
        logger.error("  - Invalid RunPod API key")
        logger.error("  - Docker image not available")
        logger.error("  - GPU type not available")
        logger.error("  - Network connectivity issues")
        logger.error("  - Pod deployment timeout")
        return False


async def build_and_push_image(push_to_registry: bool = False):
    """Build the Docker image for RunPod Pod deployment."""
    logger.info("🔨 Building Docker image for RunPod Pod...")
    
    import subprocess
    
    try:
        # Build the image
        build_cmd = [
            "docker", "build",
            "-f", "remote_service/Dockerfile",
            "-t", "remotemedia/remote-service:latest",
            "-t", "remotemedia/remote-service:runpod",
            "."
        ]
        
        result = subprocess.run(build_cmd, check=True, capture_output=True, text=True)
        logger.info("✅ Docker image built successfully")
        
        if push_to_registry:
            logger.info("📤 Pushing to Docker Hub...")
            push_commands = [
                ["docker", "push", "remotemedia/remote-service:latest"],
                ["docker", "push", "remotemedia/remote-service:runpod"]
            ]
            
            for cmd in push_commands:
                result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            
            logger.info("✅ Image pushed to registry")
            logger.info("RunPod can now access: remotemedia/remote-service:latest")
        else:
            logger.info("ℹ️  To push to registry, use --push flag")
            
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Build failed: {e}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="RunPod Pod VAD Example")
    parser.add_argument("--api-key", type=str, 
                       help="RunPod API key (overrides RUNPOD_API_KEY env var)")
    parser.add_argument("--gpu-type", type=str, default="RTX A4000",
                       help="GPU type to use (default: RTX A4000)")
    parser.add_argument("--test-connection", action="store_true",
                       help="Test RunPod Pod deployment and connection only")
    parser.add_argument("--build-image", action="store_true",
                       help="Build Docker image for RunPod Pod")
    parser.add_argument("--push", action="store_true",
                       help="Push image to registry (use with --build-image)")
    
    args = parser.parse_args()
    
    if args.build_image:
        success = asyncio.run(build_and_push_image(args.push))
    elif args.test_connection:
        api_key = args.api_key or os.environ.get("RUNPOD_API_KEY")
        if not api_key:
            logger.error("API key required for connection test")
            sys.exit(1)
        success = asyncio.run(test_runpod_pod_connection(api_key, args.gpu_type))
    else:
        success = asyncio.run(main(args.api_key, args.gpu_type))
    
    sys.exit(0 if success else 1)