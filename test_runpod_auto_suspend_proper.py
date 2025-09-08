#!/usr/bin/env python3
"""
Test RunPod auto-suspend/resume with proper VAD pipeline.
Based on test_vad_with_remote.py
"""

import asyncio
import logging
import sys
import os
from pathlib import Path
import time

sys.path.insert(0, str(Path(__file__).parent))

from remotemedia.core.pipeline import Pipeline
from remotemedia.core.node import RemoteExecutorConfig, Node
from remotemedia.nodes.source import MediaReaderNode, AudioTrackSource
from remotemedia.nodes.audio import AudioTransform, VoiceActivityDetector
from remotemedia.nodes.remote import RemoteObjectExecutionNode
from remotemedia.remote.runpod_pod_client import RunPodPodRemoteExecutorConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


class VADStats(Node):
    """Collect statistics from VAD output."""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.is_streaming = True
        self.total_chunks = 0
        self.speech_chunks = 0
        
    async def process(self, data_stream):
        async for data in data_stream:
            self.total_chunks += 1
            
            if isinstance(data, tuple) and len(data) == 2 and isinstance(data[1], dict):
                (audio_data, metadata) = data
                if metadata.get('is_speech'):
                    self.speech_chunks += 1
                logger.debug(f"VAD: is_speech={metadata.get('is_speech')}, ratio={metadata.get('speech_ratio', 0):.2f}")
                yield audio_data
            else:
                yield data
        
        logger.info(f"VAD Stats: {self.speech_chunks}/{self.total_chunks} chunks contained speech")


async def test_auto_suspend_with_pipeline():
    """Test auto-suspend/resume with real VAD pipeline."""
    
    api_key = os.environ.get("RUNPOD_API_KEY")
    if not api_key:
        logger.error("RUNPOD_API_KEY environment variable not set")
        return
    
    logger.info("\n=== Testing Auto-Suspend/Resume with VAD Pipeline ===\n")
    
    # Configure RunPod with aggressive idle timeout for testing
    runpod_config = RunPodPodRemoteExecutorConfig(
        api_key=api_key,
        pod_name="test-auto-suspend",  # REUSE EXISTING POD
        gpu_type="NVIDIA A40",
        auto_suspend=True,
        idle_timeout=60,  # 1 minute for testing
        auto_resume=True,
        auto_terminate=False  # Keep pod alive for testing
    )
    
    # First pipeline run
    logger.info("1. FIRST RUN - Deploy pod and process audio")
    logger.info("=" * 50)
    
    pipeline = Pipeline()
    
    # Add source nodes
    pipeline.add_node(MediaReaderNode(
        path="examples/transcribe_demo.wav",
        chunk_size=4096,
        name="MediaReader"
    ))
    
    pipeline.add_node(AudioTrackSource(name="AudioTrackSource"))
    
    pipeline.add_node(AudioTransform(
        output_sample_rate=16000,
        output_channels=1,
        name="AudioTransform"
    ))
    
    # Create VAD node for remote execution
    local_vad = VoiceActivityDetector(
        frame_duration_ms=30,
        filter_mode=False,
        include_metadata=True,
        name="VAD_Local"
    )
    local_vad.is_streaming = True
    
    # Wrap VAD for remote execution on RunPod
    remote_vad = RemoteObjectExecutionNode(
        obj_to_execute=local_vad,
        remote_config=runpod_config,
        name="VAD_RunPod"
    )
    pipeline.add_node(remote_vad)
    
    # Add stats collector
    pipeline.add_node(VADStats(name="VADStats"))
    
    # Run first pipeline
    start_time = time.time()
    async with pipeline.managed_execution():
        chunk_count = 0
        async for result in pipeline.process():
            chunk_count += 1
        logger.info(f"First run processed {chunk_count} chunks in {time.time() - start_time:.1f}s")
    
    # Check pod status
    logger.info(f"Pod suspended: {runpod_config.is_suspended}")
    
    # Wait for idle timeout
    logger.info("\n2. WAITING FOR AUTO-SUSPEND")
    logger.info("=" * 50)
    logger.info(f"Waiting {runpod_config.runpod_config.idle_timeout + 10} seconds for auto-suspend...")
    await asyncio.sleep(runpod_config.runpod_config.idle_timeout + 10)
    
    # Pod should be suspended now
    logger.info(f"After idle timeout - Pod suspended: {runpod_config.is_suspended}")
    
    # Second pipeline run - should auto-resume
    logger.info("\n3. SECOND RUN - Should auto-resume suspended pod")
    logger.info("=" * 50)
    
    pipeline2 = Pipeline()
    
    # Same pipeline setup
    pipeline2.add_node(MediaReaderNode(
        path="examples/transcribe_demo.wav",
        chunk_size=4096,
        name="MediaReader2"
    ))
    
    pipeline2.add_node(AudioTrackSource(name="AudioTrackSource2"))
    
    pipeline2.add_node(AudioTransform(
        output_sample_rate=16000,
        output_channels=1,
        name="AudioTransform2"
    ))
    
    # Reuse the same RunPod config - should auto-resume
    local_vad2 = VoiceActivityDetector(
        frame_duration_ms=30,
        filter_mode=False,
        include_metadata=True,
        name="VAD_Local2"
    )
    local_vad2.is_streaming = True
    
    remote_vad2 = RemoteObjectExecutionNode(
        obj_to_execute=local_vad2,
        remote_config=runpod_config,
        name="VAD_RunPod2"
    )
    pipeline2.add_node(remote_vad2)
    
    pipeline2.add_node(VADStats(name="VADStats2"))
    
    # Run second pipeline - should trigger auto-resume
    start_time = time.time()
    async with pipeline2.managed_execution():
        chunk_count = 0
        async for result in pipeline2.process():
            chunk_count += 1
        logger.info(f"Second run processed {chunk_count} chunks in {time.time() - start_time:.1f}s")
    
    logger.info(f"After second run - Pod suspended: {runpod_config.is_suspended}")
    
    # Manual suspend/resume test
    logger.info("\n4. MANUAL SUSPEND/RESUME TEST")
    logger.info("=" * 50)
    
    logger.info("Manually suspending pod...")
    await runpod_config.suspend()
    logger.info(f"Pod suspended: {runpod_config.is_suspended}")
    
    logger.info("Manually resuming pod...")
    host, port = await runpod_config.resume()
    logger.info(f"Pod resumed at {host}:{port}")
    
    # Cleanup
    logger.info("\n5. CLEANUP")
    logger.info("=" * 50)
    logger.info("Terminating pod...")
    if runpod_config.pod_manager:
        await runpod_config.pod_manager.terminate_pod()
    logger.info("Pod terminated")
    
    logger.info("\n=== Test Complete ===")


async def test_existing_pod_reuse():
    """Test that we can find and reuse existing pods."""
    
    api_key = os.environ.get("RUNPOD_API_KEY")
    if not api_key:
        logger.error("RUNPOD_API_KEY environment variable not set")
        return
    
    logger.info("\n=== Testing Existing Pod Discovery ===\n")
    
    # First, create a pod
    logger.info("1. Creating initial pod...")
    config1 = RunPodPodRemoteExecutorConfig(
        api_key=api_key,
        pod_name="test-auto-suspend",  # USE THE SAME POD NAME
        gpu_type="NVIDIA A40",
        auto_suspend=False,  # Keep it running
        auto_terminate=False
    )
    
    host1, port1 = await config1.deploy()
    logger.info(f"Pod created at {host1}:{port1}")
    
    # Now try to connect with a new config object
    logger.info("\n2. Attempting to reuse pod with new config...")
    config2 = RunPodPodRemoteExecutorConfig(
        api_key=api_key,
        pod_name="test-auto-suspend",  # SAME POD NAME
        gpu_type="NVIDIA A40"
    )
    
    host2, port2 = await config2.deploy()
    logger.info(f"Connected to existing pod at {host2}:{port2}")
    
    # Should be the same pod
    if host1 == host2 and port1 == port2:
        logger.info("✅ Successfully reused existing pod!")
    else:
        logger.warning("❌ Created new pod instead of reusing")
    
    # Cleanup
    logger.info("\n3. Cleaning up...")
    if config2.pod_manager:
        await config2.pod_manager.terminate_pod()
    logger.info("Pod terminated")
    
    logger.info("\n=== Test Complete ===")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test RunPod auto-suspend/resume with VAD pipeline")
    parser.add_argument("--test", choices=["suspend", "reuse", "both"], default="suspend",
                        help="Which test to run")
    args = parser.parse_args()
    
    if args.test in ["suspend", "both"]:
        asyncio.run(test_auto_suspend_with_pipeline())
    
    if args.test in ["reuse", "both"]:
        asyncio.run(test_existing_pod_reuse())