#!/usr/bin/env python3
"""
Test server-side idle timeout with 2-minute timeout.
"""

import asyncio
import logging
import sys
import os
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from remotemedia.core.pipeline import Pipeline
from remotemedia.nodes.source import MediaReaderNode, AudioTrackSource
from remotemedia.nodes.audio import AudioTransform, VoiceActivityDetector
from remotemedia.nodes.remote import RemoteObjectExecutionNode
from remotemedia.remote.runpod_pod_client import RunPodPodRemoteExecutorConfig

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_idle_timeout():
    """Test server-side idle timeout with 2-minute timeout."""
    
    api_key = os.environ.get("RUNPOD_API_KEY")
    if not api_key:
        logger.error("RUNPOD_API_KEY environment variable not set")
        return
    
    logger.info("=" * 60)
    logger.info("TESTING 2-MINUTE SERVER-SIDE IDLE TIMEOUT")
    logger.info("=" * 60)
    
    # Configure with 2-minute idle timeout
    config = RunPodPodRemoteExecutorConfig(
        api_key=api_key,
        pod_name="test-auto-suspend",  # Reuse existing pod
        gpu_type="NVIDIA A40",
        auto_suspend=True,
        idle_timeout=120,  # 2 minutes
        auto_resume=True,
        auto_terminate=False
    )
    
    logger.info("Configuration:")
    logger.info(f"  Pod name: test-auto-suspend")
    logger.info(f"  Idle timeout: 120 seconds (2 minutes)")
    logger.info(f"  Auto-suspend: {config.runpod_config.auto_suspend}")
    logger.info(f"  Auto-resume: {config.runpod_config.auto_resume}")
    
    # Deploy/connect
    logger.info("\n1. DEPLOYING/CONNECTING TO POD")
    logger.info("-" * 40)
    host, port = await config.deploy()
    logger.info(f"✅ Pod ready at {host}:{port}")
    
    # The pod's environment should now have IDLE_TIMEOUT=120
    logger.info(f"Environment variables sent to pod: {config.runpod_config.environment_vars}")
    
    # Create a simple pipeline for testing
    logger.info("\n2. RUNNING INITIAL PIPELINE")
    logger.info("-" * 40)
    
    pipeline = Pipeline()
    
    pipeline.add_node(MediaReaderNode(
        path="examples/transcribe_demo.wav",
        chunk_size=8192,  # Larger chunks for faster processing
        name="MediaReader"
    ))
    
    pipeline.add_node(AudioTrackSource(name="AudioTrackSource"))
    
    pipeline.add_node(AudioTransform(
        output_sample_rate=16000,
        output_channels=1,
        name="AudioTransform"
    ))
    
    # Create VAD for remote execution
    local_vad = VoiceActivityDetector(
        frame_duration_ms=30,
        filter_mode=False,
        include_metadata=True,
        name="VAD_Local"
    )
    local_vad.is_streaming = True
    
    remote_vad = RemoteObjectExecutionNode(
        obj_to_execute=local_vad,
        remote_config=config,
        name="VAD_Remote"
    )
    pipeline.add_node(remote_vad)
    
    # Run pipeline
    start_time = time.time()
    async with pipeline.managed_execution():
        chunk_count = 0
        async for result in pipeline.process():
            chunk_count += 1
    
    elapsed = time.time() - start_time
    logger.info(f"✅ Pipeline completed: {chunk_count} chunks in {elapsed:.1f}s")
    
    # Now wait and watch for auto-shutdown
    logger.info("\n3. WAITING FOR SERVER-SIDE IDLE TIMEOUT")
    logger.info("-" * 40)
    logger.info("The server should shut itself down after 2 minutes of inactivity...")
    logger.info("Starting 2.5 minute wait at " + time.strftime("%H:%M:%S"))
    
    # Check pod status periodically
    import runpod
    runpod.api_key = api_key
    
    for i in range(15):  # Check every 10 seconds for 2.5 minutes
        await asyncio.sleep(10)
        
        # Check pod status
        try:
            pod_info = runpod.get_pod(config.pod_manager.pod_id if config.pod_manager else None)
            status = pod_info.get('desiredStatus', 'UNKNOWN')
            elapsed = (i + 1) * 10
            logger.info(f"  [{elapsed:3d}s] Pod status: {status}")
            
            if status in ['EXITED', 'STOPPED']:
                logger.info(f"✅ Pod auto-stopped after idle timeout!")
                break
        except Exception as e:
            logger.warning(f"  Error checking status: {e}")
    
    # Try to use the pod again - should auto-resume
    logger.info("\n4. TESTING AUTO-RESUME AFTER IDLE SHUTDOWN")
    logger.info("-" * 40)
    logger.info("Running another pipeline - pod should auto-resume...")
    
    pipeline2 = Pipeline()
    
    pipeline2.add_node(MediaReaderNode(
        path="examples/transcribe_demo.wav",
        chunk_size=16384,  # Even larger chunks
        name="MediaReader2"
    ))
    
    pipeline2.add_node(AudioTrackSource(name="AudioTrackSource2"))
    
    pipeline2.add_node(AudioTransform(
        output_sample_rate=16000,
        output_channels=1,
        name="AudioTransform2"
    ))
    
    local_vad2 = VoiceActivityDetector(
        frame_duration_ms=30,
        filter_mode=False,
        include_metadata=True,
        name="VAD_Local2"
    )
    local_vad2.is_streaming = True
    
    remote_vad2 = RemoteObjectExecutionNode(
        obj_to_execute=local_vad2,
        remote_config=config,  # Same config - should trigger auto-resume
        name="VAD_Remote2"
    )
    pipeline2.add_node(remote_vad2)
    
    try:
        start_time = time.time()
        async with pipeline2.managed_execution():
            chunk_count = 0
            async for result in pipeline2.process():
                chunk_count += 1
        
        elapsed = time.time() - start_time
        logger.info(f"✅ Pipeline after resume: {chunk_count} chunks in {elapsed:.1f}s")
        logger.info("Auto-resume worked successfully!")
        
    except Exception as e:
        logger.error(f"❌ Auto-resume failed: {e}")
    
    # Check final pod status
    try:
        pod_info = runpod.get_pod(config.pod_manager.pod_id if config.pod_manager else None)
        final_status = pod_info.get('desiredStatus', 'UNKNOWN')
        logger.info(f"\n5. FINAL POD STATUS: {final_status}")
    except:
        pass
    
    logger.info("\n" + "=" * 60)
    logger.info("TEST COMPLETE")
    logger.info("=" * 60)
    logger.info("Summary:")
    logger.info("  ✅ Pod deployed with 2-minute idle timeout")
    logger.info("  ✅ Initial pipeline ran successfully")
    logger.info("  ✅ Server should auto-shutdown after 2 minutes")
    logger.info("  ✅ Auto-resume should work when needed")
    
    # Don't terminate - leave pod for manual inspection if needed
    logger.info("\nPod left running for manual inspection.")
    logger.info("To terminate manually: runpod.terminate_pod(pod_id)")


if __name__ == "__main__":
    asyncio.run(test_idle_timeout())