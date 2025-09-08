#!/usr/bin/env python3
"""
Test script to demonstrate RunPod Pod auto-suspend/resume functionality.
"""

import asyncio
import logging
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from remotemedia.remote.runpod_pod_client import RunPodPodRemoteExecutorConfig, create_runpod_pod_client
from remotemedia.nodes.audio import VoiceActivityDetector
from remotemedia.nodes.remote import RemoteObjectExecutionNode

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_auto_suspend():
    """Test the auto-suspend and resume functionality."""
    
    api_key = os.environ.get("RUNPOD_API_KEY")
    if not api_key:
        logger.error("RUNPOD_API_KEY environment variable not set")
        return
    
    # Configure with aggressive idle timeout for testing
    config = RunPodPodRemoteExecutorConfig(
        api_key=api_key,
        pod_name="test-auto-suspend",
        gpu_type="NVIDIA A40",
        auto_suspend=True,
        idle_timeout=60,  # 1 minute idle timeout for testing
        auto_resume=True,
        auto_terminate=False  # Keep pod around for testing
    )
    
    logger.info("=== Testing Auto-Suspend/Resume ===")
    
    # Deploy/connect to pod
    logger.info("1. Deploying pod...")
    host, port = await config.deploy()
    logger.info(f"   Pod deployed at {host}:{port}")
    logger.info(f"   Is suspended: {config.is_suspended}")
    
    # Create a simple VAD node for testing
    vad = VoiceActivityDetector(
        energy_threshold=0.01,
        speech_threshold=0.2
    )
    
    # Create client
    client = await create_runpod_pod_client(
        api_key=api_key,
        pod_name="test-auto-suspend",
        auto_suspend=True,
        idle_timeout=60
    )
    
    # Execute something to mark activity
    logger.info("2. Executing test task...")
    test_data = (b"\x00" * 1000, 16000)  # Dummy audio data
    result = await client.execute_object_method(vad, "process", [test_data])
    logger.info(f"   Task executed successfully")
    
    # Wait for idle timeout
    logger.info("3. Waiting for auto-suspend (60 seconds)...")
    await asyncio.sleep(70)  # Wait a bit more than idle timeout
    
    # Check if suspended
    logger.info(f"4. Pod suspended: {config.is_suspended}")
    
    # Try to execute again - should auto-resume
    logger.info("5. Executing another task (should auto-resume)...")
    result = await client.execute_object_method(vad, "process", [test_data])
    logger.info(f"   Task executed after auto-resume")
    
    # Manually suspend
    logger.info("6. Manually suspending pod...")
    await config.suspend()
    logger.info(f"   Pod suspended: {config.is_suspended}")
    
    # Manually resume
    logger.info("7. Manually resuming pod...")
    host, port = await config.resume()
    logger.info(f"   Pod resumed at {host}:{port}")
    
    # Cleanup
    logger.info("8. Disconnecting (pod will stay alive)...")
    await client.disconnect()
    
    logger.info("=== Test Complete ===")
    logger.info("Pod is still running but can be terminated manually")
    logger.info(f"Pod name: test-auto-suspend")


async def test_existing_pod_reuse():
    """Test reusing an existing pod."""
    
    api_key = os.environ.get("RUNPOD_API_KEY")
    if not api_key:
        logger.error("RUNPOD_API_KEY environment variable not set")
        return
    
    logger.info("=== Testing Existing Pod Reuse ===")
    
    # Try to connect to the pod created in previous test
    config = RunPodPodRemoteExecutorConfig(
        api_key=api_key,
        pod_name="test-auto-suspend",
        gpu_type="NVIDIA A40",
        auto_suspend=True,
        idle_timeout=60
    )
    
    logger.info("1. Looking for existing pod 'test-auto-suspend'...")
    host, port = await config.deploy()
    logger.info(f"   Connected to existing pod at {host}:{port}")
    
    # Create client
    client = await create_runpod_pod_client(
        api_key=api_key,
        pod_name="test-auto-suspend"
    )
    
    # Execute a task
    logger.info("2. Executing task on existing pod...")
    vad = VoiceActivityDetector()
    test_data = (b"\x00" * 1000, 16000)
    result = await client.execute_object_method(vad, "process", [test_data])
    logger.info(f"   Task executed successfully on existing pod")
    
    # Cleanup - terminate the pod this time
    logger.info("3. Terminating pod...")
    if config.pod_manager:
        await config.pod_manager.terminate_pod()
    logger.info("   Pod terminated")
    
    logger.info("=== Test Complete ===")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test RunPod auto-suspend/resume")
    parser.add_argument("--test", choices=["suspend", "reuse", "both"], default="both",
                        help="Which test to run")
    args = parser.parse_args()
    
    if args.test in ["suspend", "both"]:
        asyncio.run(test_auto_suspend())
    
    if args.test in ["reuse", "both"]:
        asyncio.run(test_existing_pod_reuse())