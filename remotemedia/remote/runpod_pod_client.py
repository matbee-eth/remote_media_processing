"""
RunPod Pod Remote Execution Client

This module provides RunPod Pod integration that works with our existing gRPC infrastructure.
Unlike the serverless approach, this deploys our full gRPC service as a RunPod Pod and 
discovers the connection details dynamically.
"""

import asyncio
import logging
import json
import os
import time
from typing import Any, Dict, Optional, Tuple
from dataclasses import dataclass, field
import runpod

from ..core.exceptions import RemoteExecutionError, ConfigurationError
from ..core.node import RemoteExecutorConfig
from .client import RemoteExecutionClient

logger = logging.getLogger(__name__)


@dataclass
class RunPodPodConfig:
    """Configuration for RunPod Pod deployment."""
    
    # Required
    api_key: str
    
    # Pod configuration  
    pod_name: str = "remotemedia-grpc"
    gpu_type: str = "RTX A4000"
    image: str = "acidhax/remotemedia-service:latest"
    volume_size: int = 20
    
    # Connection settings
    deploy_timeout: int = 300  # 5 minutes
    auto_terminate: bool = False  # Keep pod running by default
    
    # Advanced settings
    environment_vars: Dict[str, str] = field(default_factory=lambda: {
        "GRPC_PORT": "50051",
        "LOG_LEVEL": "INFO",
        "PYTHONUNBUFFERED": "1"
    })


class RunPodPodManager:
    """Manages RunPod Pod lifecycle for gRPC service deployment."""
    
    def __init__(self, config: RunPodPodConfig):
        """Initialize with RunPod Pod configuration."""
        self.config = config
        runpod.api_key = config.api_key
        self.pod_id: Optional[str] = None
        self.connection_info: Optional[Tuple[str, int]] = None
        logger.debug(f"RunPod API key set, length: {len(config.api_key) if config.api_key else 0}")
        
    async def deploy_pod(self) -> Tuple[str, int]:
        """
        Deploy the gRPC service as a RunPod Pod and return connection details.
        
        Returns:
            Tuple of (host, port) for gRPC connection
            
        Raises:
            RemoteExecutionError: If deployment fails
        """
        logger.info(f"Deploying RunPod Pod: {self.config.pod_name}")
        
        try:
            # Create the pod
            pod_response = await asyncio.to_thread(
                runpod.create_pod,
                name=self.config.pod_name,
                image_name=self.config.image,
                gpu_type_id=self.config.gpu_type,
                volume_in_gb=self.config.volume_size,
                container_disk_in_gb=self.config.volume_size,
                ports="50051/tcp",
                env=self.config.environment_vars
            )
            
            self.pod_id = pod_response['id']
            logger.info(f"Pod created: {self.pod_id}")
            
            # Wait for pod to be ready
            await self._wait_for_pod_ready()
            
            # Get connection info
            host, port = await self._get_connection_info()
            self.connection_info = (host, port)
            
            logger.info(f"Pod ready at {host}:{port}")
            return host, port
            
        except Exception as e:
            logger.error(f"Failed to deploy pod: {e}")
            if self.pod_id:
                await self._cleanup_pod()
            raise RemoteExecutionError(f"RunPod deployment failed: {e}") from e
    
    async def _wait_for_pod_ready(self) -> None:
        """Wait for the pod to reach RUNNING status."""
        if not self.pod_id:
            raise RuntimeError("No pod ID available")
            
        logger.info("Waiting for pod to be ready...")
        start_time = time.time()
        
        while time.time() - start_time < self.config.deploy_timeout:
            try:
                pod_info = await asyncio.to_thread(runpod.get_pod, self.pod_id)
                status = pod_info.get('desiredStatus', 'UNKNOWN')
                
                logger.debug(f"Pod status: {status}")
                
                if status == "RUNNING":
                    # Give the gRPC service a moment to start
                    await asyncio.sleep(10)
                    return
                elif status in ["FAILED", "TERMINATED"]:
                    raise RemoteExecutionError(f"Pod failed with status: {status}")
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.warning(f"Error checking pod status: {e}")
                await asyncio.sleep(5)
        
        raise RemoteExecutionError(f"Pod failed to become ready within {self.config.deploy_timeout}s")
    
    async def _get_connection_info(self) -> Tuple[str, int]:
        """Get the public IP and port for the gRPC service."""
        if not self.pod_id:
            raise RuntimeError("No pod ID available")
            
        logger.info("Waiting for pod runtime info to be available...")
        
        # Retry logic for getting connection info
        max_retries = 30  # 5 minutes with 10s intervals
        for attempt in range(max_retries):
            try:
                pod_info = await asyncio.to_thread(runpod.get_pod, self.pod_id)
                runtime = pod_info.get('runtime')
                
                if not runtime:
                    logger.debug(f"Attempt {attempt + 1}: Runtime info not yet available")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(10)
                        continue
                    else:
                        raise ValueError("Runtime info never became available")
                
                ports = runtime.get('ports', [])
                
                if not ports:
                    logger.debug(f"Attempt {attempt + 1}: No ports in runtime info yet")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(10)
                        continue
                    else:
                        raise ValueError("No ports found in pod runtime info")
                
                # Find the gRPC port (50051)
                grpc_port_info = None
                for port in ports:
                    if port.get('privatePort') == 50051:
                        grpc_port_info = port
                        break
                
                if not grpc_port_info:
                    logger.debug(f"Attempt {attempt + 1}: gRPC port 50051 not found in ports: {ports}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(10)
                        continue
                    else:
                        raise ValueError("gRPC port 50051 not found in pod ports")
                
                # Get public connection details
                host = grpc_port_info.get('ip') or pod_info.get('machine', {}).get('podHostIp')
                public_port = grpc_port_info.get('publicPort')
                
                if not host or not public_port:
                    logger.debug(f"Attempt {attempt + 1}: Missing host ({host}) or port ({public_port})")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(10)
                        continue
                    else:
                        raise ValueError("Could not determine host/port from pod info")
                
                logger.info(f"Connection info found: {host}:{public_port}")
                return str(host), int(public_port)
                
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to get connection info after {max_retries} attempts: {e}")
                    raise RemoteExecutionError(f"Could not get pod connection info: {e}") from e
                else:
                    logger.debug(f"Attempt {attempt + 1} failed: {e}")
                    await asyncio.sleep(10)
    
    async def terminate_pod(self) -> None:
        """Terminate the running pod."""
        if not self.pod_id:
            return
            
        try:
            logger.info(f"Terminating pod {self.pod_id}")
            await asyncio.to_thread(runpod.terminate_pod, self.pod_id)
            logger.info("Pod terminated successfully")
        except Exception as e:
            logger.error(f"Failed to terminate pod: {e}")
    
    async def _cleanup_pod(self) -> None:
        """Clean up pod on failure."""
        if self.config.auto_terminate:
            await self.terminate_pod()


class RunPodPodRemoteExecutorConfig(RemoteExecutorConfig):
    """
    Remote executor configuration that automatically deploys and connects to RunPod Pods.
    
    This extends the standard RemoteExecutorConfig to add RunPod Pod management.
    Once deployed, it works exactly like a regular gRPC connection.
    """
    
    def __init__(
        self,
        api_key: str,
        pod_name: str = "remotemedia-grpc",
        gpu_type: str = "RTX A4000", 
        image: str = "acidhax/remotemedia-service:latest",
        auto_terminate: bool = False,
        deploy_timeout: int = 300,
        **kwargs
    ):
        """
        Initialize RunPod Pod configuration.
        
        Args:
            api_key: RunPod API key
            pod_name: Name for the pod
            gpu_type: GPU type to use
            image: Docker image containing gRPC service
            auto_terminate: Whether to auto-terminate pod on cleanup
            deploy_timeout: Timeout for pod deployment
            **kwargs: Additional RemoteExecutorConfig arguments
        """
        # Initialize with placeholder values - will be updated after deployment
        super().__init__(
            host="pending",
            port=50051,
            **kwargs
        )
        
        # RunPod-specific configuration
        self.runpod_config = RunPodPodConfig(
            api_key=api_key,
            pod_name=pod_name,
            gpu_type=gpu_type,
            image=image,
            auto_terminate=auto_terminate,
            deploy_timeout=deploy_timeout
        )
        
        self.pod_manager: Optional[RunPodPodManager] = None
        self._deployed = False
    
    async def deploy(self) -> Tuple[str, int]:
        """
        Deploy the RunPod Pod and get connection details.
        
        Returns:
            Tuple of (host, port) for the deployed service
        """
        if self._deployed:
            return self.host, self.port
            
        self.pod_manager = RunPodPodManager(self.runpod_config)
        host, port = await self.pod_manager.deploy_pod()
        
        # Update connection details
        self.host = host
        self.port = port
        self._deployed = True
        
        logger.info(f"RunPod Pod deployed and ready: {host}:{port}")
        return host, port
    
    async def cleanup(self) -> None:
        """Clean up the RunPod Pod if auto_terminate is enabled."""
        if self.pod_manager and self.runpod_config.auto_terminate:
            await self.pod_manager.terminate_pod()
    
    @property
    def provider(self) -> str:
        return "runpod-pod"


class RunPodPodRemoteExecutionClient(RemoteExecutionClient):
    """
    Remote execution client that automatically handles RunPod Pod deployment.
    
    This wraps the standard gRPC RemoteExecutionClient and adds automatic
    RunPod Pod management. It's a drop-in replacement that handles the 
    deployment complexity behind the scenes.
    """
    
    def __init__(self, config: RunPodPodRemoteExecutorConfig):
        """Initialize with RunPod Pod configuration."""
        self.runpod_config = config
        # Don't call super().__init__ yet - we need to deploy first
        self._grpc_client: Optional[RemoteExecutionClient] = None
    
    async def connect(self) -> None:
        """Deploy the RunPod Pod and establish gRPC connection."""
        logger.info("Deploying RunPod Pod and establishing connection...")
        
        # Deploy the pod
        host, port = await self.runpod_config.deploy()
        
        # Create and connect the underlying gRPC client
        grpc_config = RemoteExecutorConfig(
            host=host,
            port=port,
            protocol=self.runpod_config.protocol,
            auth_token=self.runpod_config.auth_token,
            timeout=self.runpod_config.timeout,
            max_retries=self.runpod_config.max_retries,
            ssl_enabled=False  # RunPod Pods don't have SSL certificates
        )
        
        self._grpc_client = RemoteExecutionClient(grpc_config)
        await self._grpc_client.connect()
        
        logger.info("RunPod Pod gRPC connection established")
    
    async def disconnect(self) -> None:
        """Disconnect and optionally terminate the pod."""
        if self._grpc_client:
            await self._grpc_client.disconnect()
        
        await self.runpod_config.cleanup()
    
    # Delegate all other methods to the underlying gRPC client
    def __getattr__(self, name):
        if self._grpc_client:
            return getattr(self._grpc_client, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}' (client not connected)")


# Convenience function for simple usage
async def create_runpod_pod_client(
    api_key: str,
    pod_name: str = "remotemedia-grpc",
    gpu_type: str = "RTX A4000",
    **kwargs
) -> RemoteExecutionClient:
    """
    Create and connect a RunPod Pod gRPC client.
    
    This is a convenience function that handles all the deployment and connection
    automatically. Returns a standard RemoteExecutionClient interface.
    
    Args:
        api_key: RunPod API key
        pod_name: Name for the pod
        gpu_type: GPU type to use
        **kwargs: Additional configuration options
        
    Returns:
        Connected RemoteExecutionClient
    """
    config = RunPodPodRemoteExecutorConfig(
        api_key=api_key,
        pod_name=pod_name,
        gpu_type=gpu_type,
        **kwargs
    )
    
    client = RunPodPodRemoteExecutionClient(config)
    await client.connect()
    
    return client