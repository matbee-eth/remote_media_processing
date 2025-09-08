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
    
    # Auto-suspend settings
    auto_suspend: bool = True  # Auto-suspend pod when idle
    idle_timeout: int = 300  # 5 minutes idle before suspend
    auto_resume: bool = True  # Auto-resume pod when needed
    
    # Advanced settings
    environment_vars: Dict[str, str] = field(default_factory=dict)
    
    def __post_init__(self):
        """Set up environment variables including idle timeout."""
        # Default environment variables
        default_env = {
            "GRPC_PORT": "50051",
            "LOG_LEVEL": "INFO",
            "PYTHONUNBUFFERED": "1"
        }
        
        # Add idle timeout if auto-suspend is enabled
        if self.auto_suspend and self.idle_timeout > 0:
            default_env["IDLE_TIMEOUT"] = str(self.idle_timeout)
            default_env["RUNPOD_API_KEY"] = self.api_key
            default_env["RUNPOD_POD_NAME"] = self.pod_name
        
        # Merge with user-provided environment variables
        for key, value in default_env.items():
            if key not in self.environment_vars:
                self.environment_vars[key] = value


class RunPodPodManager:
    """Manages RunPod Pod lifecycle for gRPC service deployment."""
    
    def __init__(self, config: RunPodPodConfig):
        """Initialize with RunPod Pod configuration."""
        self.config = config
        runpod.api_key = config.api_key
        self.pod_id: Optional[str] = None
        self.connection_info: Optional[Tuple[str, int]] = None
        self.last_activity_time: float = time.time()
        self.is_suspended: bool = False
        self._idle_check_task: Optional[asyncio.Task] = None
        logger.debug(f"RunPod API key set, length: {len(config.api_key) if config.api_key else 0}")
        
    async def find_existing_pod(self) -> Optional[str]:
        """
        Find an existing pod with the same name.
        
        Returns:
            Pod ID if found, None otherwise
        """
        try:
            pods = await asyncio.to_thread(runpod.get_pods)
            for pod in pods:
                if pod.get('name') == self.config.pod_name:
                    status = pod.get('desiredStatus', '')
                    pod_id = pod.get('id')
                    logger.info(f"Found existing pod '{self.config.pod_name}' with ID {pod_id}, status: {status}")
                    
                    if status in ['RUNNING', 'EXITED']:
                        return pod_id
                    elif status == 'STOPPED':
                        self.is_suspended = True
                        return pod_id
            return None
        except Exception as e:
            logger.warning(f"Error checking for existing pods: {e}")
            return None
    
    async def deploy_pod(self) -> Tuple[str, int]:
        """
        Deploy the gRPC service as a RunPod Pod and return connection details.
        Will reuse existing pod if found.
        
        Returns:
            Tuple of (host, port) for gRPC connection
            
        Raises:
            RemoteExecutionError: If deployment fails
        """
        # Check for existing pod first
        existing_pod_id = await self.find_existing_pod()
        if existing_pod_id:
            self.pod_id = existing_pod_id
            logger.info(f"Reusing existing pod: {self.pod_id}")
            
            if self.is_suspended:
                # Resume the suspended pod
                return await self.resume_pod()
            else:
                # Get connection info for running pod
                try:
                    host, port = await self._get_connection_info()
                    self.connection_info = (host, port)
                    
                    # Start idle monitoring if configured
                    self.start_idle_monitoring()
                    
                    logger.info(f"Connected to existing pod at {host}:{port}")
                    return host, port
                except Exception as e:
                    logger.warning(f"Failed to connect to existing pod: {e}, creating new pod")
                    # Fall through to create new pod
        
        logger.info(f"Deploying new RunPod Pod: {self.config.pod_name}")
        
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
            
            # Start idle monitoring if configured
            self.start_idle_monitoring()
            
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
    
    async def suspend_pod(self) -> None:
        """Suspend (stop) the running pod to save costs."""
        if not self.pod_id or self.is_suspended:
            return
            
        try:
            logger.info(f"Suspending pod {self.pod_id}")
            await asyncio.to_thread(runpod.stop_pod, self.pod_id)
            self.is_suspended = True
            logger.info("Pod suspended successfully")
        except Exception as e:
            logger.error(f"Failed to suspend pod: {e}")
    
    async def resume_pod(self) -> Tuple[str, int]:
        """Resume (restart) a suspended pod."""
        if not self.pod_id or not self.is_suspended:
            if self.connection_info:
                return self.connection_info
            raise RuntimeError("Pod not suspended or no pod ID available")
            
        try:
            logger.info(f"Resuming pod {self.pod_id}")
            # RunPod resume_pod requires gpu_count parameter
            await asyncio.to_thread(runpod.resume_pod, self.pod_id, 1)
            self.is_suspended = False
            
            # Wait for pod to be ready again
            await self._wait_for_pod_ready()
            
            # Get new connection info (might have changed)
            host, port = await self._get_connection_info()
            self.connection_info = (host, port)
            
            logger.info(f"Pod resumed at {host}:{port}")
            return host, port
            
        except Exception as e:
            logger.error(f"Failed to resume pod: {e}")
            raise RemoteExecutionError(f"Pod resume failed: {e}") from e
    
    async def terminate_pod(self) -> None:
        """Terminate the running pod."""
        if not self.pod_id:
            return
            
        # Cancel idle check if running
        if self._idle_check_task:
            self._idle_check_task.cancel()
            self._idle_check_task = None
            
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
    
    def mark_activity(self) -> None:
        """Mark that the pod was used (reset idle timer)."""
        self.last_activity_time = time.time()
        logger.debug(f"Activity marked at {self.last_activity_time}")
    
    async def _idle_check_loop(self) -> None:
        """Background task to check for idle timeout and auto-suspend."""
        if not self.config.auto_suspend:
            return
            
        logger.info(f"Starting idle check loop with {self.config.idle_timeout}s timeout")
        
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                if self.is_suspended:
                    continue
                    
                idle_time = time.time() - self.last_activity_time
                logger.debug(f"Pod idle for {idle_time:.1f}s")
                
                if idle_time > self.config.idle_timeout:
                    logger.info(f"Pod idle for {idle_time:.1f}s, suspending...")
                    await self.suspend_pod()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in idle check loop: {e}")
                await asyncio.sleep(60)  # Wait longer on error
    
    def start_idle_monitoring(self) -> None:
        """Start monitoring for idle timeout."""
        # Only do client-side idle monitoring if server-side idle timeout is not configured
        if (self.config.auto_suspend and not self._idle_check_task and 
            self.config.idle_timeout == 0):  # Server-side idle timeout disabled
            logger.info("Starting client-side idle monitoring (server-side disabled)")
            self._idle_check_task = asyncio.create_task(self._idle_check_loop())
        elif self.config.auto_suspend and self.config.idle_timeout > 0:
            logger.info(f"Skipping client-side idle monitoring (server-side {self.config.idle_timeout}s enabled)")


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
        auto_suspend: bool = True,
        idle_timeout: int = 300,
        auto_resume: bool = True,
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
            auto_suspend: Whether to auto-suspend pod when idle
            idle_timeout: Seconds of inactivity before auto-suspend
            auto_resume: Whether to auto-resume pod when needed
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
            auto_suspend=auto_suspend,
            idle_timeout=idle_timeout,
            auto_resume=auto_resume,
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
    
    async def suspend(self) -> None:
        """Manually suspend the pod to save costs."""
        if self.pod_manager:
            await self.pod_manager.suspend_pod()
    
    async def resume(self) -> Tuple[str, int]:
        """Manually resume a suspended pod."""
        if not self.pod_manager:
            raise RuntimeError("No pod manager available")
        return await self.pod_manager.resume_pod()
    
    @property
    def provider(self) -> str:
        return "runpod-pod"
    
    @property
    def is_suspended(self) -> bool:
        """Check if the pod is currently suspended."""
        return self.pod_manager.is_suspended if self.pod_manager else False


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
        
        # Deploy the pod (or resume if suspended)
        if self.runpod_config.pod_manager and self.runpod_config.pod_manager.is_suspended:
            if self.runpod_config.runpod_config.auto_resume:
                logger.info("Pod is suspended, resuming...")
                host, port = await self.runpod_config.pod_manager.resume_pod()
            else:
                raise RemoteExecutionError("Pod is suspended and auto_resume is disabled")
        else:
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
    
    def _mark_activity(self):
        """Mark activity on the pod manager."""
        if self.runpod_config.pod_manager:
            self.runpod_config.pod_manager.mark_activity()
    
    async def execute_node(self, *args, **kwargs):
        """Execute a node remotely with activity tracking."""
        self._mark_activity()
        if not self._grpc_client:
            await self.connect()
        return await self._grpc_client.execute_node(*args, **kwargs)
    
    async def stream_node(self, *args, **kwargs):
        """Stream from a node remotely with activity tracking."""
        self._mark_activity()
        if not self._grpc_client:
            await self.connect()
        return await self._grpc_client.stream_node(*args, **kwargs)
    
    async def stream_object(self, *args, **kwargs):
        """Stream from an object remotely with activity tracking."""
        self._mark_activity()
        if not self._grpc_client:
            await self.connect()
        # Don't await - stream_object returns an AsyncGenerator
        async for result in self._grpc_client.stream_object(*args, **kwargs):
            yield result
    
    async def execute_object_method(self, *args, **kwargs):
        """Execute an object method remotely with activity tracking."""
        self._mark_activity()
        if not self._grpc_client:
            await self.connect()
        return await self._grpc_client.execute_object_method(*args, **kwargs)
    
    async def execute_custom_task(self, *args, **kwargs):
        """Execute a custom task remotely with activity tracking."""
        self._mark_activity()
        if not self._grpc_client:
            await self.connect()
        return await self._grpc_client.execute_custom_task(*args, **kwargs)
    
    # Delegate all other methods to the underlying gRPC client
    def __getattr__(self, name):
        if self._grpc_client:
            # Mark activity for any method call
            self._mark_activity()
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