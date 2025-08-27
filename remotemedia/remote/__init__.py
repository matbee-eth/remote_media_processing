"""
Remote execution components for the RemoteMedia SDK.
"""

from .client import RemoteExecutionClient
from .runpod_pod_client import RunPodPodRemoteExecutionClient, RunPodPodRemoteExecutorConfig, create_runpod_pod_client
from ..core.node import RemoteExecutorConfig

def create_remote_client(config):
    """
    Factory function to create the appropriate remote client based on config type.
    
    Args:
        config: RemoteExecutorConfig or RunPodPodRemoteExecutorConfig
        
    Returns:
        Appropriate remote client instance
    """
    if isinstance(config, RunPodPodRemoteExecutorConfig):
        return RunPodPodRemoteExecutionClient(config)
    elif isinstance(config, RemoteExecutorConfig):
        return RemoteExecutionClient(config)
    else:
        raise TypeError(f"Unsupported config type: {type(config)}")

__all__ = [
    "RemoteExecutionClient", 
    "RunPodPodRemoteExecutionClient", 
    "RunPodPodRemoteExecutorConfig",
    "create_remote_client",
    "create_runpod_pod_client"
] 