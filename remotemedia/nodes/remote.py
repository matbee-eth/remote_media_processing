"""
Node for executing other nodes on a remote service.
"""
from typing import Any, Dict, AsyncGenerator, Optional
import logging
import asyncio

from ..core.node import Node, RemoteExecutorConfig
from ..core.exceptions import NodeError
from ..remote.client import RemoteExecutionClient
from ..remote.runpod_pod_client import RunPodPodRemoteExecutionClient, RunPodPodRemoteExecutorConfig
from ..remote import create_remote_client

logger = logging.getLogger(__name__)


class RemoteExecutionNode(Node):
    """
    A gateway node that executes a specified node type on a remote service.
    
    This node acts as a bridge in a local pipeline, sending its input data
    to a remote service for processing by another node and then passing the
    result on to the next local node. This version supports streaming.
    """

    def __init__(self, node_to_execute: str, remote_config, 
                 node_config: Dict[str, Any] = None, serialization_format: str = "pickle", **kwargs):
        """
        Initializes the RemoteExecutionNode.

        Args:
            node_to_execute (str): The class name of the node to execute remotely.
            remote_config: Configuration for the remote connection (RemoteExecutorConfig or RunPodPodRemoteExecutorConfig).
            node_config (Dict[str, Any], optional): The configuration for the remote node itself. Defaults to None.
            serialization_format (str, optional): Serialization format to use. Defaults to "pickle".
        """
        super().__init__(**kwargs)
        if not isinstance(remote_config, (RemoteExecutorConfig, RunPodPodRemoteExecutorConfig)):
            raise ValueError("remote_config must be a valid RemoteExecutorConfig or RunPodPodRemoteExecutorConfig instance.")
            
        self.node_to_execute = node_to_execute
        self.remote_config = remote_config
        self.node_config = node_config or {}
        self.serialization_format = serialization_format
        self.is_streaming = True  # Mark as a streaming node
        self.client = None  # Will be set to appropriate client type

    async def initialize(self):
        """Initializes the remote execution client and connects."""
        await super().initialize()
        self.client = create_remote_client(self.remote_config)
        
        # Connect if it's a gRPC client, otherwise prepare for use
        if hasattr(self.client, 'connect'):
            await self.client.connect()
            logger.info(f"RemoteExecutionNode '{self.name}' connected to {self.remote_config.host}:{self.remote_config.port}")
        else:
            logger.info(f"RemoteExecutionNode '{self.name}' configured for {self.remote_config.provider} execution")

    async def cleanup(self):
        """Cleans up the client connection."""
        if self.client:
            if hasattr(self.client, 'disconnect'):
                await self.client.disconnect()
                logger.info(f"RemoteExecutionNode '{self.name}' disconnected.")
            elif hasattr(self.client, '__aexit__'):
                # For context manager clients like RunPod
                await self.client.__aexit__(None, None, None)
        await super().cleanup()

    async def process(self, data_stream: AsyncGenerator[Any, None]) -> AsyncGenerator[Any, None]:
        """
        Sends a stream of data to the remote service for execution and yields the results.
        """
        if not self.client:
            raise NodeError("Remote client not initialized.")

        # Check if we need to verify connection for gRPC clients
        if hasattr(self.client, 'stub') and not self.client.stub:
            raise NodeError("Remote client not connected.")

        logger.debug(f"RemoteExecutionNode '{self.name}': starting stream to remote for node '{self.node_to_execute}'")

        try:
            # Use appropriate streaming method based on client type
            if hasattr(self.client, 'stream_node'):
                # gRPC client
                async for result in self.client.stream_node(
                    node_type=self.node_to_execute,
                    config=self.node_config,
                    input_stream=data_stream,
                    serialization_format=self.serialization_format
                ):
                    yield result
            elif hasattr(self.client, 'stream_object'):
                # RunPod or other clients - use a mock object to represent the node type
                class NodeTypeWrapper:
                    def __init__(self, node_type: str, config: Dict[str, Any]):
                        self.node_type = node_type
                        self.config = config
                    
                    def process(self, input_stream):
                        # This won't actually be called - it's just for the interface
                        return input_stream
                
                wrapper = NodeTypeWrapper(self.node_to_execute, self.node_config)
                async for result in self.client.stream_object(
                    obj=wrapper,
                    config=self.node_config,
                    input_stream=data_stream
                ):
                    yield result
            else:
                raise NodeError(f"Client type {type(self.client)} does not support streaming")
        except Exception as e:
            logger.error(f"RemoteExecutionNode '{self.name}': Failed to stream remote node '{self.node_to_execute}'. Error: {e}")
            # The exception will be propagated by the pipeline
            raise

    def __repr__(self) -> str:
        """String representation of the node."""
        return f"{self.__class__.__name__}(name='{self.name}', target='{self.node_to_execute}')"


class RemoteObjectExecutionNode(Node):
    """
    A node that executes a cloudpickled Python object on a remote server.
    """
    def __init__(self, node_object: Any = None, obj_to_execute: Any = None, remote_config=None, node_config: Optional[Dict[str, Any]] = None, **kwargs):
        super().__init__(**kwargs)
        
        # Support both old and new parameter names for backward compatibility
        if node_object is not None and obj_to_execute is None:
            obj_to_execute = node_object
        elif obj_to_execute is None:
            raise ValueError("Either node_object or obj_to_execute must be provided")
        
        # For Node objects, we don't require initialize/cleanup methods
        if hasattr(obj_to_execute, '__class__') and hasattr(obj_to_execute.__class__, '__bases__'):
            # It's likely a Node subclass, check for process method
            if not hasattr(obj_to_execute, 'process'):
                raise ValueError("The object to execute must have a process method.")
        else:
            # For other objects, require the full interface
            if not all(hasattr(obj_to_execute, attr) for attr in ['initialize', 'process', 'cleanup']):
                raise ValueError("The object to execute must have initialize, process, and cleanup methods.")
            
        if not isinstance(remote_config, (RemoteExecutorConfig, RunPodPodRemoteExecutorConfig)):
            raise ValueError("remote_config must be a valid RemoteExecutorConfig or RunPodPodRemoteExecutorConfig instance.")

        self.obj_to_execute = obj_to_execute
        self.remote_config = remote_config
        self.node_config = node_config or {}
        self.client = None  # Will be set to appropriate client type
        self.session_id: Optional[str] = None
        self.is_streaming = getattr(self.obj_to_execute, 'is_streaming', False)
        self.uses_sessions = isinstance(remote_config, RemoteExecutorConfig)  # Only gRPC uses sessions

    async def initialize(self):
        """
        Initializes the remote object by sending it to the server, having it
        initialized there, and establishing a session (for gRPC) or preparing for use (for RunPod).
        """
        await super().initialize()
        self.client = create_remote_client(self.remote_config)

        if self.uses_sessions:
            # gRPC client with session support
            await self.client.connect()
            
            logger.info(f"Initializing remote object for node '{self.name}'...")
            response = await self.client.execute_object_method(
                obj=self.obj_to_execute,
                method_name='initialize',
                method_args=[]
            )
            self.session_id = response.get('session_id')
            if not self.session_id:
                raise NodeError("Failed to get a session ID for the remote object.")
            logger.info(f"Remote object for '{self.name}' initialized with session ID: {self.session_id}")
        else:
            # RunPod client - no session needed, just ensure client is ready
            logger.info(f"Remote object for '{self.name}' configured for {self.remote_config.provider} execution")
            
        await asyncio.sleep(0) # Allow context switching

    async def cleanup(self):
        """Disconnects the remote execution client and cleans up the remote session."""
        if self.client and self.uses_sessions and self.session_id:
            logger.info(f"Cleaning up remote session {self.session_id} for node '{self.name}'...")
            try:
                await self.client.execute_object_method(
                    obj=self.obj_to_execute, # obj is not used, but required by method
                    session_id=self.session_id,
                    method_name='cleanup',
                    method_args=[]
                )
            except Exception as e:
                logger.error(f"Failed to cleanly close remote session {self.session_id}: {e}")

        if self.client:
            if hasattr(self.client, 'disconnect'):
                await self.client.disconnect()
            elif hasattr(self.client, '__aexit__'):
                await self.client.__aexit__(None, None, None)
            self.client = None
        self.session_id = None
        await super().cleanup()

    async def process(self, data: Any) -> AsyncGenerator[Any, None]:
        """
        Processes data by streaming it to the remote object.
        """
        if not self.client:
            raise NodeError("Remote client not initialized.")
        
        if self.uses_sessions and not self.session_id:
            raise NodeError("Remote object session not initialized.")
        
        if self.is_streaming:
            try:
                if self.uses_sessions:
                    # gRPC client with session support
                    async for result in self.client.stream_object(
                        session_id=self.session_id,
                        config=self.node_config,
                        input_stream=data
                    ):
                        yield result
                else:
                    # RunPod client - direct object streaming
                    async with self.client as client:
                        async for result in client.stream_object(
                            obj=self.obj_to_execute,
                            config=self.node_config,
                            input_stream=data
                        ):
                            yield result
            except Exception as e:
                logger.error(f"Error streaming object remotely: {e}", exc_info=True)
                raise NodeError("Remote object stream failed") from e
        else:
            # Non-streaming case is not fully supported in this flow,
            # as it would require re-initializing for each call.
            raise NotImplementedError("Non-streaming remote object execution is not supported with this session-based approach.")

    def __repr__(self) -> str:
        """String representation of the node."""
        target_name = getattr(self.obj_to_execute, 'name', self.obj_to_execute.__class__.__name__)
        return f"{self.__class__.__name__}(name='{self.name}', target='{target_name}')"


__all__ = ["RemoteExecutionNode", "RemoteObjectExecutionNode"] 