"""
Pipeline Registry for managing pipeline definitions and instances.

This module provides a registry system for:
- Registering pipeline definitions
- Managing pipeline lifecycle
- Providing discovery interfaces
- Tracking pipeline metrics and usage
"""

import time
import asyncio
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from collections import defaultdict

from .pipeline import Pipeline
from .exceptions import PipelineError

logger = logging.getLogger(__name__)


@dataclass
class PipelineMetrics:
    """Metrics for pipeline execution."""
    total_executions: int = 0
    total_errors: int = 0
    total_execution_time_ms: float = 0.0
    last_execution_timestamp: Optional[float] = None
    
    @property
    def average_execution_time_ms(self) -> float:
        """Calculate average execution time."""
        if self.total_executions == 0:
            return 0.0
        return self.total_execution_time_ms / self.total_executions
    
    def record_execution(self, execution_time_ms: float, success: bool = True):
        """Record an execution."""
        self.total_executions += 1
        self.total_execution_time_ms += execution_time_ms
        if not success:
            self.total_errors += 1
        self.last_execution_timestamp = time.time()


@dataclass
class RegisteredPipeline:
    """Registered pipeline information."""
    pipeline_id: str
    name: str
    definition: Dict[str, Any]
    metadata: Dict[str, Any]
    registered_timestamp: float
    usage_count: int = 0
    category: str = "general"
    description: str = ""
    dependencies: List[str] = field(default_factory=list)
    metrics: PipelineMetrics = field(default_factory=PipelineMetrics)


class PipelineRegistry:
    """
    Registry for managing pipeline definitions and instances.
    
    This class provides centralized management of pipeline definitions,
    allowing pipelines to be registered, discovered, and executed remotely.
    """
    
    def __init__(self):
        """Initialize the pipeline registry."""
        self.pipelines: Dict[str, RegisteredPipeline] = {}
        self.pipeline_instances: Dict[str, Pipeline] = {}
        self.session_pipelines: Dict[str, str] = {}  # session_id -> pipeline_id
        self._cleanup_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def register_pipeline(
        self,
        name: str,
        definition: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
        dependencies: Optional[List[str]] = None,
        category: str = "general",
        description: str = ""
    ) -> str:
        """
        Register a pipeline definition.
        
        Args:
            name: Pipeline name
            definition: Pipeline definition dictionary
            metadata: Optional metadata
            dependencies: Optional list of dependencies
            category: Pipeline category for organization
            description: Human-readable description
            
        Returns:
            Unique pipeline ID
            
        Raises:
            PipelineError: If registration fails
        """
        async with self._lock:
            # Generate unique pipeline ID
            pipeline_id = f"pipeline_{name}_{int(time.time() * 1000)}"
            
            # Create registered pipeline entry
            registered = RegisteredPipeline(
                pipeline_id=pipeline_id,
                name=name,
                definition=definition,
                metadata=metadata or {},
                registered_timestamp=time.time(),
                category=category,
                description=description,
                dependencies=dependencies or []
            )
            
            self.pipelines[pipeline_id] = registered
            
            self.logger.info(f"Registered pipeline '{name}' with ID: {pipeline_id}")
            return pipeline_id
    
    async def unregister_pipeline(self, pipeline_id: str) -> bool:
        """
        Unregister a pipeline.
        
        Args:
            pipeline_id: Pipeline ID to unregister
            
        Returns:
            True if unregistered, False if not found
        """
        async with self._lock:
            if pipeline_id not in self.pipelines:
                return False
            
            # Clean up any instances
            if pipeline_id in self.pipeline_instances:
                instance = self.pipeline_instances[pipeline_id]
                if instance.is_initialized:
                    await instance.cleanup()
                del self.pipeline_instances[pipeline_id]
            
            # Remove from registry
            del self.pipelines[pipeline_id]
            
            self.logger.info(f"Unregistered pipeline: {pipeline_id}")
            return True
    
    async def get_pipeline_instance(
        self,
        pipeline_id: str,
        create_if_missing: bool = True
    ) -> Optional[Pipeline]:
        """
        Get or create a pipeline instance.
        
        Args:
            pipeline_id: Pipeline ID
            create_if_missing: Create instance if it doesn't exist
            
        Returns:
            Pipeline instance or None if not found
            
        Raises:
            PipelineError: If pipeline creation fails
        """
        async with self._lock:
            if pipeline_id not in self.pipelines:
                raise PipelineError(f"Pipeline not found: {pipeline_id}")
            
            # Return existing instance
            if pipeline_id in self.pipeline_instances:
                return self.pipeline_instances[pipeline_id]
            
            if not create_if_missing:
                return None
            
            # Create new instance from definition
            registered = self.pipelines[pipeline_id]
            try:
                pipeline = await Pipeline.from_definition(registered.definition)
                self.pipeline_instances[pipeline_id] = pipeline
                registered.usage_count += 1
                
                self.logger.info(f"Created pipeline instance: {pipeline_id}")
                return pipeline
                
            except Exception as e:
                raise PipelineError(f"Failed to create pipeline instance: {e}")
    
    def list_pipelines(
        self,
        category: Optional[str] = None,
        include_definitions: bool = False
    ) -> List[Dict[str, Any]]:
        """
        List registered pipelines.
        
        Args:
            category: Filter by category
            include_definitions: Include full definitions
            
        Returns:
            List of pipeline information dictionaries
        """
        pipelines = []
        
        for pipeline_id, registered in self.pipelines.items():
            # Filter by category if specified
            if category and registered.category != category:
                continue
            
            info = {
                "pipeline_id": pipeline_id,
                "name": registered.name,
                "category": registered.category,
                "description": registered.description,
                "registered_timestamp": registered.registered_timestamp,
                "usage_count": registered.usage_count,
                "metadata": registered.metadata
            }
            
            if include_definitions:
                info["definition"] = registered.definition
            
            pipelines.append(info)
        
        return pipelines
    
    def get_pipeline_info(
        self,
        pipeline_id: str,
        include_definition: bool = False,
        include_metrics: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Get detailed pipeline information.
        
        Args:
            pipeline_id: Pipeline ID
            include_definition: Include full definition
            include_metrics: Include execution metrics
            
        Returns:
            Pipeline information or None if not found
        """
        if pipeline_id not in self.pipelines:
            return None
        
        registered = self.pipelines[pipeline_id]
        
        info = {
            "pipeline_id": pipeline_id,
            "name": registered.name,
            "category": registered.category,
            "description": registered.description,
            "registered_timestamp": registered.registered_timestamp,
            "usage_count": registered.usage_count,
            "metadata": registered.metadata,
            "dependencies": registered.dependencies
        }
        
        if include_definition:
            info["definition"] = registered.definition
        
        if include_metrics:
            metrics = registered.metrics
            info["metrics"] = {
                "total_executions": metrics.total_executions,
                "total_errors": metrics.total_errors,
                "average_execution_time_ms": metrics.average_execution_time_ms,
                "last_execution_timestamp": metrics.last_execution_timestamp
            }
        
        return info
    
    async def execute_pipeline(
        self,
        pipeline_id: str,
        input_data: Any,
        runtime_config: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        Execute a registered pipeline.
        
        Args:
            pipeline_id: Pipeline ID to execute
            input_data: Input data for the pipeline
            runtime_config: Optional runtime configuration overrides
            
        Returns:
            Pipeline execution result
            
        Raises:
            PipelineError: If execution fails
        """
        if pipeline_id not in self.pipelines:
            raise PipelineError(f"Pipeline not found: {pipeline_id}")
        
        registered = self.pipelines[pipeline_id]
        start_time = time.time()
        
        try:
            # Get or create pipeline instance
            pipeline = await self.get_pipeline_instance(pipeline_id)
            
            # Apply runtime configuration if provided
            if runtime_config:
                # This would override specific node configurations
                # Implementation depends on specific requirements
                pass
            
            # Initialize pipeline if needed
            if not pipeline.is_initialized:
                await pipeline.initialize()
            
            # Execute pipeline
            # The pipeline.process() method expects an async iterable
            # Convert single input to async iterable
            async def single_item_generator():
                yield input_data
            
            # The pipeline.process() method always returns an async generator
            # We need to collect the results
            results = []
            async for item in pipeline.process(single_item_generator()):
                results.append(item)
            
            # For non-streaming pipelines, return the last result
            # For streaming pipelines, return all results
            has_streaming = any(getattr(node, 'is_streaming', False) for node in pipeline.nodes)
            if has_streaming:
                result = results  # Return all results for streaming
            else:
                result = results[-1] if results else None  # Return final result for non-streaming
            
            # Record metrics
            execution_time_ms = (time.time() - start_time) * 1000
            registered.metrics.record_execution(execution_time_ms, success=True)
            
            return result
            
        except Exception as e:
            # Record error metrics
            execution_time_ms = (time.time() - start_time) * 1000
            registered.metrics.record_execution(execution_time_ms, success=False)
            
            raise PipelineError(f"Pipeline execution failed: {e}")
    
    def create_streaming_session(
        self,
        pipeline_id: str,
        session_id: str
    ):
        """
        Create a streaming session for a pipeline.
        
        Args:
            pipeline_id: Pipeline ID
            session_id: Unique session ID
        """
        self.session_pipelines[session_id] = pipeline_id
        self.logger.info(f"Created streaming session {session_id} for pipeline {pipeline_id}")
    
    def get_session_pipeline(self, session_id: str) -> Optional[str]:
        """
        Get pipeline ID for a session.
        
        Args:
            session_id: Session ID
            
        Returns:
            Pipeline ID or None if not found
        """
        return self.session_pipelines.get(session_id)
    
    def close_streaming_session(self, session_id: str) -> bool:
        """
        Close a streaming session.
        
        Args:
            session_id: Session ID
            
        Returns:
            True if closed, False if not found
        """
        if session_id in self.session_pipelines:
            del self.session_pipelines[session_id]
            self.logger.info(f"Closed streaming session: {session_id}")
            return True
        return False
    
    async def cleanup_inactive_instances(self, max_age_seconds: int = 3600):
        """
        Clean up inactive pipeline instances.
        
        Args:
            max_age_seconds: Maximum age for inactive instances
        """
        current_time = time.time()
        instances_to_remove = []
        
        async with self._lock:
            for pipeline_id, registered in self.pipelines.items():
                if pipeline_id not in self.pipeline_instances:
                    continue
                
                # Check last execution time
                last_execution = registered.metrics.last_execution_timestamp
                if last_execution and (current_time - last_execution) > max_age_seconds:
                    instances_to_remove.append(pipeline_id)
            
            # Clean up inactive instances
            for pipeline_id in instances_to_remove:
                instance = self.pipeline_instances[pipeline_id]
                if instance.is_initialized:
                    await instance.cleanup()
                del self.pipeline_instances[pipeline_id]
                self.logger.info(f"Cleaned up inactive pipeline instance: {pipeline_id}")
    
    async def start_cleanup_task(self, interval_seconds: int = 600):
        """
        Start periodic cleanup task.
        
        Args:
            interval_seconds: Cleanup interval in seconds
        """
        async def cleanup_loop():
            while True:
                try:
                    await asyncio.sleep(interval_seconds)
                    await self.cleanup_inactive_instances()
                except Exception as e:
                    self.logger.error(f"Cleanup task error: {e}")
        
        if not self._cleanup_task or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(cleanup_loop())
            self.logger.info("Started pipeline cleanup task")
    
    async def stop_cleanup_task(self):
        """Stop the cleanup task."""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self.logger.info("Stopped pipeline cleanup task")
    
    async def shutdown(self):
        """Shutdown the registry and clean up all resources."""
        await self.stop_cleanup_task()
        
        # Clean up all pipeline instances
        async with self._lock:
            for pipeline_id, instance in self.pipeline_instances.items():
                if instance.is_initialized:
                    await instance.cleanup()
            
            self.pipeline_instances.clear()
            self.session_pipelines.clear()
        
        self.logger.info("Pipeline registry shutdown complete")


# Global registry instance
_global_registry: Optional[PipelineRegistry] = None


def get_global_registry() -> PipelineRegistry:
    """Get or create the global pipeline registry."""
    global _global_registry
    if _global_registry is None:
        _global_registry = PipelineRegistry()
    return _global_registry