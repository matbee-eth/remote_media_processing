#!/usr/bin/env python3
"""
Test Pipeline Export and Registration

This example demonstrates:
1. Creating a pipeline
2. Exporting the pipeline definition
3. Registering it with the gRPC service
4. Executing it remotely from JavaScript clients
"""

import sys
import os
import asyncio
import json
import logging

# Add the parent directory to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from remotemedia import Pipeline
from remotemedia.nodes import PassThroughNode, AudioTransform, VideoTransform, CalculatorNode, TextTransformNode
from remotemedia.nodes.io_nodes import DataSourceNode, DataSinkNode
from remotemedia.utils import setup_logging
from remotemedia.core.pipeline_registry import get_global_registry


async def create_and_export_basic_pipeline():
    """Create and export the basic pipeline from basic_pipeline.py."""
    print("\n=== Basic Pipeline Export ===\n")
    
    # Create the basic pipeline
    pipeline = Pipeline(name="BasicProcessingPipeline")
    
    # Add nodes
    pipeline.add_node(PassThroughNode(name="input"))
    pipeline.add_node(AudioTransform(sample_rate=44100, name="audio_proc"))
    pipeline.add_node(VideoTransform(resolution=(1920, 1080), name="video_proc"))
    pipeline.add_node(PassThroughNode(name="output"))
    
    # Export the pipeline definition
    definition = pipeline.export_definition()
    
    print(" Exported Pipeline Definition:")
    print(json.dumps(definition, indent=2))
    
    return pipeline, definition


async def create_calculator_pipeline():
    """Create a calculator pipeline with source and sink nodes."""
    print("\n=== Calculator Pipeline with Source/Sink ===\n")
    
    pipeline = Pipeline(name="CalculatorPipeline")
    
    # Add source for receiving data from JavaScript
    pipeline.add_node(DataSourceNode(
        buffer_size=100,
        timeout_seconds=30,
        name="js_input"
    ))
    
    # Add calculator node
    pipeline.add_node(CalculatorNode(
        name="calculator",
        verbose=True
    ))
    
    # Add sink for sending results to JavaScript
    pipeline.add_node(DataSinkNode(
        buffer_output=True,
        buffer_size=100,
        name="js_output"
    ))
    
    # Export definition
    definition = pipeline.export_definition()
    
    print(" Exported Calculator Pipeline:")
    print(f"  - Nodes: {len(definition['nodes'])}")
    print(f"  - Connections: {len(definition['connections'])}")
    print(f"  - Dependencies: {definition['dependencies']}")
    
    return pipeline, definition


async def create_text_processing_pipeline():
    """Create a text processing pipeline suitable for JavaScript integration."""
    print("\n=== Text Processing Pipeline ===\n")
    
    pipeline = Pipeline(name="TextProcessingPipeline")
    
    # Add nodes for text processing
    pipeline.add_node(DataSourceNode(name="text_input"))
    
    pipeline.add_node(PassThroughNode(name="text_processor_1"))
    
    pipeline.add_node(PassThroughNode(name="text_processor_2"))
    
    pipeline.add_node(DataSinkNode(
        buffer_output=True,
        name="text_output"
    ))
    
    # Export definition
    definition = pipeline.export_definition()
    
    print(" Exported Text Pipeline:")
    print(f"  - Pipeline: {definition['name']}")
    print(f"  - Nodes: {[n['node_type'] for n in definition['nodes']]}")
    print(f"  - Metadata: {definition['metadata']}")
    
    return pipeline, definition


async def test_pipeline_registry():
    """Test the pipeline registry functionality."""
    print("\n=== Testing Pipeline Registry ===\n")
    
    registry = get_global_registry()
    
    # Register all pipelines
    pipelines = [
        await create_and_export_basic_pipeline(),
        await create_calculator_pipeline(),
        await create_text_processing_pipeline()
    ]
    
    registered_ids = []
    
    for pipeline, definition in pipelines:
        # Register the pipeline
        pipeline_id = await registry.register_pipeline(
            name=definition['name'],
            definition=definition,
            metadata={
                "exported": "true",
                "source": "test_script"
            },
            dependencies=definition.get('dependencies', []),
            category="examples",
            description=f"Example {definition['name']} for testing"
        )
        
        registered_ids.append(pipeline_id)
        print(f" Registered: {definition['name']} -> {pipeline_id}")
    
    # List all registered pipelines
    print("\n Listing all registered pipelines:")
    all_pipelines = registry.list_pipelines(include_definitions=False)
    for p in all_pipelines:
        print(f"  - {p['name']} ({p['pipeline_id']})")
        print(f"    Category: {p['category']}")
        print(f"    Registered: {p['registered_timestamp']}")
    
    # Get detailed info for one pipeline
    if registered_ids:
        print(f"\n Getting details for {registered_ids[0]}:")
        info = registry.get_pipeline_info(
            registered_ids[0],
            include_definition=True,
            include_metrics=True
        )
        if info:
            print(f"  Name: {info['name']}")
            print(f"  Nodes: {len(info['definition']['nodes'])}")
            print(f"  Dependencies: {info['dependencies']}")
    
    # Test pipeline execution
    print("\nTesting pipeline execution:")
    calc_pipeline_id = registered_ids[1]  # Calculator pipeline
    
    test_input = {
        "operation": "add",
        "args": [10, 20, 30]
    }
    
    try:
        result = await registry.execute_pipeline(
            calc_pipeline_id,
            test_input
        )
        print(f"  Input: {test_input}")
        print(f"  Result: {result}")
    except Exception as e:
        print(f"  Execution error: {e}")
    
    # Test creating pipeline from definition
    print("\n Testing pipeline recreation from definition:")
    if all_pipelines:
        first_def = all_pipelines[0]['definition'] if 'definition' in all_pipelines[0] else None
        if not first_def:
            # Get the full definition
            info = registry.get_pipeline_info(
                all_pipelines[0]['pipeline_id'],
                include_definition=True
            )
            first_def = info['definition'] if info else None
        
        if first_def:
            recreated = await Pipeline.from_definition(first_def)
            print(f"   Successfully recreated pipeline: {recreated.name}")
            print(f"  Nodes: {[node.name for node in recreated.nodes]}")
    
    # Clean up
    print("\n Cleaning up registered pipelines:")
    for pipeline_id in registered_ids:
        success = await registry.unregister_pipeline(pipeline_id)
        print(f"  {'' if success else ''} Unregistered: {pipeline_id}")
    
    return True


async def demonstrate_nodejs_integration():
    """Show how Node.js clients would use these pipelines."""
    print("\n=== Node.js Integration Example ===\n")
    
    print(" JavaScript clients can now:")
    print("  1. Connect to the gRPC service at localhost:50052")
    print("  2. List available pipelines with ListPipelines()")
    print("  3. Execute pipelines with ExecutePipeline()")
    print("  4. Stream data with StreamPipeline()")
    print()
    print("Example JavaScript code:")
    print("""
    // Using the PipelineClient from nodejs-client
    const client = new PipelineClient('localhost', 50052);
    await client.connect();
    
    // Register a pipeline
    const pipelineId = await client.registerPipeline(
        'my_pipeline',
        pipelineDefinition
    );
    
    // Execute the pipeline
    const result = await client.executePipeline(
        pipelineId,
        inputData
    );
    
    // Stream data through pipeline
    const stream = client.streamPipeline(pipelineId);
    stream.on('data', (data) => console.log(data));
    await stream.send(myData);
    """)


async def main():
    """Main test function."""
    # Set up logging
    setup_logging(level="INFO")
    
    print("RemoteMedia Pipeline Export and Registration Test")
    print("=" * 60)
    
    try:
        # Test pipeline export
        await create_and_export_basic_pipeline()
        await create_calculator_pipeline()
        await create_text_processing_pipeline()
        
        # Test registry
        await test_pipeline_registry()
        
        # Show Node.js integration
        await demonstrate_nodejs_integration()
        
        print("\n All tests completed successfully!")
        print("\nPipelines are now exportable and can be registered for use by JavaScript clients.")
        print("The gRPC service at localhost:50052 will serve these pipelines to any connected client.")
        
    except Exception as e:
        print(f"\n Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)