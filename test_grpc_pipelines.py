#!/usr/bin/env python3
"""
Test pipeline registration through gRPC service directly.
"""

import asyncio
import grpc
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / 'remote_service' / 'src'))

import execution_pb2
import execution_pb2_grpc
import types_pb2

from remotemedia import Pipeline
from remotemedia.nodes import PassThroughNode, CalculatorNode
from remotemedia.nodes.io_nodes import DataSourceNode, DataSinkNode


async def test_grpc_pipeline_operations():
    """Test pipeline operations through gRPC."""
    print("🔌 Testing gRPC Pipeline Operations")
    
    # Create gRPC client
    channel = grpc.aio.insecure_channel('localhost:50052')
    client = execution_pb2_grpc.RemoteExecutionServiceStub(channel)
    
    try:
        # 1. Create a test pipeline (without source/sink for simpler testing)
        print("\n1️⃣ Creating test pipeline...")
        pipeline = Pipeline(name="GRPCTestPipeline")
        pipeline.add_node(PassThroughNode(name="input"))
        pipeline.add_node(CalculatorNode(name="calc", verbose=True))
        pipeline.add_node(PassThroughNode(name="output"))
        
        definition = pipeline.export_definition()
        print(f"   Pipeline has {len(definition['nodes'])} nodes")
        
        # 2. Register pipeline via gRPC
        print("\n2️⃣ Registering pipeline via gRPC...")
        
        # Convert to proto format
        proto_definition = execution_pb2.PipelineDefinition(
            name=definition['name'],
            config={k: str(v) for k, v in definition.get('config', {}).items()},
            metadata={k: str(v) for k, v in definition.get('metadata', {}).items()}
        )
        
        # Add nodes
        for node in definition['nodes']:
            node_def = proto_definition.nodes.add()
            node_def.node_id = node['node_id']
            node_def.node_type = node['node_type'] 
            for k, v in node.get('config', {}).items():
                node_def.config[k] = str(v)
            node_def.is_remote = node.get('is_remote', False)
            node_def.remote_endpoint = node.get('remote_endpoint') or ''
            node_def.is_streaming = node.get('is_streaming', False)
            node_def.is_source = node.get('is_source', False)
            node_def.is_sink = node.get('is_sink', False)
        
        # Add connections
        for conn in definition.get('connections', []):
            conn_def = proto_definition.connections.add()
            conn_def.from_node = conn['from_node']
            conn_def.to_node = conn['to_node']
            conn_def.output_port = conn.get('output_port', 'default')
            conn_def.input_port = conn.get('input_port', 'default')
        
        # Register request
        register_request = execution_pb2.RegisterPipelineRequest(
            pipeline_name="grpc_test_calculator",
            definition=proto_definition,
            auto_export=True
        )
        
        register_request.metadata["category"] = "test"
        register_request.metadata["description"] = "Test calculator pipeline via gRPC"
        register_request.dependencies.append("remotemedia")
        
        response = await client.RegisterPipeline(register_request)
        
        if response.status == types_pb2.EXECUTION_STATUS_SUCCESS:
            pipeline_id = response.pipeline_id
            print(f"   ✅ Pipeline registered: {pipeline_id}")
        else:
            print(f"   ❌ Registration failed: {response.error_message}")
            return
        
        # 3. List pipelines via gRPC
        print("\n3️⃣ Listing pipelines via gRPC...")
        list_request = execution_pb2.ListPipelinesRequest(include_definitions=False)
        list_response = await client.ListPipelines(list_request)
        
        print(f"   Found {len(list_response.pipelines)} pipeline(s):")
        for p in list_response.pipelines:
            print(f"     - {p.name} ({p.category}): {p.description}")
            print(f"       ID: {p.pipeline_id}")
        
        # 4. Get pipeline info
        print("\n4️⃣ Getting pipeline details...")
        info_request = execution_pb2.GetPipelineInfoRequest(
            pipeline_id=pipeline_id,
            include_definition=True,
            include_metrics=True
        )
        info_response = await client.GetPipelineInfo(info_request)
        
        if info_response.status == types_pb2.EXECUTION_STATUS_SUCCESS:
            info = info_response.pipeline_info
            print(f"   Name: {info.name}")
            print(f"   Nodes: {len(info.definition.nodes)}")
            print(f"   Connections: {len(info.definition.connections)}")
        
        # 5. Test execution
        print("\n5️⃣ Testing pipeline execution...")
        test_input = {
            "operation": "add", 
            "args": [10, 20, 5]
        }
        
        exec_request = execution_pb2.ExecutePipelineRequest(
            pipeline_id=pipeline_id,
            input_data=json.dumps(test_input).encode(),
            serialization_format="json"
        )
        
        exec_response = await client.ExecutePipeline(exec_request)
        
        if exec_response.status == types_pb2.EXECUTION_STATUS_SUCCESS:
            result = json.loads(exec_response.output_data.decode())
            print(f"   Input: {test_input}")
            print(f"   Result: {result}")
        else:
            print(f"   ❌ Execution failed: {exec_response.error_message}")
        
        # 6. Clean up
        print("\n6️⃣ Cleaning up...")
        unreg_request = execution_pb2.UnregisterPipelineRequest(pipeline_id=pipeline_id)
        unreg_response = await client.UnregisterPipeline(unreg_request)
        
        if unreg_response.status == types_pb2.EXECUTION_STATUS_SUCCESS:
            print("   ✅ Pipeline unregistered")
        
        print("\n✅ All gRPC pipeline operations completed successfully!")
        
    except Exception as e:
        print(f"❌ gRPC test failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await channel.close()


if __name__ == "__main__":
    asyncio.run(test_grpc_pipeline_operations())