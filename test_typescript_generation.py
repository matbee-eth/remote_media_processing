#!/usr/bin/env python3
"""
Test TypeScript interface generation locally without needing the server running.
"""

import sys
import os
import asyncio

# Add the remote service src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "remote_service", "src"))


async def test_typescript_generation():
    """Test the TypeScript interface generation."""
    
    # Import here to avoid issues
    from server import RemoteExecutionServicer
    from config import ServiceConfig
    
    # Create a servicer instance within async context
    config = ServiceConfig()
    servicer = RemoteExecutionServicer(config)
    
    # Generate TypeScript definitions
    typescript_defs = servicer._generate_typescript_interfaces()
    
    # Print the generated definitions
    print("Generated TypeScript Definitions:")
    print("=" * 80)
    print(typescript_defs)
    print("=" * 80)
    
    # Save to file
    output_file = "remotemedia-types.d.ts"
    with open(output_file, 'w') as f:
        f.write(typescript_defs)
    
    print(f"\n✅ TypeScript definitions saved to: {output_file}")
    
    # Verify the content
    required_interfaces = [
        "RemoteMediaNode",
        "RemoteExecutorConfig",
        "SessionState",
        "ExecutionOptions",
        "RemoteExecutionClient",
        "RemoteProxyClient",
        "NodeType",
        "AudioTransformConfig"
    ]
    
    print("\n📋 Verification:")
    for interface in required_interfaces:
        if f"export interface {interface}" in typescript_defs or f"export enum {interface}" in typescript_defs:
            print(f"  ✅ {interface} found")
        else:
            print(f"  ❌ {interface} missing")
    
    # Clean up the servicer
    await servicer._cleanup_connection_resources("test")
    if servicer._cleanup_task:
        servicer._cleanup_task.cancel()
        try:
            await servicer._cleanup_task
        except asyncio.CancelledError:
            pass
    
    return True


if __name__ == "__main__":
    try:
        asyncio.run(test_typescript_generation())
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)