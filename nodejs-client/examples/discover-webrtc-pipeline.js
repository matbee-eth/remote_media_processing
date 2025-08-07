#!/usr/bin/env node

/**
 * Example: Discover and Use WebRTC Pipeline
 * 
 * This example demonstrates how to:
 * 1. Connect to the RemoteMedia server
 * 2. Discover registered pipelines (including the WebRTC pipeline)
 * 3. Get detailed information about the WebRTC pipeline
 * 4. Execute the pipeline with sample data
 */

import { PipelineClient } from '../dist/src/pipeline-client.js';

async function discoverWebRTCsdPipeline() {
    console.log('🔍 Discovering WebRTC Pipeline...\n');
    
    // Connect to the pipeline service
    const client = new PipelineClient('localhost', 50052);
    
    try {
        // Connect to the service
        console.log('🔌 Connecting to pipeline service...');
        await client.connect();
        console.log('✅ Connected to pipeline service\n');
        
        // 1. List all available pipelines
        console.log('📋 Listing all registered pipelines:');
        const pipelines = await client.listPipelines();
        
        if (pipelines.length === 0) {
            console.log('   No pipelines found. Make sure the WebRTC server is running.');
            return;
        }
        
        pipelines.forEach((pipeline, i) => {
            console.log(`   ${i + 1}. ${pipeline.name} (${pipeline.id})`);
            console.log(`      Description: ${pipeline.description || 'No description'}`);
            console.log(`      Category: ${pipeline.metadata?.category || 'Unknown'}`);
            console.log(`      Tags: ${pipeline.metadata?.tags?.join(', ') || 'None'}`);
            console.log('');
        });
        
        // 2. Find the WebRTC pipeline
        const webrtcPipeline = pipelines.find(p => 
            p.name.includes('webrtc') || 
            p.metadata?.tags?.includes('webrtc')
        );
        
        if (!webrtcPipeline) {
            console.log('❌ WebRTC pipeline not found in registry');
            return;
        }
        
        console.log(`🎯 Found WebRTC Pipeline: ${webrtcPipeline.name}`);
        
        // 3. Get detailed information about the WebRTC pipeline
        console.log('\n📊 Getting detailed pipeline information:');
        const info = await client.getPipelineInfo(webrtcPipeline.id);
        
        console.log(`   Name: ${info.name}`);
        console.log(`   ID: ${info.id}`);
        console.log(`   Description: ${info.metadata?.description || 'No description'}`);
        console.log(`   Author: ${info.metadata?.author || 'Unknown'}`);
        console.log(`   Version: ${info.metadata?.version || 'Unknown'}`);
        console.log(`   Use Case: ${info.metadata?.use_case || 'Unknown'}`);
        console.log('');
        
        // Display pipeline structure
        console.log('🏗️  Pipeline Structure:');
        if (info.definition?.nodes) {
            console.log(`   Nodes (${info.definition.nodes.length}):`);
            info.definition.nodes.forEach((node, i) => {
                console.log(`      ${i + 1}. ${node.name || node.type} (${node.type})`);
                if (node.config && Object.keys(node.config).length > 0) {
                    console.log(`         Config: ${JSON.stringify(node.config, null, 0)}`);
                }
            });
        }
        console.log('');
        
        // Display features and capabilities
        if (info.metadata?.features) {
            console.log('✨ Features:');
            info.metadata.features.forEach(feature => {
                console.log(`   • ${feature}`);
            });
            console.log('');
        }
        
        if (info.metadata?.tools) {
            console.log('🛠️  Available Tools:');
            info.metadata.tools.forEach(tool => {
                console.log(`   • ${tool}`);
            });
            console.log('');
        }
        
        if (info.metadata?.requirements) {
            console.log('📦 Requirements:');
            info.metadata.requirements.forEach(req => {
                console.log(`   • ${req}`);
            });
            console.log('');
        }
        
        // 4. Example: Execute with sample audio data (mock)
        console.log('🎵 Example: Executing WebRTC pipeline with sample data...');
        
        try {
            // Create some mock audio data for demonstration
            const sampleAudioData = {
                samples: new Float32Array(16000), // 1 second of silence at 16kHz
                sampleRate: 16000,
                channels: 1,
                timestamp: Date.now(),
                metadata: {
                    source: 'nodejs-client-example',
                    format: 'pcm_f32le'
                }
            };
            
            // Fill with some sine wave data for testing
            for (let i = 0; i < sampleAudioData.samples.length; i++) {
                sampleAudioData.samples[i] = Math.sin(2 * Math.PI * 440 * i / 16000) * 0.1; // 440Hz sine wave
            }
            
            console.log('   Sample data prepared:');
            console.log(`   • Duration: ${sampleAudioData.samples.length / sampleAudioData.sampleRate}s`);
            console.log(`   • Sample rate: ${sampleAudioData.sampleRate}Hz`);
            console.log(`   • Channels: ${sampleAudioData.channels}`);
            console.log(`   • Format: ${sampleAudioData.metadata.format}`);
            
            // Note: In a real application, you might not want to execute this
            // as it requires the full ML pipeline to be running
            console.log('\n💡 To execute this pipeline:');
            console.log('   1. Ensure the remote execution service is running');
            console.log('   2. Make sure all ML models are loaded');
            console.log('   3. Use: await client.executePipeline(pipelineId, audioData)');
            
            // Uncomment to actually execute (requires full setup):
            // const result = await client.executePipeline(webrtcPipeline.id, sampleAudioData);
            // console.log('Result:', result);
            
        } catch (error) {
            console.log(`   ⚠️  Execution example skipped: ${error.message}`);
            console.log('   This is normal if the ML pipeline is not fully set up.');
        }
        
        // 5. Show how to clone the pipeline
        console.log('\n🔄 Pipeline Cloning Example:');
        console.log('   To create a customized version of this pipeline:');
        console.log(`   const clonedId = await client.clonePipeline('${webrtcPipeline.id}', 'my-user-id', {`);
        console.log('     newName: "my-custom-webrtc-pipeline",');
        console.log('     cloneNodes: true');
        console.log('   });');
        
        console.log('\n✅ WebRTC pipeline discovery complete!');
        console.log('\n🚀 Next steps:');
        console.log('   • Use this pipeline in your WebRTC applications');
        console.log('   • Clone and customize for your specific needs');
        console.log('   • Integrate with your JavaScript WebRTC client');
        console.log('   • Stream real audio data through the pipeline');
        
    } catch (error) {
        console.error('❌ Error discovering pipeline:', error.message);
        if (error.code === 'ECONNREFUSED') {
            console.log('\n💡 Make sure:');
            console.log('   1. The remote execution server is running (port 50052)');
            console.log('   2. The WebRTC pipeline server has been started');
            console.log('   3. Pipeline registration completed successfully');
        }
    }
}

// Run the discovery example
if (import.meta.url === `file://${process.argv[1]}`) {
    discoverWebRTCPipeline().catch(console.error);
}

export { discoverWebRTCPipeline };