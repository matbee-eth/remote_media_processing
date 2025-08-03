/**
 * Simple RemoteProxyClient Example
 * 
 * Demonstrates the Python-like API for remote object execution
 * that matches the Python RemoteProxyClient interface.
 */

import { RemoteProxyClient, RemoteExecutorConfig, withRemoteProxy } from '../../src/remote-proxy-client';

// Example matching the Python documentation
async function pythonStyleExample() {
  console.log('🐍 Python-Style API Example');
  console.log('===========================\n');
  
  // Python code that will run on the server
  const calculatorPythonCode = `
class Calculator:
    def add(self, a, b):
        return a + b
    
    def multiply(self, a, b):
        return a * b
`;

  // Define the TypeScript interface
  interface Calculator {
    add(a: number, b: number): number;
    multiply(a: number, b: number): number;
  }
  
  // JavaScript version (for type safety)
  class Calculator {
    add(a: number, b: number): number { return a + b; }
    multiply(a: number, b: number): number { return a * b; }
  }

  // Configure connection
  const config: RemoteExecutorConfig = {
    host: "localhost",
    port: 50052
  };

  // Method 1: Direct usage (similar to Python)
  console.log('Method 1: Direct usage');
  console.log('---------------------');
  
  const client = new RemoteProxyClient(config);
  await client.connect();
  
  const calculator = new Calculator();
  const remoteCalc = await client.createProxy(calculator, calculatorPythonCode);
  
  // Use it exactly like a local object (just add await)
  const result = await remoteCalc.add(5, 3);
  console.log(`5 + 3 = ${result}`);
  
  await client.close();

  // Method 2: Using async with pattern (like Python)
  console.log('\nMethod 2: Async with pattern');
  console.log('---------------------------');
  
  await withRemoteProxy(config, async (client) => {
    const remoteCalc = await client.createProxy(calculator, calculatorPythonCode);
    
    // Use it exactly like a local object (just add await)
    const sum = await remoteCalc.add(5, 3);
    console.log(`5 + 3 = ${sum}`);
    
    const product = await remoteCalc.multiply(4, 7);
    console.log(`4 * 7 = ${product}`);
  });
  // Client automatically closed here
}

// More complex example with state
async function statefulExample() {
  console.log('\n\n📊 Stateful Object Example');
  console.log('==========================\n');
  
  const counterPythonCode = `
class Counter:
    def __init__(self):
        self.count = 0
    
    def increment(self):
        self.count += 1
        return self.count
    
    def decrement(self):
        self.count -= 1
        return self.count
    
    def get_count(self):
        return self.count
    
    def reset(self):
        self.count = 0
        return self.count
`;

  interface Counter {
    increment(): number;
    decrement(): number;
    get_count(): number;
    reset(): number;
  }
  
  class Counter {
    increment(): number { return 0; }
    decrement(): number { return 0; }
    get_count(): number { return 0; }
    reset(): number { return 0; }
  }

  await withRemoteProxy({ host: "localhost", port: 50052 }, async (client) => {
    const counter = new Counter();
    const remote = await client.createProxy(counter, counterPythonCode);
    
    console.log('Initial count:', await remote.get_count());
    
    console.log('Incrementing 3 times...');
    for (let i = 0; i < 3; i++) {
      const count = await remote.increment();
      console.log(`  Count: ${count}`);
    }
    
    console.log('Decrementing once...');
    const afterDec = await remote.decrement();
    console.log(`  Count: ${afterDec}`);
    
    console.log('Final count:', await remote.get_count());
    
    console.log('Resetting...');
    await remote.reset();
    console.log('Count after reset:', await remote.get_count());
  });
}

// Data processing example
async function dataProcessingExample() {
  console.log('\n\n📈 Data Processing Example');
  console.log('=========================\n');
  
  const dataProcessorCode = `
import numpy as np

class DataProcessor:
    def mean(self, numbers):
        return np.mean(numbers)
    
    def std(self, numbers):
        return np.std(numbers)
    
    def normalize(self, numbers):
        arr = np.array(numbers)
        return ((arr - arr.mean()) / arr.std()).tolist()
    
    def stats(self, numbers):
        arr = np.array(numbers)
        return {
            'mean': float(arr.mean()),
            'std': float(arr.std()),
            'min': float(arr.min()),
            'max': float(arr.max()),
            'count': len(arr)
        }
`;

  interface DataProcessor {
    mean(numbers: number[]): number;
    std(numbers: number[]): number;
    normalize(numbers: number[]): number[];
    stats(numbers: number[]): {
      mean: number;
      std: number;
      min: number;
      max: number;
      count: number;
    };
  }
  
  class DataProcessor {
    mean(numbers: number[]): number { return 0; }
    std(numbers: number[]): number { return 0; }
    normalize(numbers: number[]): number[] { return []; }
    stats(numbers: number[]): any { return {}; }
  }

  const config: RemoteExecutorConfig = {
    host: "localhost",
    port: 50052
  };

  await withRemoteProxy(config, async (client) => {
    const processor = new DataProcessor();
    const remote = await client.createProxy(processor, dataProcessorCode, ['numpy']);
    
    const data = [10, 20, 30, 40, 50, 25, 35];
    
    console.log('Data:', data);
    console.log(`Mean: ${await remote.mean(data)}`);
    console.log(`Std Dev: ${await remote.std(data)}`);
    
    const normalized = await remote.normalize(data);
    console.log('Normalized:', normalized.map(n => n.toFixed(3)));
    
    const stats = await remote.stats(data);
    console.log('\nStatistics:');
    console.log(`  Mean: ${stats.mean}`);
    console.log(`  Std Dev: ${stats.std}`);
    console.log(`  Min: ${stats.min}`);
    console.log(`  Max: ${stats.max}`);
    console.log(`  Count: ${stats.count}`);
  });
}

// Main function
async function main() {
  console.log('🚀 RemoteProxyClient Examples (Python-like API)');
  console.log('==============================================\n');
  
  try {
    await pythonStyleExample();
    await statefulExample();
    await dataProcessingExample();
    
    console.log('\n✅ All examples completed successfully!');
  } catch (error) {
    console.error('Error:', error);
  }
}

// Run the examples
if (require.main === module) {
  main().catch(console.error);
}