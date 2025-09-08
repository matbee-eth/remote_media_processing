/**
 * Calculator Proxy Example
 * 
 * Demonstrates using RemoteProxyClient with a Python-like API
 * to execute a calculator remotely.
 */

import { RemoteProxyClient, RemoteExecutorConfig, withRemoteProxy } from '../../src/remote-proxy-client';

// Define our calculator interface
interface Calculator {
  add(a: number, b: number): number;
  subtract(a: number, b: number): number;
  multiply(a: number, b: number): number;
  divide(a: number, b: number): number;
  power(base: number, exponent: number): number;
}

// JavaScript implementation (for reference)
class LocalCalculator implements Calculator {
  add(a: number, b: number): number {
    return a + b;
  }
  
  subtract(a: number, b: number): number {
    return a - b;
  }
  
  multiply(a: number, b: number): number {
    return a * b;
  }
  
  divide(a: number, b: number): number {
    if (b === 0) throw new Error("Division by zero");
    return a / b;
  }
  
  power(base: number, exponent: number): number {
    return Math.pow(base, exponent);
  }
}

// Python implementation that will run remotely
const pythonCalculatorCode = `
class LocalCalculator:
    def __init__(self):
        self.name = "Python Calculator"
    
    def add(self, a, b):
        return a + b
    
    def subtract(self, a, b):
        return a - b
    
    def multiply(self, a, b):
        return a * b
    
    def divide(self, a, b):
        if b == 0:
            raise ValueError("Division by zero")
        return a / b
    
    def power(self, base, exponent):
        return base ** exponent
`;

async function example1_directUsage() {
  console.log('📐 Example 1: Direct Usage');
  console.log('========================\n');
  
  // Configure connection
  const config: RemoteExecutorConfig = {
    host: "localhost",
    port: 50052
  };
  
  // Create client and connect
  const client = new RemoteProxyClient(config);
  await client.connect();
  
  // Create a local calculator instance
  const calculator = new LocalCalculator();
  
  // Create remote proxy with Python implementation
  const remoteCalc = await client.createProxy(calculator, pythonCalculatorCode);
  
  // Use it exactly like a local object (just add await)
  console.log('Testing basic operations:');
  const sum = await remoteCalc.add(5, 3);
  console.log(`5 + 3 = ${sum}`);
  
  const difference = await remoteCalc.subtract(10, 4);
  console.log(`10 - 4 = ${difference}`);
  
  const product = await remoteCalc.multiply(6, 7);
  console.log(`6 * 7 = ${product}`);
  
  const quotient = await remoteCalc.divide(20, 4);
  console.log(`20 / 4 = ${quotient}`);
  
  const power = await remoteCalc.power(2, 8);
  console.log(`2 ^ 8 = ${power}`);
  
  // Clean up
  await client.close();
}

async function example2_withPattern() {
  console.log('\n📐 Example 2: Using "with" Pattern');
  console.log('==================================\n');
  
  const config: RemoteExecutorConfig = {
    host: "localhost",
    port: 50052
  };
  
  // Use the Python-style "async with" pattern
  await withRemoteProxy(config, async (client) => {
    const calculator = new LocalCalculator();
    const remoteCalc = await client.createProxy(calculator, pythonCalculatorCode);
    
    // Perform calculations
    console.log('Complex calculation: (10 + 5) * 2 - 8 / 4');
    
    const step1 = await remoteCalc.add(10, 5);
    console.log(`Step 1: 10 + 5 = ${step1}`);
    
    const step2 = await remoteCalc.multiply(step1, 2);
    console.log(`Step 2: ${step1} * 2 = ${step2}`);
    
    const step3 = await remoteCalc.divide(8, 4);
    console.log(`Step 3: 8 / 4 = ${step3}`);
    
    const result = await remoteCalc.subtract(step2, step3);
    console.log(`Final: ${step2} - ${step3} = ${result}`);
  });
  // Client is automatically closed here
}

async function example3_scientificCalculator() {
  console.log('\n📐 Example 3: Scientific Calculator');
  console.log('===================================\n');
  
  // Python code with numpy dependency
  const scientificCalculatorCode = `
import numpy as np

class ScientificCalculator:
    def __init__(self):
        self.name = "Scientific Calculator"
    
    def sin(self, angle):
        return np.sin(np.radians(angle))
    
    def cos(self, angle):
        return np.cos(np.radians(angle))
    
    def tan(self, angle):
        return np.tan(np.radians(angle))
    
    def sqrt(self, number):
        return np.sqrt(number)
    
    def log(self, number, base=np.e):
        return np.log(number) / np.log(base)
    
    def factorial(self, n):
        if n < 0:
            raise ValueError("Factorial not defined for negative numbers")
        result = 1
        for i in range(1, n + 1):
            result *= i
        return result
`;

  interface ScientificCalculator {
    sin(angle: number): number;
    cos(angle: number): number;
    tan(angle: number): number;
    sqrt(number: number): number;
    log(number: number, base?: number): number;
    factorial(n: number): number;
  }
  
  class LocalScientificCalculator implements ScientificCalculator {
    sin(angle: number): number { return Math.sin(angle * Math.PI / 180); }
    cos(angle: number): number { return Math.cos(angle * Math.PI / 180); }
    tan(angle: number): number { return Math.tan(angle * Math.PI / 180); }
    sqrt(number: number): number { return Math.sqrt(number); }
    log(number: number, base: number = Math.E): number { return Math.log(number) / Math.log(base); }
    factorial(n: number): number {
      if (n < 0) throw new Error("Factorial not defined for negative numbers");
      let result = 1;
      for (let i = 1; i <= n; i++) result *= i;
      return result;
    }
  }
  
  await withRemoteProxy({ host: "localhost", port: 50052 }, async (client) => {
    const sciCalc = new LocalScientificCalculator();
    const remote = await client.createProxy(sciCalc, scientificCalculatorCode, ['numpy']);
    
    console.log('Trigonometric functions:');
    console.log(`sin(30°) = ${await remote.sin(30)}`);
    console.log(`cos(60°) = ${await remote.cos(60)}`);
    console.log(`tan(45°) = ${await remote.tan(45)}`);
    
    console.log('\nOther functions:');
    console.log(`sqrt(16) = ${await remote.sqrt(16)}`);
    console.log(`log(100, 10) = ${await remote.log(100, 10)}`);
    console.log(`5! = ${await remote.factorial(5)}`);
  });
}

async function example4_errorHandling() {
  console.log('\n📐 Example 4: Error Handling');
  console.log('============================\n');
  
  await withRemoteProxy({ host: "localhost", port: 50052 }, async (client) => {
    const calculator = new LocalCalculator();
    const remote = await client.createProxy(calculator, pythonCalculatorCode);
    
    try {
      console.log('Attempting division by zero...');
      await remote.divide(10, 0);
    } catch (error: any) {
      console.log(`✅ Error caught: ${error.message}`);
    }
    
    try {
      console.log('\nAttempting invalid operation...');
      // This will fail because 'invalidMethod' doesn't exist
      await (remote as any).invalidMethod(1, 2);
    } catch (error: any) {
      console.log(`✅ Error caught: ${error.message}`);
    }
  });
}

// Main function
async function main() {
  console.log('🤖 RemoteProxyClient Calculator Examples');
  console.log('=======================================\n');
  console.log('This example demonstrates a Python-like API for remote execution.\n');
  
  try {
    await example1_directUsage();
    await example2_withPattern();
    await example3_scientificCalculator();
    await example4_errorHandling();
    
    console.log('\n✅ All examples completed successfully!');
  } catch (error) {
    console.error('Fatal error:', error);
  }
}

// Run the examples
if (require.main === module) {
  main().catch(console.error);
}