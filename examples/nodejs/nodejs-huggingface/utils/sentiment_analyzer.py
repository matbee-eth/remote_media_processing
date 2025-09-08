
import asyncio
from typing import List, Dict, Any

class HuggingFaceSentimentAnalyzer:
    def __init__(self, model_name: str = "distilbert-base-uncased-finetuned-sst-2-english"):
        self.model_name = model_name
        self.pipeline = None
        
    async def initialize(self):
        """Load the Hugging Face pipeline."""
        print(f"Loading model: {self.model_name}")
        from transformers import pipeline
        import torch
        
        # Determine device
        if torch.cuda.is_available():
            device = "cuda:0"
        elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            device = "mps"
        else:
            device = "cpu"
            
        print(f"Using device: {device}")
        
        # Load pipeline (this may download the model on first run)
        self.pipeline = await asyncio.to_thread(
            pipeline,
            task="sentiment-analysis",
            model=self.model_name,
            device=device
        )
        print("Model loaded successfully!")
        
    async def analyze(self, text: str) -> Dict[str, Any]:
        """Analyze sentiment of a single text."""
        if not self.pipeline:
            await self.initialize()
            
        # Run inference
        result = await asyncio.to_thread(self.pipeline, text)
        return result[0]  # Return first result
        
    async def analyze_batch(self, texts: List[str]) -> List[Dict[str, Any]]:
        """Analyze sentiment of multiple texts."""
        if not self.pipeline:
            await self.initialize()
            
        # Run batch inference
        results = await asyncio.to_thread(self.pipeline, texts)
        return results
        
    def cleanup(self):
        """Clean up resources."""
        if hasattr(self, 'pipeline') and self.pipeline is not None:
            del self.pipeline
            self.pipeline = None
            
            # Clear CUDA cache if using GPU
            try:
                import torch
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
            except:
                pass

# Create the analyzer instance
analyzer = HuggingFaceSentimentAnalyzer()
