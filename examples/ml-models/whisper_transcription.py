#!/usr/bin/env python3
"""
Example of using the WhisperTranscriptionNode for real-time audio transcription.
"""

import asyncio
import logging
import os

# Ensure the 'remotemedia' package is in the Python path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from remotemedia.core.pipeline import Pipeline
from remotemedia.nodes.source import MediaReaderNode, AudioTrackSource
from remotemedia.nodes.audio import AudioTransform
from remotemedia.nodes.ml import WhisperTranscriptionNode
from remotemedia.nodes.ml.whisper_transcription import TranscriptionDelta, WordTiming, WordUpdate
from remotemedia.nodes import PassThroughNode

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class SimpleTranscriptionDisplayNode(PassThroughNode):
    """A node that shows real-time transcription updates with corrections."""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.words = {}  # timestamp -> WordUpdate (to handle corrections)
        self.last_update_time = 0
        self.update_count = 0

    async def process(self, data):
        if isinstance(data, WordUpdate):
            # Store word by timestamp (rounded to handle corrections)
            time_key = round(data.start, 1)
            
            # Check if this is a correction
            is_correction = time_key in self.words and self.words[time_key].word != data.word
            
            # Update our word storage
            self.words[time_key] = data
            
            # Build current transcription from all stored words
            current_text = self._build_current_transcription()
            
            # Show updates periodically or on corrections
            should_update = (
                is_correction or
                self.update_count % 5 == 0 or  # Every 5 words
                data.end - self.last_update_time > 2.0  # Every 2 seconds
            )
            
            if should_update:
                if is_correction:
                    print(f"\n[{data.end:.1f}s] CORRECTED: {current_text}")
                else:
                    print(f"\n[{data.end:.1f}s] {current_text}")
                self.last_update_time = data.end
                
            self.update_count += 1
                
        yield data
    
    async def cleanup(self):
        """Log the final transcription when the stream ends."""
        if self.words:
            final_text = self._build_current_transcription()
            print(f"\n{'='*80}")
            print(f"FINAL TRANSCRIPTION ({len(self.words)} words):")
            print(f"{'='*80}")
            print(final_text)
            print(f"{'='*80}")
    
    def _build_current_transcription(self):
        """Build transcription from all stored words in chronological order."""
        sorted_words = sorted(self.words.values(), key=lambda w: w.start)
        return " ".join(word.word for word in sorted_words)




async def main():
    """
    Main function to set up and run the transcription pipeline.
    """
    # 1. Use existing audio file
    dummy_audio_path = "../media-files/harvard.wav"
    
    if not os.path.exists(dummy_audio_path):
        logging.error(f"Audio file not found: {dummy_audio_path}")
        return

    # 2. Create and configure the pipeline
    pipeline = Pipeline()

    # The MediaReaderNode will provide the initial stream of audio chunks
    # Use real-time playback speed to simulate streaming audio input
    pipeline.add_node(MediaReaderNode(path=dummy_audio_path, chunk_size=4096, real_time=True))
    # Convert av.AudioFrame objects into (ndarray, sample_rate) tuples
    pipeline.add_node(AudioTrackSource())
    # Whisper expects 16kHz audio, so we resample it.
    pipeline.add_node(AudioTransform(output_sample_rate=16000, output_channels=1))

    # Add the Whisper node with delta transcription settings
    # Start with small buffer (3s), grow to 15s, with 1.5s overlap for context
    pipeline.add_node(WhisperTranscriptionNode(
        initial_buffer_duration_s=3,
        max_buffer_duration_s=12,
        buffer_growth_factor=1.4,
        overlap_duration_s=1.0
    ))

    # Add a node to show real-time transcription updates
    pipeline.add_node(SimpleTranscriptionDisplayNode())

    # 3. Run the pipeline
    logging.info("Starting transcription pipeline...")
    async with pipeline.managed_execution():
        async for word_update in pipeline.process():
            # The pipeline runs as we consume its output stream.
            # Each word_update is a WordUpdate object processed by StreamingTranscriptionDisplayNode
            pass

    logging.info("Transcription pipeline finished.")


if __name__ == "__main__":
    # Note: The first time you run this, it will download the Whisper model,
    # which can be several gigabytes.
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"An error occurred: {e}") 