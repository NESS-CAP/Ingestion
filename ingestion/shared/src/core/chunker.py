from typing import List, Dict
from ingestion.shared.config.settings import CHUNK_SIZE, CHUNK_OVERLAP
import hashlib

class DocumentChunker:
    """Splits documents into overlapping chunks"""

    def __init__(self, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP):
        self.chunk_size = chunk_size
        self.overlap = overlap

    def chunk_text(self, text: str, metadata: Dict = None) -> List[Dict]:
        """
        Split text into overlapping chunks.

        Args:
            text: The text to chunk
            metadata: Optional metadata to attach to each chunk

        Returns:
            List of chunk dictionaries with id, text, position, and metadata
        """
        chunks = []
        start = 0

        while start < len(text):
            end = min(start + self.chunk_size, len(text))
            chunk_text = text[start:end]

            # Generate unique chunk ID based on content hash
            chunk_id = hashlib.md5(chunk_text.encode()).hexdigest()

            chunks.append({
                "id": chunk_id,
                "text": chunk_text,
                "start": start,
                "end": end,
                "metadata": metadata or {}
            })

            # If we've reached the end, break to avoid infinite loop
            if end == len(text):
                break

            # Move start position: go back by overlap amount
            start = end - self.overlap
            # Ensure we make forward progress
            if start <= 0:
                start = end

        return chunks
