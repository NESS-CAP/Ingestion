"""
Stage 1: HTML Loading and Semantic Chunking

Loads HTML e-laws documents and chunks them using LangChain's semantic-aware
HTML header splitter to preserve hierarchical structure (regulation → division →
part → section → clause → subclause → item).
"""

import logging
from typing import Dict, List, Any
import json
from pathlib import Path
from langchain_community.document_loaders import BSHTMLLoader
from langchain_text_splitters import HTMLHeaderTextSplitter
import requests
from bs4 import BeautifulSoup
import sys

# Add root ingestion directory to path
sys.path.insert(0, str(Path(__file__).parents[3]))

from ingestion.shared.config.sources import ELAWS_OBC_HTML_URL
from ingestion.shared.config.settings import CHUNK_SIZE

logger = logging.getLogger(__name__)


class HTMLLoader:
    """Load HTML e-laws documents and perform semantic chunking"""

    def __init__(self, url: str = ELAWS_OBC_HTML_URL, chunk_size: int = CHUNK_SIZE):
        """
        Initialize HTML loader.

        Args:
            url: URL to e-laws HTML document
            chunk_size: Size of text chunks
        """
        self.url = url
        self.chunk_size = chunk_size
        self.documents = []

    def load_from_url(self) -> List[Dict[str, Any]]:
        """
        Load HTML from URL and return structured documents with chunks.

        Returns:
            List of documents with metadata and chunks
        """
        logger.info(f"Loading HTML from {self.url}")

        try:
            # Fetch HTML content
            response = requests.get(self.url, timeout=30)
            response.raise_for_status()

            # Save temporarily for processing
            temp_html_path = "/tmp/elaws_temp.html"
            with open(temp_html_path, "w", encoding="utf-8") as f:
                f.write(response.text)

            # Load using BeautifulSoup via LangChain
            loader = BSHTMLLoader(temp_html_path)
            docs = loader.load()

            logger.info(f"Loaded {len(docs)} documents from HTML")

            # Process and chunk documents
            chunked_docs = self._chunk_documents(docs)

            return chunked_docs

        except Exception as e:
            logger.error(f"Error loading HTML: {e}")
            raise

    def load_from_file(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Load HTML from local file.

        Args:
            file_path: Path to HTML file

        Returns:
            List of documents with metadata and chunks
        """
        logger.info(f"Loading HTML from {file_path}")

        try:
            loader = BSHTMLLoader(file_path)
            docs = loader.load()

            logger.info(f"Loaded {len(docs)} documents from {file_path}")

            # Process and chunk documents
            chunked_docs = self._chunk_documents(docs)

            return chunked_docs

        except Exception as e:
            logger.error(f"Error loading HTML file: {e}")
            raise

    def _chunk_documents(self, docs: List[Any]) -> List[Dict[str, Any]]:
        """
        Chunk documents preserving header hierarchy.

        Args:
            docs: LangChain documents loaded from HTML

        Returns:
            List of processed documents with chunks
        """
        logger.info(f"Chunking {len(docs)} documents")

        # Define header hierarchy to preserve semantic structure
        # Maps HTML headers to e-laws structural levels
        headers_to_split_on = [
            ("h1", "regulation"),      # O. Reg. 332/12
            ("h2", "division"),         # Division A, B, etc.
            ("h3", "part"),             # Part 3, 9, 11
            ("h4", "section"),          # 3.2.2
            ("h5", "subsection"),       # 3.2.2.1
            ("h6", "clause_group"),     # Groups of clauses
        ]

        # Create semantic splitter
        splitter = HTMLHeaderTextSplitter(
            headers_to_split_on=headers_to_split_on,
            return_each_line=False,
            auto_breaking_enabled=True
        )

        processed_docs = []

        for doc in docs:
            try:
                # Split document by headers
                header_splits = splitter.split_text(doc.page_content)

                logger.info(f"Split document into {len(header_splits)} header-aware chunks")

                # Further chunk if needed using recursive splitter
                final_chunks = self._recursive_chunk(header_splits)

                processed_docs.append({
                    "source": doc.metadata.get("source", "unknown"),
                    "title": doc.metadata.get("title", "Ontario Building Code"),
                    "chunks": final_chunks,
                    "total_chunks": len(final_chunks),
                    "metadata": doc.metadata
                })

            except Exception as e:
                logger.warning(f"Error processing document: {e}")
                continue

        logger.info(f"Processed {len(processed_docs)} documents with {sum(d['total_chunks'] for d in processed_docs)} total chunks")

        return processed_docs

    def _recursive_chunk(self, splits: List[Any]) -> List[Dict[str, Any]]:
        """
        Further chunk header-based splits using recursive character splitting
        for finer granularity.

        Args:
            splits: Header-aware splits from HTMLHeaderTextSplitter

        Returns:
            List of final chunks with metadata
        """
        from langchain_text_splitters import RecursiveCharacterTextSplitter

        # Create recursive splitter with small chunk size for fine granularity
        # This ensures we get clause-level chunks
        recursive_splitter = RecursiveCharacterTextSplitter(
            separators=["\n\n", "\n", "(", ")", " ", ""],
            chunk_size=200,  # Smaller chunks for clauses/subclauses
            chunk_overlap=30,
            length_function=len
        )

        chunks = []

        for split in splits:
            # Get metadata from header splits
            metadata = split.metadata if hasattr(split, 'metadata') else {}

            # If content is small enough, keep as-is
            if len(split.page_content) <= self.chunk_size:
                chunks.append({
                    "content": split.page_content,
                    "metadata": metadata,
                    "size": len(split.page_content),
                })
            else:
                # Recursively chunk larger content
                sub_chunks = recursive_splitter.split_text(split.page_content)
                for sub_chunk in sub_chunks:
                    chunks.append({
                        "content": sub_chunk,
                        "metadata": metadata,
                        "size": len(sub_chunk),
                    })

        return chunks

    def save_chunks(self, chunks: List[Dict[str, Any]], output_path: str):
        """
        Save chunked documents to JSON file.

        Args:
            chunks: Processed chunks with metadata
            output_path: Path to save JSON output
        """
        logger.info(f"Saving {len(chunks)} chunks to {output_path}")

        output = {
            "documents": chunks,
            "total_documents": len(chunks),
            "total_chunks": sum(d.get("total_chunks", 0) for d in chunks),
            "source_url": self.url
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        logger.info(f"Chunks saved to {output_path}")


def main():
    """Load and chunk e-laws HTML"""
    logging.basicConfig(level=logging.INFO)

    loader = HTMLLoader()

    # Load from URL
    try:
        chunks = loader.load_from_url()
        loader.save_chunks(chunks, "data/html_chunks.json")
        logger.info(f"Successfully loaded and chunked HTML from {ELAWS_OBC_HTML_URL}")
    except Exception as e:
        logger.error(f"Failed to load from URL: {e}")
        logger.info("Falling back to local file if available")


if __name__ == "__main__":
    main()
