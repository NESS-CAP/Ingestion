#!/usr/bin/env python3
"""
PDF Text Extraction Script

Extracts text from all PDFs in pdf_read_naive/data/ directory and saves as text files
to the main ingestion data/ folder.

Usage:
    python3 pdf_read_naive/extract_pdfs.py
"""

import sys
import os
import logging
from pathlib import Path

# Add parent directory to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from pdf_read_naive.pdf_reader import extract_all_pdfs, save_extracted_text

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Extract all PDFs and save as text files"""
    logger.info("=" * 60)
    logger.info("PDF Text Extraction")
    logger.info("=" * 60)

    # Get pdf/data directory
    pdf_data_dir = os.path.join(os.path.dirname(__file__), 'data')

    logger.info(f"\nSearching for PDFs in: {pdf_data_dir}\n")

    # Extract all PDFs
    extracted = extract_all_pdfs(pdf_data_dir)

    if not extracted:
        logger.warning("No PDFs found to extract")
        logger.info("\nTo use this script:")
        logger.info(f"1. Place PDF files in: {pdf_data_dir}")
        logger.info("2. Run: python3 pdf/extract_pdfs.py")
        return 1

    # Save extracted text files
    logger.info("\n" + "=" * 60)
    logger.info("Saving Extracted Text")
    logger.info("=" * 60 + "\n")

    saved_files = []
    for pdf_name, text in extracted.items():
        # Create output filename from PDF name
        text_filename = pdf_name.replace('.pdf', '.txt')

        # Save the extracted text
        output_path = save_extracted_text(text, text_filename)
        saved_files.append(output_path)

        # Also print summary
        logger.info(f"  Characters extracted: {len(text)}")
        logger.info(f"  Output: {text_filename}\n")

    # Summary
    logger.info("=" * 60)
    logger.info("Extraction Complete")
    logger.info("=" * 60)
    logger.info(f"Processed: {len(extracted)} PDF(s)")
    logger.info(f"Saved: {len(saved_files)} text file(s)")
    logger.info(f"\nText files saved to: {os.path.join(project_root, 'data')}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
