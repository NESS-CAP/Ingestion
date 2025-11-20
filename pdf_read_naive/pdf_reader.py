"""
PDF text extraction utility.

Reads PDFs from pdf_read_naive/data/ directory and extracts all text.
"""

import os
import PyPDF2
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def extract_text_from_pdf(pdf_path: str) -> str:
    """
    Extract all text from a single PDF file.

    Args:
        pdf_path: Path to PDF file

    Returns:
        Extracted text from all pages
    """
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")

    text = ""
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            num_pages = len(reader.pages)

            logger.info(f"Extracting text from {os.path.basename(pdf_path)} ({num_pages} pages)...")

            for page_num, page in enumerate(reader.pages, 1):
                page_text = page.extract_text()
                if page_text:
                    text += f"\n--- Page {page_num} ---\n{page_text}"
                logger.debug(f"  Page {page_num}/{num_pages}")

        logger.info(f"✓ Extracted {len(text)} characters from {num_pages} pages")
        return text

    except Exception as e:
        logger.error(f"Error extracting text from {pdf_path}: {e}")
        raise


def extract_all_pdfs(pdf_dir: str = None) -> dict:
    """
    Extract text from all PDFs in a directory.

    Args:
        pdf_dir: Directory containing PDFs (defaults to pdf/data/)

    Returns:
        Dictionary mapping filenames to extracted text
    """
    if pdf_dir is None:
        # Get the pdf/data directory relative to this file
        pdf_dir = os.path.join(os.path.dirname(__file__), 'data')

    if not os.path.exists(pdf_dir):
        logger.warning(f"PDF directory not found: {pdf_dir}")
        return {}

    results = {}
    pdf_files = list(Path(pdf_dir).glob('*.pdf'))

    if not pdf_files:
        logger.warning(f"No PDF files found in {pdf_dir}")
        return results

    logger.info(f"Found {len(pdf_files)} PDF(s) in {pdf_dir}")
    logger.info("=" * 60)

    for pdf_file in sorted(pdf_files):
        try:
            text = extract_text_from_pdf(str(pdf_file))
            results[pdf_file.name] = text
        except Exception as e:
            logger.error(f"Failed to extract {pdf_file.name}: {e}")

    return results


def save_extracted_text(text: str, output_filename: str, output_dir: str = None) -> str:
    """
    Save extracted text to a file.

    Args:
        text: Extracted text to save
        output_filename: Name of output file
        output_dir: Output directory (defaults to ingestion/data/)

    Returns:
        Path to saved file
    """
    if output_dir is None:
        # Save to ingestion/data/ directory
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        output_dir = os.path.join(project_root, 'data')

    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, output_filename)

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(text)
        logger.info(f"✓ Saved to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Failed to save text: {e}")
        raise
