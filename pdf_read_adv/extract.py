#!/usr/bin/env python3
"""
Simple PDF extraction script.

Reads PDFs from ./data/ and saves results to ../data/

Usage:
    python pdf_read_adv/extract.py

    OR from pdf_read_adv/:
    python extract.py
"""

import sys
import os
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from pdf_read_adv import OBCStructuredReader

def main():
    """Extract all PDFs"""

    # Get directories
    script_dir = os.path.dirname(__file__)
    input_dir = os.path.join(script_dir, "data")
    output_dir = os.path.join(os.path.dirname(script_dir), "data")

    # Create output dir
    os.makedirs(output_dir, exist_ok=True)

    # Find PDFs
    pdf_files = list(Path(input_dir).glob("*.pdf"))

    if not pdf_files:
        print(f"❌ No PDFs found in {input_dir}")
        return 1

    print(f"✓ Found {len(pdf_files)} PDF(s)")
    print()

    for pdf_file in sorted(pdf_files):
        pdf_name = pdf_file.name
        print(f"Processing: {pdf_name}")

        try:
            # Read PDF
            reader = OBCStructuredReader(str(pdf_file))

            # Extract
            data = reader.read()

            # Save JSON
            json_file = pdf_name.replace(".pdf", "_structure.json")
            json_path = os.path.join(output_dir, json_file)
            reader.export_json(json_path)
            print(f"  ✓ Saved: {json_file}")

            # Extract images
            image_dir = pdf_name.replace(".pdf", "_images")
            images = reader.extract_images(os.path.join(output_dir, image_dir))

            if images:
                print(f"  ✓ Extracted {len(images)} images")

            print()

        except Exception as e:
            print(f"  ❌ Error: {e}")
            print()

    print("✅ Done!")
    print(f"Results in: {output_dir}/")
    return 0

if __name__ == "__main__":
    sys.exit(main())
