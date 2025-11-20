"""
pdf_read_with_GPT: Hybrid PDF extraction with semantic enrichment

Stage 1: Local extraction (structure, hierarchy, tables, images)
Stage 2: OpenAI Vision API enrichment (semantic understanding)

Output: Enriched JSON with semantic annotations for graph generation
"""

from .stage1_extraction import Stage1Extractor
from .stage2_enrichment import Stage2Enrichment

__all__ = ['Stage1Extractor', 'Stage2Enrichment']
