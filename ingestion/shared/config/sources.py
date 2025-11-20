"""
Configuration for e-laws source URLs.

This module stores the URLs for official e-laws documents, particularly
the Ontario Building Code (OBC) regulations that are used for ingestion.
"""

# Ontario Building Code - O. Reg. 332/12
# Official HTML version from Ontario e-laws portal
ELAWS_OBC_HTML_URL = "https://www.ontario.ca/laws/regulation/120332"

# Alternative formats or related regulations can be added here
ELAWS_SOURCES = {
    "obc": {
        "name": "Ontario Building Code - O. Reg. 332/12",
        "url": ELAWS_OBC_HTML_URL,
        "description": "Official regulation for building code in Ontario",
    },
    # Add more e-laws sources as needed
    # "other_regulation": {
    #     "name": "Regulation Name",
    #     "url": "https://www.ontario.ca/laws/regulation/...",
    #     "description": "Description",
    # },
}
