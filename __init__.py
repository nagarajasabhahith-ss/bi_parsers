"""
BI Parsers Library - Pluggable parsers for multiple BI tools.

This library provides a pluggable architecture for parsing exports
from various BI tools (Cognos, Tableau, Power BI, etc.).
"""

from .core import (
    BaseParser,
    BaseExtractor,
    ParserRegistry,
    create_parser,
    ExtractedObject,
    Relationship,
    ParseError,
    ParseResult,
    ObjectType,
    RelationshipType,
    ParseErrorLevel,
)

# Import and register parsers
from .cognos import CognosParser

# Register Cognos parser
ParserRegistry.register("cognos", CognosParser)

__version__ = "0.1.0"

__all__ = [
    "BaseParser",
    "BaseExtractor",
    "ParserRegistry",
    "create_parser",
    "ExtractedObject",
    "Relationship",
    "ParseError",
    "ParseResult",
    "ObjectType",
    "RelationshipType",
    "ParseErrorLevel",
    "CognosParser",
]
