"""
Parser registry for managing BI tool parsers.
"""
from typing import Dict, Type, Optional
import logging

from .base_parser import BaseParser


logger = logging.getLogger(__name__)


class ParserRegistry:
    """
    Registry for managing BI tool parsers.
    
    Provides a pluggable architecture where new BI tool parsers
    can be registered and retrieved dynamically.
    """
    
    _parsers: Dict[str, Type[BaseParser]] = {}
    
    @classmethod
    def register(cls, tool_name: str, parser_class: Type[BaseParser]) -> None:
        """
        Register a parser for a BI tool.
        
        Args:
            tool_name: Name of the BI tool (e.g., "cognos", "tableau")
            parser_class: Parser class extending BaseParser
        
        Raises:
            ValueError: If parser_class doesn't extend BaseParser
        """
        if not issubclass(parser_class, BaseParser):
            raise ValueError(
                f"Parser class must extend BaseParser, got {parser_class}"
            )
        
        cls._parsers[tool_name.lower()] = parser_class
        logger.info(f"Registered parser for '{tool_name}'")
    
    @classmethod
    def get_parser(cls, tool_name: str, config: dict = None) -> BaseParser:
        """
        Get a parser instance for a BI tool.
        
        Args:
            tool_name: Name of the BI tool
            config: Optional configuration for the parser
        
        Returns:
            Parser instance
        
        Raises:
            ValueError: If no parser registered for tool_name
        """
        parser_class = cls._parsers.get(tool_name.lower())
        
        if not parser_class:
            available = ", ".join(cls._parsers.keys())
            raise ValueError(
                f"No parser registered for '{tool_name}'. "
                f"Available parsers: {available}"
            )
        
        return parser_class(config=config)
    
    @classmethod
    def list_parsers(cls) -> list[str]:
        """
        List all registered BI tool parsers.
        
        Returns:
            List of registered tool names
        """
        return list(cls._parsers.keys())
    
    @classmethod
    def is_supported(cls, tool_name: str) -> bool:
        """
        Check if a BI tool is supported.
        
        Args:
            tool_name: Name of the BI tool
        
        Returns:
            True if supported, False otherwise
        """
        return tool_name.lower() in cls._parsers


def create_parser(tool_name: str, config: dict = None) -> BaseParser:
    """
    Factory function to create a parser instance.
    
    Args:
        tool_name: Name of the BI tool (e.g., "cognos", "tableau")
        config: Optional configuration for the parser
    
    Returns:
        Parser instance
    
    Raises:
        ValueError: If no parser registered for tool_name
    """
    return ParserRegistry.get_parser(tool_name, config)
