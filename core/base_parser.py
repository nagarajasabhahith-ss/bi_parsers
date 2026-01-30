"""
Abstract base parser class for BI tool parsers.
"""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Union
import logging

from .models import ParseResult


logger = logging.getLogger(__name__)


class BaseParser(ABC):
    """
    Abstract base class for all BI tool parsers.
    
    Each BI tool (Cognos, Tableau, Power BI, etc.) should implement
    this interface to provide consistent parsing functionality.
    """
    
    def __init__(self, config: dict = None):
        """
        Initialize the parser.
        
        Args:
            config: Optional parser-specific configuration
        """
        self.config = config or {}
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    @property
    @abstractmethod
    def tool_name(self) -> str:
        """Return the name of the BI tool this parser handles."""
        pass
    
    @property
    @abstractmethod
    def supported_versions(self) -> List[str]:
        """Return list of supported BI tool versions."""
        pass
    
    @abstractmethod
    def parse(self, file_path: Union[str, Path]) -> ParseResult:
        """
        Parse a BI tool export file.
        
        Args:
            file_path: Path to the export file (can be .zip, .xml, .json, etc.)
        
        Returns:
            ParseResult containing extracted objects, relationships, and errors
        
        Raises:
            ValueError: If file format is invalid
            FileNotFoundError: If file doesn't exist
        """
        pass
    
    @abstractmethod
    def validate_export(self, file_path: Union[str, Path]) -> bool:
        """
        Validate that the file is a valid export for this BI tool.
        
        Args:
            file_path: Path to the file to validate
        
        Returns:
            True if valid, False otherwise
        """
        pass
    
    def _log_progress(self, message: str, level: str = "info") -> None:
        """Helper to log parsing progress."""
        log_method = getattr(self.logger, level)
        log_method(message)
