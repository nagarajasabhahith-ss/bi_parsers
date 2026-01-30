"""
Abstract base extractor class for parsing specific object types.
"""
from abc import ABC, abstractmethod
from typing import List, Any
import logging

from .models import ExtractedObject, Relationship, ParseError


logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """
    Abstract base class for extracting specific object types from BI exports.
    
    Each extractor is responsible for parsing one type of BI object
    (e.g., reports, dashboards, data modules).
    """
    
    def __init__(self, bi_tool: str):
        """
        Initialize the extractor.
        
        Args:
            bi_tool: Name of the BI tool (e.g., "cognos", "tableau")
        """
        self.bi_tool = bi_tool
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    @property
    @abstractmethod
    def object_type(self) -> str:
        """Return the object type this extractor handles."""
        pass
    
    @abstractmethod
    def extract(self, source: Any) -> tuple[List[ExtractedObject], List[Relationship], List[ParseError]]:
        """
        Extract objects from the source data.
        
        Args:
            source: The data source to extract from (XML element, JSON object, etc.)
        
        Returns:
            Tuple of (objects, relationships, errors)
        """
        pass
    
    def _create_object(
        self,
        object_id: str,
        name: str,
        **kwargs
    ) -> ExtractedObject:
        """
        Helper to create an ExtractedObject with common fields pre-filled.
        
        Args:
            object_id: Unique identifier
            name: Object name
            **kwargs: Additional fields for ExtractedObject
        
        Returns:
            ExtractedObject instance
        """
        return ExtractedObject(
            object_id=object_id,
            name=name,
            object_type=self.object_type,
            bi_tool=self.bi_tool,
            **kwargs
        )
    
    def _create_relationship(
        self,
        source_id: str,
        target_id: str,
        relationship_type: str,
        **kwargs
    ) -> Relationship:
        """
        Helper to create a Relationship.
        
        Args:
            source_id: Source object ID
            target_id: Target object ID
            relationship_type: Type of relationship
            **kwargs: Additional fields for Relationship
        
        Returns:
            Relationship instance
        """
        from .models import RelationshipType
        
        return Relationship(
            source_id=source_id,
            target_id=target_id,
            relationship_type=RelationshipType(relationship_type),
            **kwargs
        )
    
    def _create_error(
        self,
        level: str,
        message: str,
        **kwargs
    ) -> ParseError:
        """
        Helper to create a ParseError.
        
        Args:
            level: Error level (warning, error, critical)
            message: Error message
            **kwargs: Additional fields for ParseError
        
        Returns:
            ParseError instance
        """
        from .models import ParseErrorLevel
        
        return ParseError(
            level=ParseErrorLevel(level),
            message=message,
            **kwargs
        )
    
    def _log_extraction(self, count: int, errors: int = 0) -> None:
        """Log extraction results."""
        self.logger.info(
            f"Extracted {count} {self.object_type} objects "
            f"({errors} errors)"
        )
