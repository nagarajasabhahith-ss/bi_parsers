"""
Core data models for BI Parser Library.

These models represent the common output format across all BI tool parsers.
"""
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set
from pydantic import BaseModel, Field, PrivateAttr


class ObjectType(str, Enum):
    """Common BI object types across different tools."""
    FOLDER = "folder"
    REPORT = "report"
    DASHBOARD = "dashboard"
    DATA_MODULE = "data_module"
    DATA_SOURCE = "data_source"
    DATA_SOURCE_CONNECTION = "data_source_connection"
    PACKAGE = "package"
    QUERY = "query"
    VISUALIZATION = "visualization"
    # Data module sub-components
    TABLE = "table"  # Query subjects / tables
    COLUMN = "column"  # Individual columns
    DIMENSION = "dimension"  # Dimension columns
    MEASURE = "measure"  # Measure columns
    FILTER = "filter"
    CALCULATED_FIELD = "calculated_field"
    PARAMETER = "parameter"
    PROMPT = "prompt"
    HIERARCHY = "hierarchy"
    SORT = "sort"
    PAGE = "page"  # Report/dashboard pages
    TAB = "tab"  # Dashboard/report tabs
    OUTPUT = "output"  # Report output formats/versions
    UNKNOWN = "unknown"


class RelationshipType(str, Enum):
    """Types of relationships between objects."""
    PARENT_CHILD = "parent_child"
    USES = "uses"
    REFERENCES = "references"
    CONTAINS = "contains"
    DEPENDS_ON = "depends_on"
    # Data module relationships
    HAS_COLUMN = "has_column"  # table -> column
    AGGREGATES = "aggregates"  # measure aggregation
    FILTERS_BY = "filters_by"  # report/query -> filter
    USES_PARAMETER = "uses_parameter"  # object -> parameter
    CONNECTS_TO = "connects_to"  # module -> data source
    JOINS_TO = "joins_to"  # table -> table (join relationship)


class ParseErrorLevel(str, Enum):
    """Error severity levels."""
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ExtractedObject(BaseModel):
    """Represents a BI object extracted from source."""
    
    # Core identification
    object_id: str = Field(..., description="Unique identifier within source system")
    object_type: ObjectType
    name: str
    
    # Hierarchy
    parent_id: Optional[str] = None
    path: Optional[str] = None
    
    # Metadata
    properties: Dict[str, Any] = Field(default_factory=dict, description="Tool-specific properties")
    created_at: Optional[datetime] = None
    modified_at: Optional[datetime] = None
    owner: Optional[str] = None
    
    # Source tracking
    source_file: Optional[str] = None
    bi_tool: str  # e.g., "cognos", "tableau", "powerbi"
    
    class Config:
        use_enum_values = True


class Relationship(BaseModel):
    """Represents a relationship between two BI objects."""
    
    source_id: str
    target_id: str
    relationship_type: RelationshipType
    
    # Optional metadata
    properties: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True


class ParseError(BaseModel):
    """Represents an error encountered during parsing."""
    
    level: ParseErrorLevel
    message: str
    
    # Context
    file_name: Optional[str] = None
    line_number: Optional[int] = None
    xpath: Optional[str] = None
    object_id: Optional[str] = None
    
    # Additional details
    details: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        use_enum_values = True


class ParseResult(BaseModel):
    """The complete result of parsing a BI export."""
    
    objects: List[ExtractedObject] = Field(default_factory=list)
    relationships: List[Relationship] = Field(default_factory=list)
    errors: List[ParseError] = Field(default_factory=list)
    
    # Statistics
    stats: Dict[str, Any] = Field(default_factory=dict)
    
    # Deduplication: skip adding object if object_id already seen (same export, package + dataSource or multi-package)
    _seen_object_ids: Set[str] = PrivateAttr(default_factory=set)
    
    def has_object_id(self, obj_id: str) -> bool:
        """Return True if an object with this object_id was already added."""
        return obj_id in self._seen_object_ids
    
    def add_object(self, obj: ExtractedObject) -> None:
        """Add an extracted object; skip if object_id already present (first-wins)."""
        if obj.object_id in self._seen_object_ids:
            return
        self._seen_object_ids.add(obj.object_id)
        self.objects.append(obj)
    
    def add_relationship(self, rel: Relationship) -> None:
        """Add a relationship."""
        self.relationships.append(rel)
    
    def add_error(self, error: ParseError) -> None:
        """Add a parse error."""
        self.errors.append(error)
    
    def calculate_stats(self) -> None:
        """Calculate statistics from parsed data."""
        # Count by object type (convert enum to string value)
        type_counts = {}
        for obj in self.objects:
            # Convert enum to string value for consistent dictionary keys
            obj_type = obj.object_type.value if isinstance(obj.object_type, Enum) else str(obj.object_type)
            type_counts[obj_type] = type_counts.get(obj_type, 0) + 1
        
        # Count by relationship type
        rel_type_counts = {}
        for rel in self.relationships:
            rel_type = rel.relationship_type.value if isinstance(rel.relationship_type, Enum) else str(rel.relationship_type)
            rel_type_counts[rel_type] = rel_type_counts.get(rel_type, 0) + 1
        
        # Count by error level (convert enum to string value)
        error_counts = {}
        for error in self.errors:
            error_level = error.level.value if isinstance(error.level, Enum) else str(error.level)
            error_counts[error_level] = error_counts.get(error_level, 0) + 1
        
        self.stats = {
            "total_objects": len(self.objects),
            "total_relationships": len(self.relationships),
            "total_errors": len(self.errors),
            "objects_by_type": type_counts,
            "relationships_by_type": rel_type_counts,
            "errors_by_level": error_counts,
        }
