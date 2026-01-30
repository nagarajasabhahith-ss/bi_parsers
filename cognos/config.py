"""
Configuration for Cognos parser.
"""
from pydantic import BaseModel, Field


class CognosConfig(BaseModel):
    """Configuration options for Cognos parser."""
    
    # Cleanup options
    cleanup_temp: bool = Field(
        default=True,
        description="Cleanup temporary extraction directory after parsing"
    )
    
    # Parsing options
    max_file_size_mb: int = Field(
        default=500,
        description="Maximum file size in MB for parsing"
    )
    
    streaming_threshold_mb: int = Field(
        default=50,
        description="Use streaming parser for files larger than this (MB)"
    )
    
    # Object filtering
    include_folders: bool = Field(
        default=True,
        description="Include folder objects in results"
    )
    
    include_hidden: bool = Field(
        default=False,
        description="Include hidden objects in results"
    )
    
    # Relationship mapping
    extract_column_lineage: bool = Field(
        default=True,
        description="Extract column-level lineage from queries"
    )
    
    max_relationship_depth: int = Field(
        default=10,
        description="Maximum depth for relationship traversal"
    )
    
    # Error handling
    continue_on_error: bool = Field(
        default=True,
        description="Continue parsing even if individual objects fail"
    )
    
    strict_validation: bool = Field(
        default=False,
        description="Enable strict validation of Cognos exports"
    )
