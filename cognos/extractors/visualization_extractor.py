"""
Visualization extractor for Cognos.

This module provides utility functions for extracting visualization data
from both dashboard JSON specifications and report XML specifications.

Note: Visualizations are not standalone objects in Cognos exports.
They are embedded within dashboards (as JSON widgets) and reports (as XML elements).
The actual extraction is done by DashboardExtractor and ReportExtractor.
This module provides shared utilities and the mapping functions.
"""
from typing import List, Any, Dict, Optional
from datetime import datetime

from ...core import BaseExtractor, ExtractedObject, Relationship, ParseError, ObjectType, RelationshipType
from ...core.handlers import XmlHandler
from ..visualization_types import (
    map_dashboard_visid_to_type,
    map_report_element_to_type,
    DASHBOARD_VIS_ID_MAP,
    REPORT_ELEMENT_MAP,
    ALL_VISUALIZATION_TYPES
)


class VisualizationExtractor(BaseExtractor):
    """
    Utility extractor for Cognos visualization objects.
    
    Note: In Cognos exports, visualizations don't have their own class type.
    They are embedded in:
    - Dashboards (exploration): JSON specification with 'widgets' array
    - Reports (report): XML specification with chart/list/crosstab elements
    
    The actual extraction is performed by DashboardExtractor and ReportExtractor.
    This class provides utility methods for visualization-related operations.
    """
    
    def __init__(self):
        super().__init__(bi_tool="cognos")
    
    @property
    def object_type(self) -> str:
        return ObjectType.VISUALIZATION.value
    
    def extract(
        self,
        source: Any
    ) -> tuple[List[ExtractedObject], List[Relationship], List[ParseError]]:
        """
        Extract visualization objects from XML element.
        
        Note: This is a fallback extractor. In practice, visualizations are
        extracted from dashboard and report specifications by their respective
        extractors. This method handles the rare case of a standalone
        'visualization' class object in the XML.
        
        Args:
            source: XML element containing visualization data
        
        Returns:
            Tuple of (objects, relationships, errors)
        """
        objects = []
        relationships = []
        errors = []
        
        try:
            # Get basic info
            obj_id = XmlHandler.get_text(source, "id")
            name = XmlHandler.get_text(source, "name", default="<unnamed>")
            parent_id = XmlHandler.get_text(source, "parentId")
            store_id = XmlHandler.get_text(source, "storeID")
            obj_class = XmlHandler.get_text(source, "class")
            
            # Get properties
            props_elem = source.find("props")
            properties = {
                "storeID": store_id,
                "cognosClass": obj_class,
                "visualization_type": "Unknown"  # Default, will be overridden if spec found
            }
            
            created_at = None
            modified_at = None
            owner = None
            
            if props_elem is not None:
                # Extract timestamps
                creation_time_str = XmlHandler.get_text(
                    props_elem,
                    "creationTime/value"
                )
                mod_time_str = XmlHandler.get_text(
                    props_elem,
                    "modificationTime/value"
                )
                
                if creation_time_str:
                    try:
                        created_at = datetime.fromisoformat(
                            creation_time_str.replace('Z', '+00:00')
                        )
                        properties["creationTime"] = creation_time_str
                    except ValueError:
                        pass
                
                if mod_time_str:
                    try:
                        modified_at = datetime.fromisoformat(
                            mod_time_str.replace('Z', '+00:00')
                        )
                        properties["modificationTime"] = mod_time_str
                    except ValueError:
                        pass
                
                # Extract owner
                owner = XmlHandler.get_text(
                    props_elem,
                    "owner/value/item/searchPath/value"
                )
                if owner:
                    properties["owner"] = owner

                # Optional props (display, visibility)
                display_seq = XmlHandler.get_text(props_elem, "displaySequence/value")
                if display_seq is not None:
                    try:
                        properties["displaySequence"] = int(display_seq)
                    except (ValueError, TypeError):
                        pass
                hidden = XmlHandler.get_text(props_elem, "hidden/value")
                if hidden:
                    properties["hidden"] = hidden.lower() == "true"
            
            # Create visualization object
            viz = self._create_object(
                object_id=obj_id,
                name=name,
                parent_id=parent_id,
                properties=properties,
                created_at=created_at,
                modified_at=modified_at,
                owner=owner,
            )
            
            objects.append(viz)
            
            # Create parent-child relationship (usually Parent is Dashboard/Report or Page)
            if parent_id:
                rel = self._create_relationship(
                    source_id=parent_id,
                    target_id=obj_id,
                    relationship_type=RelationshipType.PARENT_CHILD
                )
                relationships.append(rel)
            
        except Exception as e:
            error = self._create_error(
                level="error",
                message=f"Failed to extract visualization: {str(e)}"
            )
            errors.append(error)
        
        self._log_extraction(len(objects), len(errors))
        return objects, relationships, errors
    
    @staticmethod
    def get_supported_visualization_types() -> List[str]:
        """Return list of all supported visualization types."""
        return ALL_VISUALIZATION_TYPES
    
    @staticmethod
    def map_visid(vis_id: str) -> str:
        """Map a dashboard visId to human-readable chart type."""
        return map_dashboard_visid_to_type(vis_id)
    
    @staticmethod
    def map_element(element_tag: str, chart_type: Optional[str] = None) -> str:
        """Map a report XML element to human-readable chart type."""
        return map_report_element_to_type(element_tag, chart_type)
