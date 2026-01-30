"""
Folder extractor for Cognos.
"""
from typing import List, Any
from datetime import datetime

from ...core import BaseExtractor, ExtractedObject, Relationship, ParseError, ObjectType
from ...core.handlers import XmlHandler


class FolderExtractor(BaseExtractor):
    """Extractor for Cognos folder objects."""
    
    def __init__(self):
        super().__init__(bi_tool="cognos")
    
    @property
    def object_type(self) -> str:
        return ObjectType.FOLDER.value
    
    def extract(
        self,
        source: Any
    ) -> tuple[List[ExtractedObject], List[Relationship], List[ParseError]]:
        """
        Extract folder objects from XML element.
        
        Args:
            source: XML element containing folder data
        
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
            
            # Get properties
            props_elem = source.find("props")
            properties = {
                "storeID": store_id,
                "cognosClass": "folder"
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
                
                # Extract other properties
                hidden = XmlHandler.get_text(props_elem, "hidden/value")
                if hidden:
                    properties["hidden"] = hidden.lower() == "true"
            
            # Create folder object
            folder = self._create_object(
                object_id=obj_id,
                name=name,
                parent_id=parent_id,
                properties=properties,
                created_at=created_at,
                modified_at=modified_at,
                owner=owner,
            )
            
            objects.append(folder)
            
            # Create parent-child relationship
            if parent_id:
                rel = self._create_relationship(
                    source_id=parent_id,
                    target_id=obj_id,
                    relationship_type="parent_child"
                )
                relationships.append(rel)
            
        except Exception as e:
            error = self._create_error(
                level="error",
                message=f"Failed to extract folder: {str(e)}"
            )
            errors.append(error)
        
        self._log_extraction(len(objects), len(errors))
        return objects, relationships, errors
