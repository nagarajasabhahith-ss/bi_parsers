"""
Cognos 11.x Parser Implementation.
"""
from pathlib import Path
from typing import Union, List
import logging

from ..core import BaseParser, ParseResult, ParseError, ParseErrorLevel, ObjectType
from ..core.handlers import ZipHandler, XmlHandler
from .config import CognosConfig


logger = logging.getLogger(__name__)


class CognosParser(BaseParser):
    """Parser for IBM Cognos Analytics 11.x exports."""
    
    def __init__(self, config: dict = None):
        """Initialize Cognos parser."""
        super().__init__(config)
        self.cognos_config = CognosConfig(**(config or {}))
        self.temp_dir = None
    
    @property
    def tool_name(self) -> str:
        """Return the name of the BI tool."""
        return "cognos"
    
    @property
    def supported_versions(self) -> List[str]:
        """Return list of supported Cognos versions."""
        return ["11.0", "11.1", "11.2"]
    
    def validate_export(self, file_path: Union[str, Path]) -> bool:
        """
        Validate that the file is a valid Cognos export.
        
        Accepts either:
        - A ZIP file containing content.xml and package*.xml
        - An already-extracted directory containing content.xml and package*.xml
        
        Args:
            file_path: Path to the ZIP file or extracted directory to validate
        
        Returns:
            True if valid Cognos export, False otherwise
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            self._log_progress(f"File not found: {file_path}", "error")
            return False
        
        # Already-extracted directory: check for content.xml and package*.xml
        if file_path.is_dir():
            if not (file_path / "content.xml").exists():
                self._log_progress("Missing content.xml in export directory", "warning")
                return False
            package_files = list(file_path.glob("package*.xml"))
            if not package_files:
                self._log_progress("No package*.xml files found in export directory", "warning")
                return False
            self._log_progress("Valid Cognos export directory detected", "info")
            return True
        
        # ZIP file
        if not ZipHandler.is_zip(file_path):
            self._log_progress(f"Not a ZIP file or export directory: {file_path}", "warning")
            return False
        
        # Check for required files inside ZIP
        try:
            contents = ZipHandler.list_contents(file_path)
            
            if "content.xml" not in contents:
                self._log_progress("Missing content.xml in export", "warning")
                return False
            
            has_package = any(
                name.startswith("package") and name.endswith(".xml")
                for name in contents
            )
            if not has_package:
                self._log_progress("No package*.xml files found", "warning")
                return False
            
            self._log_progress("Valid Cognos export detected", "info")
            return True
            
        except Exception as e:
            self._log_progress(f"Error validating export: {e}", "error")
            return False
    
    def parse(self, file_path: Union[str, Path]) -> ParseResult:
        """
        Parse a Cognos export file.
        
        Args:
            file_path: Path to the Cognos export ZIP file
        
        Returns:
            ParseResult containing extracted objects, relationships, and errors
        
        Raises:
            ValueError: If file format is invalid
            FileNotFoundError: If file doesn't exist
        """
        file_path = Path(file_path)
        result = ParseResult()
        
        self._log_progress(f"Starting parse of {file_path.name}", "info")
        
        # Validate export
        if not self.validate_export(file_path):
            error = ParseError(
                level=ParseErrorLevel.CRITICAL,
                message=f"Invalid Cognos export file: {file_path.name}",
                file_name=str(file_path)
            )
            result.add_error(error)
            return result
        
        try:
            # Use directory directly or extract ZIP
            if file_path.is_dir():
                self._log_progress("Using export directory", "info")
                self.temp_dir = file_path
            else:
                self._log_progress("Extracting ZIP archive", "info")
                self.temp_dir = ZipHandler.extract(file_path)
            
            # Parse manifest (content.xml) to understand structure
            self._parse_manifest(self.temp_dir, result)
            
            # Parse package files
            self._parse_packages(self.temp_dir, result)
            
            # Parse data sources (dataSource.xml)
            self._parse_data_sources(self.temp_dir, result)
            
            # Post-process: Create CONNECTS_TO relationships from modules to data sources
            self._create_data_source_connections(result)
            
            # Calculate statistics
            result.calculate_stats()
            
            self._log_progress(
                f"Parse complete: {len(result.objects)} objects, "
                f"{len(result.relationships)} relationships, "
                f"{len(result.errors)} errors",
                "info"
            )
            
        except Exception as e:
            logger.exception(f"Error parsing Cognos export: {e}")
            error = ParseError(
                level=ParseErrorLevel.CRITICAL,
                message=f"Fatal error during parsing: {str(e)}",
                file_name=str(file_path)
            )
            result.add_error(error)
        
        finally:
            # Cleanup temporary directory
            if self.temp_dir and self.cognos_config.cleanup_temp:
                ZipHandler.cleanup(self.temp_dir)
                self.temp_dir = None
        
        return result
    
    def _create_data_source_connections(self, result: ParseResult) -> None:
        """
        Post-process to create CONNECTS_TO relationships from modules to data sources.
        
        Matches modules' useSpec references (storeIDs) to data source objects.
        """
        from ..core import Relationship, RelationshipType
        
        # Build mapping of storeID to data source object IDs
        data_sources_by_store_id = {}
        for obj in result.objects:
            if obj.object_type in [ObjectType.DATA_SOURCE, ObjectType.DATA_SOURCE_CONNECTION]:
                store_id = obj.properties.get("storeID")
                if store_id:
                    data_sources_by_store_id[store_id] = obj.object_id
        
        # Also check stats for data sources mapping
        if "data_sources_by_store_id" in result.stats:
            data_sources_by_store_id.update(result.stats["data_sources_by_store_id"])
        
        # Find all USES relationships that reference data sources
        uses_rels = [r for r in result.relationships if r.relationship_type == RelationshipType.USES]
        
        for rel in uses_rels:
            # Check if the target is a storeID that maps to a data source
            target_id = rel.target_id
            if target_id in data_sources_by_store_id:
                # This is a data source reference, create CONNECTS_TO relationship
                ds_obj_id = data_sources_by_store_id[target_id]
                
                # Check if CONNECTS_TO relationship already exists
                existing = any(
                    r.source_id == rel.source_id and 
                    r.target_id == ds_obj_id and 
                    r.relationship_type == RelationshipType.CONNECTS_TO
                    for r in result.relationships
                )
                
                if not existing:
                    connects_rel = Relationship(
                        source_id=rel.source_id,
                        target_id=ds_obj_id,
                        relationship_type=RelationshipType.CONNECTS_TO,
                        properties={
                            "connection_type": rel.properties.get("ref_type", "unknown"),
                            "identifier": rel.properties.get("identifier", ""),
                            "source": "useSpec"
                        }
                    )
                    result.add_relationship(connects_rel)
        
        # Also check for data modules that might reference data sources directly
        for obj in result.objects:
            if obj.object_type == ObjectType.DATA_MODULE:
                store_id = obj.properties.get("storeID")
                # Check if this module's storeID is referenced by any data source
                # (This would indicate the module is connected to a data source)
                for ds_obj in result.objects:
                    if ds_obj.object_type == ObjectType.DATA_SOURCE:
                        ds_store_id = ds_obj.properties.get("storeID")
                        # Check if module references this data source via useSpec
                        # Look for USES relationships from this module
                        module_uses = [
                            r for r in result.relationships 
                            if r.source_id == obj.object_id and 
                            r.relationship_type == RelationshipType.USES and
                            r.properties.get("dependency_type") == "data_source"
                        ]
                        for use_rel in module_uses:
                            if use_rel.target_id in data_sources_by_store_id:
                                ds_id = data_sources_by_store_id[use_rel.target_id]
                                # Create CONNECTS_TO if it doesn't exist
                                existing = any(
                                    r.source_id == obj.object_id and 
                                    r.target_id == ds_id and 
                                    r.relationship_type == RelationshipType.CONNECTS_TO
                                    for r in result.relationships
                                )
                                if not existing:
                                    connects_rel = Relationship(
                                        source_id=obj.object_id,
                                        target_id=ds_id,
                                        relationship_type=RelationshipType.CONNECTS_TO,
                                        properties={
                                            "connection_type": use_rel.properties.get("ref_type", "unknown"),
                                            "source": "post_process"
                                        }
                                    )
                                    result.add_relationship(connects_rel)
    
    def _parse_manifest(self, extract_dir: Path, result: ParseResult) -> None:
        """
        Parse content.xml manifest file.
        
        Args:
            extract_dir: Directory containing extracted files
            result: ParseResult to add objects/errors to
        """
        manifest_path = extract_dir / "content.xml"
        
        if not manifest_path.exists():
            result.add_error(ParseError(
                level=ParseErrorLevel.ERROR,
                message="content.xml not found in export",
                file_name="content.xml"
            ))
            return
        
        try:
            self._log_progress("Parsing content.xml manifest", "debug")
            tree = XmlHandler.parse(manifest_path)
            root = XmlHandler.get_root(tree)
            
            # Extract metadata
            cm_version = XmlHandler.get_text(root, ".//cmBuildNumber", default="unknown")
            edition = XmlHandler.get_text(root, ".//edition", default="unknown")
            archive_version = XmlHandler.get_text(root, ".//archiveVersion", default="unknown")
            
            # Store in result stats
            result.stats["cognos_version"] = cm_version
            result.stats["cognos_edition"] = edition
            result.stats["archive_version"] = archive_version
            
            self._log_progress(
                f"Cognos {cm_version} ({edition}) - Archive v{archive_version}",
                "info"
            )
            
        except Exception as e:
            logger.exception(f"Error parsing manifest: {e}")
            result.add_error(ParseError(
                level=ParseErrorLevel.ERROR,
                message=f"Failed to parse content.xml: {str(e)}",
                file_name="content.xml"
            ))

    def _parse_data_sources(self, extract_dir: Path, result: ParseResult) -> None:
        """
        Parse dataSource.xml file.
        
        Args:
            extract_dir: Directory containing extracted files
            result: ParseResult to add objects/errors to
        """
        ds_path = extract_dir / "dataSource.xml"
        if not ds_path.exists():
            # dataSource.xml is optional
            return
            
        self._log_progress("Parsing dataSource.xml", "debug")
        
        try:
            tree = XmlHandler.parse(ds_path)
            root = XmlHandler.get_root(tree)
            
            # Extract data source objects
            objects_elem = root.find(".//objects")
            if objects_elem is not None:
                from ..core import ExtractedObject, Relationship, RelationshipType
                
                # Track data sources by storeID for relationship creation
                data_sources_by_store_id = {}
                
                for obj_elem in objects_elem.findall("object"):
                    obj_class = XmlHandler.get_text(obj_elem, "class", default="unknown")
                    obj_id = XmlHandler.get_text(obj_elem, "id")
                    obj_name = XmlHandler.get_text(obj_elem, "name", default="<unnamed>")
                    parent_id = XmlHandler.get_text(obj_elem, "parentId")
                    
                    # Map to appropriate object type
                    obj_type = None
                    if obj_class in ["dataSource", "dataSourceReference"]:
                        obj_type = ObjectType.DATA_SOURCE
                    elif obj_class in ["dataSourceConnection", "connection"]:
                        obj_type = ObjectType.DATA_SOURCE_CONNECTION
                    elif obj_class in ["package", "packageReference"]:
                        # Packages in dataSource.xml are data sources
                        obj_type = ObjectType.DATA_SOURCE
                    elif obj_class in ["smartsModule", "dataModule", "module"]:
                        # Skip if this module was already extracted from a package file (avoid duplicate module + children)
                        if result.has_object_id(obj_id):
                            continue
                        # In dataSource.xml, smartsModule is a sub-module (child of baseModule); dataModule/module are main.
                        # All extracted as DATA_MODULE; is_main_module set in data_module_extractor.
                        # Use the data module extractor
                        from .extractors import DataModuleExtractor
                        dm_extractor = DataModuleExtractor()
                        dm_objects, dm_rels, dm_errors = dm_extractor.extract(obj_elem)
                        for obj in dm_objects:
                            obj.source_file = ds_path.name
                            result.add_object(obj)
                        for rel in dm_rels:
                            result.add_relationship(rel)
                        for err in dm_errors:
                            err.file_name = ds_path.name
                            result.add_error(err)
                        continue  # Skip further processing for data modules
                    elif obj_class in ["dataSourceSchema", "baseModule"]:
                        # These are metadata objects, extract as data sources
                        obj_type = ObjectType.DATA_SOURCE
                    
                    if obj_type:
                        props = {}
                        props_elem = obj_elem.find("props")
                        if props_elem is not None:
                            store_id = XmlHandler.get_text(props_elem, "storeID")
                            if store_id:
                                props["storeID"] = store_id
                                data_sources_by_store_id[store_id] = obj_id
                            
                            # Extract connection properties
                            connection_string = XmlHandler.get_text(props_elem, "connectionString/value")
                            if connection_string:
                                props["connection_string"] = connection_string
                            
                            # Try to get data source type from XML
                            data_source_type = XmlHandler.get_text(props_elem, "dataSourceType/value")
                            if not data_source_type:
                                # Try alternative paths
                                data_source_type = XmlHandler.get_text(props_elem, "dataSourceType")
                            
                            # If still not found, infer from connection string or name
                            if not data_source_type and connection_string:
                                conn_str_lower = connection_string.lower()
                                if 'bigquery' in conn_str_lower or 'BIGQUERY' in connection_string:
                                    data_source_type = "bigquery"
                                elif 'oracle' in conn_str_lower:
                                    data_source_type = "oracle"
                                elif 'sqlserver' in conn_str_lower or 'sql server' in conn_str_lower or 'mssql' in conn_str_lower:
                                    data_source_type = "sqlserver"
                                elif 'mysql' in conn_str_lower:
                                    data_source_type = "mysql"
                                elif 'postgres' in conn_str_lower:
                                    data_source_type = "postgresql"
                                elif 'snowflake' in conn_str_lower:
                                    data_source_type = "snowflake"
                                elif 'redshift' in conn_str_lower:
                                    data_source_type = "redshift"
                                elif 'teradata' in conn_str_lower:
                                    data_source_type = "teradata"
                                elif 'db2' in conn_str_lower:
                                    data_source_type = "db2"
                            
                            # Also check name for hints
                            if not data_source_type:
                                name_lower = obj_name.lower()
                                if 'bigquery' in name_lower or 'bq-' in name_lower:
                                    data_source_type = "bigquery"
                                elif 'oracle' in name_lower:
                                    data_source_type = "oracle"
                                elif 'sqlserver' in name_lower or 'sql server' in name_lower:
                                    data_source_type = "sqlserver"
                            
                            if data_source_type:
                                props["data_source_type"] = data_source_type
                        
                        props["cognosClass"] = obj_class
                        
                        ds_obj = ExtractedObject(
                            object_id=obj_id,
                            object_type=obj_type,
                            name=obj_name,
                            parent_id=parent_id if parent_id else None,
                            properties=props,
                            source_file=ds_path.name,
                            bi_tool="cognos"
                        )
                        result.add_object(ds_obj)
                        
                        # Create parent-child relationship if parent exists
                        if parent_id:
                            rel = Relationship(
                                source_id=parent_id,
                                target_id=obj_id,
                                relationship_type=RelationshipType.PARENT_CHILD
                            )
                            result.add_relationship(rel)
                
                # Store data sources mapping for later use in relationship creation
                result.stats["data_sources_by_store_id"] = data_sources_by_store_id
                self._log_progress(f"Extracted {len(data_sources_by_store_id)} data sources from dataSource.xml", "info")
                
        except Exception as e:
            logger.exception(f"Error parsing dataSource.xml: {e}")
            result.add_error(ParseError(
                level=ParseErrorLevel.WARNING,
                message=f"Failed to parse dataSource.xml: {str(e)}",
                file_name="dataSource.xml"
            ))
    
    def _parse_packages(self, extract_dir: Path, result: ParseResult) -> None:
        """
        Parse all package*.xml files.
        
        Args:
            extract_dir: Directory containing extracted files
            result: ParseResult to add objects/errors to
        """
        # Find all package XML files
        package_files = sorted(extract_dir.glob("package*.xml"))
        
        if not package_files:
            result.add_error(ParseError(
                level=ParseErrorLevel.ERROR,
                message="No package*.xml files found",
                file_name="<root>"
            ))
            return
        
        self._log_progress(f"Found {len(package_files)} package files", "info")
        
        # Parse each package file
        for package_file in package_files:
            self._parse_package_file(package_file, result)
    
    def _parse_package_file(self, package_path: Path, result: ParseResult) -> None:
        """
        Parse a single package*.xml file.
        
        Args:
            package_path: Path to package XML file
            result: ParseResult to add objects/errors to
        """
        try:
            self._log_progress(f"Parsing {package_path.name}", "debug")
            
            # Use streaming parser for large files
            from .extractors import (
                FolderExtractor,
                ReportExtractor,
                DashboardExtractor,
                DataModuleExtractor,
                VisualizationExtractor,
            )
            
            # For now, parse the entire file
            tree = XmlHandler.parse(package_path)
            root = XmlHandler.get_root(tree)
            
            # Extract all objects
            objects_elem = root.find(".//objects")
            if objects_elem is None:
                self._log_progress(
                    f"No objects found in {package_path.name}",
                    "warning"
                )
                return
            
            # Initialize extractors
            extractors = {
                ObjectType.FOLDER: FolderExtractor(),
                ObjectType.REPORT: ReportExtractor(),
                ObjectType.DASHBOARD: DashboardExtractor(),
                ObjectType.DATA_MODULE: DataModuleExtractor(),
                ObjectType.VISUALIZATION: VisualizationExtractor(),
            }
            
            # Process each object element
            for obj_elem in objects_elem.findall("object"):
                self._parse_object(obj_elem, package_path.name, result, extractors)
            
        except Exception as e:
            logger.exception(f"Error parsing {package_path.name}: {e}")
            result.add_error(ParseError(
                level=ParseErrorLevel.ERROR,
                message=f"Failed to parse {package_path.name}: {str(e)}",
                file_name=package_path.name
            ))
    
    def _parse_object(
        self,
        obj_elem,
        source_file: str,
        result: ParseResult,
        extractors: dict = None
    ) -> None:
        """
        Parse a single object element.
        
        Args:
            obj_elem: XML element representing the object
            source_file: Name of source file
            result: ParseResult to add objects/errors to
            extractors: Dictionary of initialized extractors
        """
        try:
            # Get object type (class)
            obj_class = XmlHandler.get_text(obj_elem, "class", default="unknown")
            
            # Map Cognos class to our ObjectType
            from ..core import ObjectType, ExtractedObject, Relationship, RelationshipType
            
            object_type_map = {
                # Folders
                "folder": ObjectType.FOLDER,
                "catalogFolder": ObjectType.FOLDER,
                
                # Reports
                "report": ObjectType.REPORT,
                "interactiveReport": ObjectType.REPORT,
                "reportView": ObjectType.REPORT,
                "reportVersion": ObjectType.REPORT,
                "dataset2": ObjectType.REPORT,
                
                # Report Outputs (intermediate between reports and pages)
                "output": ObjectType.OUTPUT,  # Report output formats
                
                # Dashboards
                "dashboard": ObjectType.DASHBOARD,
                "exploration": ObjectType.DASHBOARD,
                "story": ObjectType.DASHBOARD,
                
                # Pages and Tabs (structural elements)
                "page": ObjectType.PAGE,  # Report/dashboard pages
                "tab": ObjectType.TAB,  # Dashboard/report tabs
                "tabPage": ObjectType.TAB,
                "reportPage": ObjectType.PAGE,
                
                # Data Modules / Models (main: module, dataModule, model; sub: smartsModule, modelView, dataSet2)
                "dataModule": ObjectType.DATA_MODULE,
                "smartsModule": ObjectType.DATA_MODULE,
                "module": ObjectType.DATA_MODULE,
                "model": ObjectType.DATA_MODULE,
                "modelView": ObjectType.DATA_MODULE,
                "dataSet2": ObjectType.DATA_MODULE,  # Datasets
                
                # Packages (separate from data modules)
                "package": ObjectType.PACKAGE,
                "packageConfiguration": ObjectType.PACKAGE,  # Package configuration objects
                
                # Visualizations
                "visualization": ObjectType.VISUALIZATION,
                
                # Queries
                "query": ObjectType.QUERY,
            }
            
            object_type = object_type_map.get(obj_class, ObjectType.UNKNOWN)
            
            # If we have a specific extractor for this type, use it
            if extractors and object_type in extractors:
                # Skip data module if already extracted (e.g. from another package file)
                if object_type == ObjectType.DATA_MODULE:
                    obj_id = XmlHandler.get_text(obj_elem, "id", default="")
                    if obj_id and result.has_object_id(obj_id):
                        return
                extractor = extractors[object_type]
                objects, relationships, errors = extractor.extract(obj_elem)
                
                for obj in objects:
                    obj.source_file = source_file
                    result.add_object(obj)
                
                for rel in relationships:
                    result.add_relationship(rel)
                    
                for err in errors:
                    err.file_name = source_file
                    result.add_error(err)
                    
                return

            # Fallback for unknown types (same as before)
            obj_id = XmlHandler.get_text(obj_elem, "id", default="")
            obj_name = XmlHandler.get_text(obj_elem, "name", default="<unnamed>")
            parent_id = XmlHandler.get_text(obj_elem, "parentId")
            store_id = XmlHandler.get_text(obj_elem, "storeID")

            # Extract properties
            props = {}
            props_elem = obj_elem.find("props")
            if props_elem is not None:
                # Extract creation/modification times
                creation_time = XmlHandler.get_text(
                    props_elem, "creationTime/value"
                )
                mod_time = XmlHandler.get_text(
                    props_elem, "modificationTime/value"
                )
                owner = XmlHandler.get_text(
                    props_elem, "owner/value/item/searchPath/value"
                )
                
                if creation_time:
                    props["creationTime"] = creation_time
                if mod_time:
                    props["modificationTime"] = mod_time
                if owner:
                    props["owner"] = owner
            
            # Add storeID and class to properties
            if store_id:
                props["storeID"] = store_id
            props["cognosClass"] = obj_class
            
            # Create extracted object
            extracted_obj = ExtractedObject(
                object_id=obj_id,
                object_type=object_type,
                name=obj_name,
                parent_id=parent_id if parent_id else None,
                properties=props,
                source_file=source_file,
                bi_tool="cognos"
            )
            
            result.add_object(extracted_obj)
            
            # Create parent-child relationship if parent exists
            if parent_id:
                relationship = Relationship(
                    source_id=parent_id,
                    target_id=obj_id,
                    relationship_type=RelationshipType.PARENT_CHILD
                )
                result.add_relationship(relationship)
            
        except Exception as e:
            logger.exception(f"Error parsing object: {e}")
            result.add_error(ParseError(
                level=ParseErrorLevel.WARNING,
                message=f"Failed to parse object: {str(e)}",
                file_name=source_file
            ))

