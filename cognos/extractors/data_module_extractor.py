"""
Data Module extractor for Cognos.

Enhanced extractor that captures:
- Tables (query subjects)
- Columns with full metadata (type, usage, aggregation)
- Calculated fields with expressions
- Filters (detail, summary, local)
- Parameters and prompts
- Hierarchies
- Data source connections
"""
from typing import List, Any, Dict, Optional, Set, Tuple
from datetime import datetime
import re
import logging
import base64
import gzip
import io

from ...core import BaseExtractor, ExtractedObject, Relationship, ParseError, ObjectType, RelationshipType
from ...core.handlers import XmlHandler


logger = logging.getLogger(__name__)

# Cognos classes that represent main/root data modules (Framework Manager modules, models).
# Sub-modules (smartsModule, modelView, dataSet2) are children or views and are not "main" modules.
MAIN_MODULE_COGNOS_CLASSES = frozenset({"module", "dataModule", "model"})

# Cognos RS_dataType mapping to standard data types
DATA_TYPE_MAP = {
    "1": "boolean",
    "2": "integer",
    "3": "string",
    "4": "float",
    "5": "double",
    "6": "timestamp",
    "7": "date",
    "8": "time",
    "9": "decimal",
    "10": "binary",
}

# Cognos RS_dataUsage mapping
DATA_USAGE_MAP = {
    "": "unknown",
    "0": "attribute",
    "1": "dimension",
    "2": "measure",
}

# Aggregation type mapping
AGGREGATION_MAP = {
    "none": "none",
    "total": "sum",
    "count": "count",
    "average": "average",
    "minimum": "minimum",
    "maximum": "maximum",
}

# Patterns that indicate a dataItem expression is an actual calculated field (user-defined expression),
# not a simple column reference or a simple aggregation. Used to avoid double-extraction and over-counting.
# Excludes: simple aggregates like SUM([Col]), COUNT([Col]), AVG([Col]) — those are measures, not calculations.
# Includes: CASE/IF logic, COALESCE/cast, date functions, arithmetic combining columns, min/max/abs in expression.
CALC_EXPRESSION_PATTERNS = (
    r'CASE\s+WHEN',
    r'\bIF\s*\(',
    r'\babs\s*\(',
    r'\bminimum\s*\(',
    r'\bmaximum\s*\(',
    r'\bcast\s*\(',
    r'\bcurrent_date',
    r'\bcurrent_timestamp',
    r'\bCOALESCE\s*\(',
    r'[\+\-\*\/]',  # Arithmetic operators (combining columns)
)
_CALC_EXPRESSION_REGEX = re.compile('|'.join(CALC_EXPRESSION_PATTERNS), re.IGNORECASE)


def _expression_is_calculated_field(expression: Optional[str]) -> bool:
    """Return True if the expression indicates a calculated field (not a simple column reference)."""
    if not expression or not isinstance(expression, str):
        return False
    return bool(_CALC_EXPRESSION_REGEX.search(expression.strip()))


class DataModuleExtractor(BaseExtractor):
    """Extractor for Cognos Data Module objects (smartsModule, module, model).
    
    Extracts comprehensive metadata including:
    - Module-level info (name, timestamps, owner)
    - Tables/Query subjects from tags
    - Columns with data type, usage, and aggregation
    - Calculated fields with expressions
    - Filters
    - Parameters and prompts
    """
    
    def __init__(self):
        super().__init__(bi_tool="cognos")
    
    @property
    def object_type(self) -> str:
        return ObjectType.DATA_MODULE.value
    
    def extract(
        self,
        source: Any
    ) -> Tuple[List[ExtractedObject], List[Relationship], List[ParseError]]:
        """
        Extract data module objects from XML element.
        
        Args:
            source: XML element containing data module data
        
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
            is_main_module = obj_class in MAIN_MODULE_COGNOS_CLASSES

            # Get properties
            props_elem = source.find("props")
            properties = {
                "storeID": store_id,
                "cognosClass": obj_class,
                "is_main_module": is_main_module,
            }
            
            created_at = None
            modified_at = None
            owner = None
            
            # Statistics for this module
            table_count = 0
            column_count = 0
            calculated_field_count = 0
            filter_count = 0
            
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

                # Optional props (display, tenant)
                display_seq = XmlHandler.get_text(props_elem, "displaySequence/value")
                if display_seq is not None:
                    try:
                        properties["displaySequence"] = int(display_seq)
                    except (ValueError, TypeError):
                        pass
                hidden = XmlHandler.get_text(props_elem, "hidden/value")
                if hidden:
                    properties["hidden"] = hidden.lower() == "true"
                tenant_id = XmlHandler.get_text(props_elem, "tenantID/value")
                if tenant_id:
                    properties["tenantID"] = tenant_id
                
                # Extract from smartsData first (has full metadata)
                smarts_columns_by_path = {}  # Map full_path -> column object
                smarts_tables_by_name = {}  # Map table_name -> table object
                
                smarts_data_elem = props_elem.find("smartsData")
                if smarts_data_elem is not None:
                    smarts_objects, smarts_rels, smarts_errors, smarts_tables, smarts_columns = self._extract_smarts_data(
                        smarts_data_elem, obj_id, name, is_main_module
                    )
                    objects.extend(smarts_objects)
                    relationships.extend(smarts_rels)
                    errors.extend(smarts_errors)
                    
                    # Build lookup maps for matching
                    for col in smarts_columns:
                        full_path = col.properties.get('full_path') or f"{col.properties.get('table', '')}.{col.name}"
                        smarts_columns_by_path[full_path] = col
                    
                    for table in smarts_tables:
                        smarts_tables_by_name[table.name] = table
                    
                    table_count = len(smarts_tables)
                    column_count = len(smarts_columns)
                
                # Extract tables and columns from tags (may have columns not in smartsData)
                tags_elem = props_elem.find("tags/value")
                if tags_elem is None:
                    tags_container = props_elem.find("tags")
                    if tags_container is not None:
                        tags_elem = tags_container.find("value")
                if tags_elem is not None:
                    tables, columns, table_rels, column_rels = self._extract_tables_and_columns(
                        tags_elem, obj_id, name, smarts_columns_by_path, smarts_tables_by_name
                    )
                    
                    # Add tables that aren't in smartsData
                    for table in tables:
                        if table.name not in smarts_tables_by_name:
                            objects.append(table)
                            relationships.extend([r for r in table_rels if r.target_id == table.object_id])
                            table_count += 1
                    
                    # Add/enrich columns from tags
                    for col in columns:
                        full_path = col.properties.get('full_path') or f"{col.properties.get('table', '')}.{col.name}"
                        
                        # Check if we have metadata from smartsData
                        if full_path in smarts_columns_by_path:
                            # Use the smartsData column (has full metadata)
                            # Don't add the tag column, smartsData column already added
                            continue
                        else:
                            # Column only in tags, add it (no metadata but still valid)
                            objects.append(col)
                            relationships.extend([r for r in column_rels if r.target_id == col.object_id])
                            column_count += 1
            
            # Count calculated fields and filters from extracted objects
            calculated_field_count = sum(1 for obj in objects if obj.object_type == ObjectType.CALCULATED_FIELD)
            filter_count = sum(1 for obj in objects if obj.object_type == ObjectType.FILTER)
            
            # Store statistics in properties
            properties["table_count"] = table_count
            properties["column_count"] = column_count
            properties["calculated_field_count"] = calculated_field_count
            properties["filter_count"] = filter_count
            
            # Create data module object
            data_module = self._create_object(
                object_id=obj_id,
                name=name,
                parent_id=parent_id,
                properties=properties,
                created_at=created_at,
                modified_at=modified_at,
                owner=owner,
            )
            
            objects.append(data_module)
            
            # Create parent-child relationship (target = this module; so target_is_main_module marks main vs sub)
            if parent_id:
                rel = self._create_relationship(
                    source_id=parent_id,
                    target_id=obj_id,
                    relationship_type=RelationshipType.PARENT_CHILD,
                    properties={"target_is_main_module": is_main_module},
                )
                relationships.append(rel)
            
        except Exception as e:
            logger.exception(f"Failed to extract data module: {e}")
            error = self._create_error(
                level="error",
                message=f"Failed to extract data module: {str(e)}"
            )
            errors.append(error)
        
        self._log_extraction(len(objects), len(errors))
        return objects, relationships, errors

    def _extract_tables_and_columns(
        self,
        tags_elem: Any,
        module_id: str,
        module_name: str,
        smarts_columns_by_path: Optional[Dict[str, ExtractedObject]] = None,
        smarts_tables_by_name: Optional[Dict[str, ExtractedObject]] = None
    ) -> Tuple[List[ExtractedObject], List[ExtractedObject], List[Relationship], List[Relationship]]:
        """
        Extract tables and columns from tags element.
        
        Tags format: Table.Column or just Table
        
        Args:
            tags_elem: XML element containing tags
            module_id: ID of the parent module
            module_name: Name of the parent module
            smarts_columns_by_path: Optional dict mapping full_path -> column from smartsData
            smarts_tables_by_name: Optional dict mapping table_name -> table from smartsData
        
        Returns:
            Tuple of (tables, columns, table_relationships, column_relationships)
        """
        if smarts_columns_by_path is None:
            smarts_columns_by_path = {}
        if smarts_tables_by_name is None:
            smarts_tables_by_name = {}
        
        tables = []
        columns = []
        table_rels = []
        column_rels = []
        
        # Track unique tables
        table_map: Dict[str, ExtractedObject] = {}
        
        for item in tags_elem.findall("item"):
            if item.text:
                tag_value = item.text.strip()
                
                if '.' in tag_value:
                    # Format: Table.Column
                    parts = tag_value.split('.', 1)
                    table_name = parts[0]
                    column_name = parts[1] if len(parts) > 1 else None
                    
                    # Check if table exists in smartsData
                    if table_name in smarts_tables_by_name:
                        # Table already extracted from smartsData, use that
                        table_map[table_name] = smarts_tables_by_name[table_name]
                    elif table_name not in table_map:
                        # Create table if not exists
                        table_id = f"{module_id}:table:{table_name}"
                        table_obj = self._create_object(
                            object_id=table_id,
                            name=table_name,
                            parent_id=module_id,
                            properties={
                                "cognosClass": "querySubject",
                                "module_name": module_name
                            }
                        )
                        table_obj.object_type = ObjectType.TABLE
                        table_map[table_name] = table_obj
                        tables.append(table_obj)
                        
                        # Create CONTAINS relationship: module -> table
                        rel = self._create_relationship(
                            source_id=module_id,
                            target_id=table_id,
                            relationship_type=RelationshipType.CONTAINS
                        )
                        table_rels.append(rel)
                    
                    # Create column if column name exists and not in smartsData
                    if column_name:
                        full_path = tag_value
                        # Check if column already exists in smartsData (has full metadata)
                        if full_path not in smarts_columns_by_path:
                            # Column only in tags, create it (no metadata)
                            table_id = table_map[table_name].object_id
                            column_id = f"{module_id}:col:{tag_value}"
                            
                            column_obj = self._create_object(
                                object_id=column_id,
                                name=column_name,
                                parent_id=table_id,
                                properties={
                                    "table": table_name,
                                    "full_path": tag_value,
                                    "identifier": tag_value,
                                    "cognosClass": "queryItem",
                                    "source": "tags"
                                }
                            )
                            column_obj.object_type = ObjectType.COLUMN
                            columns.append(column_obj)
                            
                            # Create HAS_COLUMN relationship: table -> column
                            rel = self._create_relationship(
                                source_id=table_id,
                                target_id=column_id,
                                relationship_type=RelationshipType.HAS_COLUMN
                            )
                            column_rels.append(rel)
                else:
                    # Just table name, no column
                    # Check if table exists in smartsData
                    if tag_value in smarts_tables_by_name:
                        # Table already extracted from smartsData
                        table_map[tag_value] = smarts_tables_by_name[tag_value]
                    elif tag_value not in table_map:
                        # Create table if not exists
                        table_id = f"{module_id}:table:{tag_value}"
                        table_obj = self._create_object(
                            object_id=table_id,
                            name=tag_value,
                            parent_id=module_id,
                            properties={
                                "cognosClass": "querySubject",
                                "module_name": module_name
                            }
                        )
                        table_obj.object_type = ObjectType.TABLE
                        table_map[tag_value] = table_obj
                        tables.append(table_obj)
                        
                        # Create CONTAINS relationship
                        rel = self._create_relationship(
                            source_id=module_id,
                            target_id=table_id,
                            relationship_type=RelationshipType.CONTAINS
                        )
                        table_rels.append(rel)
        
        return tables, columns, table_rels, column_rels

    def _extract_smarts_data(
        self,
        smarts_data_elem: Any,
        module_id: str,
        module_name: str,
        is_main_module: bool = False,
    ) -> Tuple[List[ExtractedObject], List[Relationship], List[ParseError], List[ExtractedObject], List[ExtractedObject]]:
        """
        Extract detailed information from smartsData blob.
        
        This may contain JSON with moserJSON module definition including:
        - useSpec references (links to base data modules)
        - calculation definitions (calculated fields)
        - metadataTreeView (folder structure)
        - dataRetrievalMode (live vs cached)
        - querySubjects (tables) with full column metadata
        - relationships (joins between tables)
        """
        import json
        
        objects = []
        relationships = []
        errors = []
        tables = []
        columns = []
        
        try:
            # smartsData may contain text content or nested value element
            content = None
            if smarts_data_elem.text:
                content = smarts_data_elem.text.strip()
            else:
                value_elem = smarts_data_elem.find("value")
                if value_elem is not None and value_elem.text:
                    content = value_elem.text.strip()
            
            if not content:
                return objects, relationships, errors, tables, columns
            
            # Try gzip+base64 decode (Cognos Trace export often stores smartsData this way)
            if content.strip().startswith("H4sI") or (len(content) > 100 and not content.strip().startswith("{")):
                try:
                    raw = base64.b64decode(content)
                    if raw[:2] == b"\x1f\x8b":  # gzip magic
                        content = gzip.GzipFile(fileobj=io.BytesIO(raw), mode="rb").read().decode("utf-8")
                except Exception:
                    pass
            
            # Try to parse as JSON
            try:
                data = json.loads(content)
            except json.JSONDecodeError:
                # Not JSON, might be XML blob - skip for now
                return objects, relationships, errors, tables, columns
            
            # Extract moserJSON if present (embedded module definition)
            moser_json = data.get("moserJSON") or data
            
            # Extract useSpec references (dependencies on other modules/data sources)
            use_specs = moser_json.get("useSpec", [])
            if not isinstance(use_specs, list):
                use_specs = [use_specs] if use_specs else []
            seen_use_spec_ids: Set[str] = set()
            for use_spec in use_specs:
                ref_store_id = use_spec.get("storeID")
                ref_type = use_spec.get("type", "module")
                ref_identifier = use_spec.get("identifier", "")
                
                if ref_store_id and ref_store_id not in seen_use_spec_ids:
                    seen_use_spec_ids.add(ref_store_id)
                    # Check if this is a data source (from dataSource.xml) or another module
                    # Try to determine based on type or check if it exists as a data source
                    rel_type = RelationshipType.USES
                    dep_type = "data_source" if ref_type in ["dataSource", "package", "connection"] else "module"
                    
                    # If it's a data source reference, use CONNECTS_TO
                    if ref_type in ["dataSource", "package", "connection", "dataSourceReference"]:
                        rel_type = RelationshipType.CONNECTS_TO
                        dep_type = "data_source"
                    
                    rel = self._create_relationship(
                        source_id=module_id,
                        target_id=ref_store_id,
                        relationship_type=rel_type,
                        properties={
                            "dependency_type": dep_type,
                            "ref_type": ref_type,
                            "identifier": ref_identifier,
                            "source_is_main_module": is_main_module,
                        },
                    )
                    relationships.append(rel)
            
            # Extract querySubjects (tables) with columns (dedupe by table_id)
            query_subjects = moser_json.get("querySubject", [])
            if not isinstance(query_subjects, list):
                query_subjects = [query_subjects] if query_subjects else []
            seen_table_ids: Set[str] = set()
            seen_column_ids: Set[str] = set()
            for qs in query_subjects:
                qs_name = qs.get("name") or qs.get("identifier", "Unknown")
                qs_id = qs.get("id") or qs.get("identifier", "")
                table_id = f"{module_id}:table:{qs_name}"
                if table_id in seen_table_ids:
                    continue
                seen_table_ids.add(table_id)
                
                # Create table object
                table_obj = self._create_object(
                    object_id=table_id,
                    name=qs_name,
                    parent_id=module_id,
                    properties={
                        "cognosClass": "querySubject",
                        "module_name": module_name,
                        "source_id": qs_id
                    }
                )
                table_obj.object_type = ObjectType.TABLE
                tables.append(table_obj)
                
                # Create CONTAINS relationship
                rel = self._create_relationship(
                    source_id=module_id,
                    target_id=table_id,
                    relationship_type=RelationshipType.CONTAINS
                )
                relationships.append(rel)
                
                # Extract columns (queryItems) from querySubject (dedupe by column_id)
                query_items = qs.get("queryItem", [])
                if not isinstance(query_items, list):
                    query_items = [query_items] if query_items else []
                for qi in query_items:
                    col_name = qi.get("name") or qi.get("identifier", "Unknown")
                    col_id = qi.get("id") or qi.get("identifier", "")
                    column_id = f"{module_id}:col:{qs_name}.{col_name}"
                    if column_id in seen_column_ids:
                        continue
                    seen_column_ids.add(column_id)
                    expression = qi.get("expression", "")
                    usage = qi.get("usage", "")
                    datatype = qi.get("datatype", "")
                    aggregate = qi.get("regularAggregate", "")
                    
                    # Map usage to data_usage (support both string and numeric Cognos codes: 0=attr, 1=dim, 2=measure)
                    usage_str = str(usage).strip() if usage is not None else ""
                    data_usage = DATA_USAGE_MAP.get(usage_str, "unknown")
                    if data_usage == "unknown" and usage_str:
                        if usage in ("fact", "measure") or usage == 2:
                            data_usage = "measure"
                        elif usage in ("attribute", "dimension") or usage in (0, 1):
                            data_usage = "dimension" if usage in (1, "dimension") else "attribute"
                    
                    # Map datatype
                    data_type = "unknown"
                    if datatype:
                        # Try to map numeric datatype codes
                        if isinstance(datatype, str) and datatype.isdigit():
                            data_type = DATA_TYPE_MAP.get(datatype, datatype)
                        else:
                            data_type = datatype
                    
                    # Map aggregate (support non-string from JSON)
                    agg_type = "none"
                    if aggregate is not None and aggregate != "":
                        agg_type = AGGREGATION_MAP.get(str(aggregate).lower(), str(aggregate))
                    
                    # Create column object with full metadata
                    column_obj = self._create_object(
                        object_id=column_id,
                        name=col_name,
                        parent_id=table_id,
                        properties={
                            "table": qs_name,
                            "full_path": f"{qs_name}.{col_name}",
                            "identifier": col_id,
                            "expression": expression,
                            "data_type": data_type,
                            "data_usage": data_usage,
                            "aggregate": agg_type,
                            "usage": usage,
                            "datatype": datatype,
                            "cognosClass": "queryItem",
                            "source": "moserJSON"
                        }
                    )
                    
                    # Determine object type (treat attribute as dimension for analytics)
                    if data_usage == "measure" or agg_type != "none":
                        column_obj.object_type = ObjectType.MEASURE
                    elif data_usage in ("dimension", "attribute"):
                        column_obj.object_type = ObjectType.DIMENSION
                    else:
                        column_obj.object_type = ObjectType.COLUMN
                    
                    columns.append(column_obj)
                    
                    # Create HAS_COLUMN relationship
                    rel = self._create_relationship(
                        source_id=table_id,
                        target_id=column_id,
                        relationship_type=RelationshipType.HAS_COLUMN
                    )
                    relationships.append(rel)
            
            # Extract relationships (joins) between querySubjects (dedupe by pair)
            rels_data = moser_json.get("relationship", [])
            if not isinstance(rels_data, list):
                rels_data = [rels_data] if rels_data else []
            seen_join_pairs: Set[Tuple[str, str]] = set()
            for rel_data in rels_data:
                left_qs = rel_data.get("leftQuerySubject")
                right_qs = rel_data.get("rightQuerySubject")
                join_type = rel_data.get("cardinality", "inner")
                join_condition = rel_data.get("expression", "")
                
                if left_qs and right_qs:
                    left_table_id = f"{module_id}:table:{left_qs}"
                    right_table_id = f"{module_id}:table:{right_qs}"
                    pair = (min(left_table_id, right_table_id), max(left_table_id, right_table_id))
                    if pair in seen_join_pairs:
                        continue
                    seen_join_pairs.add(pair)
                    
                    rel = self._create_relationship(
                        source_id=left_table_id,
                        target_id=right_table_id,
                        relationship_type=RelationshipType.JOINS_TO,
                        properties={
                            "join_type": join_type,
                            "join_condition": join_condition,
                            "left_table": left_qs,
                            "right_table": right_qs,
                            "source": "moserJSON"
                        }
                    )
                    relationships.append(rel)
            
            # Extract data source connections from various possible locations (dedupe by ds_id)
            data_source_keys = ["dataSource", "dataSourceReference", "package", "packageReference", "connection"]
            seen_ds_ids: Set[str] = set()
            for key in data_source_keys:
                data_sources = moser_json.get(key, [])
                if not isinstance(data_sources, list):
                    data_sources = [data_sources] if data_sources else []
                
                for ds in data_sources:
                    ds_id = ds.get("id") or ds.get("storeID") or ds.get("identifier")
                    if ds_id and ds_id not in seen_ds_ids:
                        seen_ds_ids.add(ds_id)
                        # Create CONNECTS_TO relationship from module to data source
                        rel = self._create_relationship(
                            source_id=module_id,
                            target_id=ds_id,
                            relationship_type=RelationshipType.CONNECTS_TO,
                            properties={
                                "connection_type": ds.get("type", key),
                                "data_source_name": ds.get("name", ""),
                                "source_key": key,
                                "source_is_main_module": is_main_module,
                            },
                        )
                        relationships.append(rel)
            
            # Also check for data source references in other structures (dedupe)
            if "dataRetrievalMode" in moser_json:
                retrieval_mode = moser_json.get("dataRetrievalMode", {})
                if isinstance(retrieval_mode, dict):
                    ds_ref = retrieval_mode.get("dataSource") or retrieval_mode.get("package")
                    if ds_ref:
                        ds_id = ds_ref.get("id") or ds_ref.get("storeID") if isinstance(ds_ref, dict) else str(ds_ref)
                        if ds_id and ds_id not in seen_ds_ids:
                            seen_ds_ids.add(ds_id)
                            rel = self._create_relationship(
                                source_id=module_id,
                                target_id=ds_id,
                                relationship_type=RelationshipType.CONNECTS_TO,
                                properties={
                                    "connection_type": "data_retrieval",
                                    "source": "dataRetrievalMode",
                                    "source_is_main_module": is_main_module,
                                },
                            )
                            relationships.append(rel)
            
            # Extract calculations (embedded calculated fields) (dedupe by calc_id)
            # Always keep object_type as CALCULATED_FIELD; store data_usage in properties for measure/dimension use.
            calculations = moser_json.get("calculation", [])
            if not isinstance(calculations, list):
                calculations = [calculations] if calculations else []
            seen_calc_ids: Set[str] = set()
            for calc in calculations:
                calc_id = f"{module_id}:emb_calc:{calc.get('identifier', 'unknown')}"
                if calc_id in seen_calc_ids:
                    continue
                seen_calc_ids.add(calc_id)
                calc_name = calc.get("label", calc.get("identifier", "Unknown"))
                expression = calc.get("expression", "")
                usage = calc.get("usage", "")
                datatype = calc.get("datatype", "")
                aggregate = calc.get("regularAggregate", "")
                
                # Keep as CALCULATED_FIELD; data_usage in properties indicates use as measure/dimension
                obj_type = ObjectType.CALCULATED_FIELD
                
                # Map datatype to data_type for consistency
                data_type = "unknown"
                if datatype:
                    if isinstance(datatype, str) and datatype.isdigit():
                        data_type = DATA_TYPE_MAP.get(datatype, datatype)
                    else:
                        data_type = datatype
                
                # Map usage to data_usage (support string and numeric: 0=attr, 1=dim, 2=measure)
                usage_str = str(usage).strip() if usage is not None else ""
                data_usage = DATA_USAGE_MAP.get(usage_str, "unknown")
                if data_usage == "unknown" and usage_str:
                    if usage in ("fact", "measure") or usage == 2:
                        data_usage = "measure"
                    elif usage in ("attribute", "dimension") or usage in (0, 1):
                        data_usage = "dimension" if usage in (1, "dimension") else "attribute"
                
                # Map aggregate (support non-string from JSON)
                agg_type = "none"
                if aggregate is not None and aggregate != "":
                    agg_type = AGGREGATION_MAP.get(str(aggregate).lower(), str(aggregate) if aggregate else "none")
                
                calc_obj = self._create_object(
                    object_id=calc_id,
                    name=calc_name,
                    parent_id=module_id,
                    properties={
                        "expression": expression,
                        "usage": usage,  # Keep original for reference
                        "data_usage": data_usage,  # Standardized
                        "datatype": datatype,  # Keep original for reference
                        "data_type": data_type,  # Standardized
                        "aggregate": agg_type,  # Standardized
                        "regularAggregate": aggregate,  # Keep original for reference
                        "cognosClass": "embeddedCalculation",
                        "source": "moserJSON"
                    }
                )
                calc_obj.object_type = obj_type
                objects.append(calc_obj)
                
                # Create relationship
                rel = self._create_relationship(
                    source_id=module_id,
                    target_id=calc_id,
                    relationship_type=RelationshipType.CONTAINS
                )
                relationships.append(rel)
                
                # Create depends_on relationships for calculated fields
                if expression:
                    # Extract column references from expression
                    col_refs = re.findall(r'\[([^\]]+)\]\.\[([^\]]+)\]\.\[([^\]]+)\]', expression)
                    for ds, table, col in col_refs:
                        # Try to find the referenced column
                        ref_col_id = f"{module_id}:col:{table}.{col}"
                        dep_rel = self._create_relationship(
                            source_id=calc_id,
                            target_id=ref_col_id,
                            relationship_type=RelationshipType.DEPENDS_ON,
                            properties={
                                "dependency_type": "column_reference",
                                "referenced_path": f"{ds}.{table}.{col}"
                            }
                        )
                        relationships.append(dep_rel)
                
                # Also create aggregates relationship if this calculated field is used as a measure
                if data_usage == "measure" and aggregate:
                    # Try to find base columns from expression
                    if expression:
                        col_refs = re.findall(r'\[([^\]]+)\]\.\[([^\]]+)\]\.\[([^\]]+)\]', expression)
                        for ds, table, col in col_refs:
                            base_col_id = f"{module_id}:col:{table}.{col}"
                            agg_rel = self._create_relationship(
                                source_id=calc_id,
                                target_id=base_col_id,
                                relationship_type=RelationshipType.AGGREGATES,
                                properties={
                                    "aggregate_type": aggregate,
                                    "base_column": col
                                }
                            )
                            relationships.append(agg_rel)
            
        except Exception as e:
            logger.debug(f"Error parsing smartsData: {e}")
            errors.append(self._create_error(
                level="warning",
                message=f"Failed to parse smartsData: {str(e)}"
            ))
        
        return objects, relationships, errors, tables, columns

    def extract_from_specification(
        self,
        spec_xml: str,
        parent_id: str,
        parent_name: str = "",
        filter_scope: Optional[str] = None,
    ) -> Tuple[List[ExtractedObject], List[Relationship], List[ParseError]]:
        """
        Extract detailed column info from report/dashboard specification XML.
        
        This parses dataItem elements with full metadata including:
        - RS_dataType (data type)
        - RS_dataUsage (dimension/measure/attribute)
        - aggregate type
        - expression
        - joins between tables
        - hierarchies
        - filters (detailFilter, summaryFilter; expression or filterDefinition)
        
        Args:
            spec_xml: The decoded specification XML string
            parent_id: ID of the parent object (report/dashboard)
            parent_name: Name of the parent object
            filter_scope: "query_level" | "report_level" | "data_module" | "data_set" for filter classification
            
        Returns:
            Tuple of (objects, relationships, errors)
        """
        objects = []
        relationships = []
        errors = []
        
        try:
            root = XmlHandler.parse_string(spec_xml)
            
            # Extract dataItems (columns used in reports)
            columns, col_rels = self._extract_data_items(root, parent_id)
            objects.extend(columns)
            relationships.extend(col_rels)
            
            # Extract calculated fields
            calc_fields, calc_rels = self._extract_calculated_fields(root, parent_id)
            objects.extend(calc_fields)
            relationships.extend(calc_rels)
            
            # Extract filters (scope: "query_level" when from report spec, "data_module" when from module)
            filters, filter_rels = self._extract_filters(root, parent_id, filter_scope=filter_scope)
            objects.extend(filters)
            relationships.extend(filter_rels)
            
            # Extract parameters/prompts
            params, param_rels = self._extract_parameters(root, parent_id)
            objects.extend(params)
            relationships.extend(param_rels)
            
            # Extract joins between tables
            join_rels = self._extract_joins(root, parent_id)
            relationships.extend(join_rels)
            
            # Extract hierarchies
            hierarchies, hierarchy_rels = self._extract_hierarchies(root, parent_id)
            objects.extend(hierarchies)
            relationships.extend(hierarchy_rels)
            
            # Extract sorts
            sorts, sort_rels = self._extract_sorts(root, parent_id)
            objects.extend(sorts)
            relationships.extend(sort_rels)
            
            # Extract prompts
            prompts, prompt_rels = self._extract_prompts(root, parent_id)
            objects.extend(prompts)
            relationships.extend(prompt_rels)
            
        except Exception as e:
            errors.append(self._create_error(
                level="warning",
                message=f"Failed to parse specification: {str(e)}"
            ))
            
        return objects, relationships, errors

    def _extract_data_items(
        self,
        root: Any,
        parent_id: str
    ) -> Tuple[List[ExtractedObject], List[Relationship]]:
        """
        Extract dataItem elements with full metadata.
        
        Parses attributes:
        - name: column display name
        - aggregate: aggregation type (total, none, etc.)
        - rollupAggregate: rollup aggregation
        - expression: column expression like [DataSource].[Table].[Column]
        - RS_dataType: data type code
        - RS_dataUsage: usage code (0=attr, 1=dim, 2=measure)
        """
        columns = []
        relationships = []
        seen_columns: Set[str] = set()
        
        # Find all dataItem elements
        for data_item in root.iter():
            if not data_item.tag.endswith('dataItem'):
                continue
                
            name = data_item.get('name')
            if not name or name in seen_columns:
                continue
            seen_columns.add(name)
            
            # Get attributes
            aggregate = data_item.get('aggregate', 'none')
            rollup_aggregate = data_item.get('rollupAggregate', '')
            
            # Get expression
            expression = None
            expr_elem = data_item.find('.//{*}expression')
            if expr_elem is not None and expr_elem.text:
                expression = expr_elem.text.strip()
            
            # Skip dataItems that are calculated fields; they are emitted only by _extract_calculated_fields
            # to avoid double-extraction (same dataItem as both measure/dimension and calculated_field).
            if _expression_is_calculated_field(expression):
                continue
            
            # Parse RS_dataType and RS_dataUsage from XMLAttributes
            data_type = None
            data_usage = None
            
            for xml_attr in data_item.iter():
                if xml_attr.tag.endswith('XMLAttribute'):
                    attr_name = xml_attr.get('name', '')
                    attr_value = xml_attr.get('value', '')
                    
                    if attr_name == 'RS_dataType':
                        data_type = DATA_TYPE_MAP.get(attr_value, 'unknown')
                    elif attr_name == 'RS_dataUsage':
                        data_usage = DATA_USAGE_MAP.get(attr_value, 'unknown')
            
            # Parse expression to get table and column references
            table_name = None
            source_column = None
            if expression:
                match = re.match(r'\[([^\]]+)\]\.\[([^\]]+)\]\.\[([^\]]+)\]', expression)
                if match:
                    # [DataSource].[Table].[Column]
                    table_name = match.group(2)
                    source_column = match.group(3)
            
            # Determine object type based on usage
            # RS_dataUsage: 0=attribute, 1=dimension, 2=measure. Treat attribute as dimension for analytics.
            obj_type = ObjectType.COLUMN
            if data_usage == 'measure' or aggregate != 'none':
                obj_type = ObjectType.MEASURE
            elif data_usage in ('dimension', 'attribute'):
                obj_type = ObjectType.DIMENSION
            
            # Create column object
            column_id = f"{parent_id}:dataitem:{name}"
            column_obj = self._create_object(
                object_id=column_id,
                name=name,
                parent_id=parent_id,
                properties={
                    "expression": expression,
                    "data_type": data_type,
                    "data_usage": data_usage,
                    "aggregate": AGGREGATION_MAP.get(aggregate, aggregate),
                    "rollup_aggregate": rollup_aggregate,
                    "source_table": table_name,
                    "source_column": source_column,
                    "cognosClass": "dataItem"
                }
            )
            column_obj.object_type = obj_type
            columns.append(column_obj)
            
            # Create relationship
            rel = self._create_relationship(
                source_id=parent_id,
                target_id=column_id,
                relationship_type=RelationshipType.CONTAINS
            )
            relationships.append(rel)
            
            # If this is a measure with aggregation, create aggregates relationship
            if obj_type == ObjectType.MEASURE and aggregate != 'none' and source_column:
                # Try to find the base column this measure aggregates
                base_col_id = f"{parent_id}:col:{table_name}.{source_column}"
                agg_rel = self._create_relationship(
                    source_id=column_id,
                    target_id=base_col_id,
                    relationship_type=RelationshipType.AGGREGATES,
                    properties={
                        "aggregate_type": AGGREGATION_MAP.get(aggregate, aggregate),
                        "base_column": source_column
                    }
                )
                relationships.append(agg_rel)
        
        return columns, relationships

    def _extract_calculated_fields(
        self,
        root: Any,
        parent_id: str
    ) -> Tuple[List[ExtractedObject], List[Relationship]]:
        """
        Extract calculated fields (dataItems with actual calculation expressions).
        
        Identifies calculated fields by expression patterns only; excludes simple
        aggregates (SUM/COUNT/AVG on a single column) so those stay as measures.
        Includes: CASE WHEN, IF(, COALESCE(, cast(, arithmetic, current_date, etc.
        """
        calc_fields = []
        relationships = []
        seen_fields: Set[str] = set()
        
        for data_item in root.iter():
            if not data_item.tag.endswith('dataItem'):
                continue
                
            name = data_item.get('name')
            if not name or name in seen_fields:
                continue
            
            # Get expression
            expr_elem = data_item.find('.//{*}expression')
            if expr_elem is None or not expr_elem.text:
                continue
                
            expression = expr_elem.text.strip()
            
            # Only emit calculated fields (same rule as _extract_data_items skip to avoid duplication)
            if not _expression_is_calculated_field(expression):
                continue
            
            seen_fields.add(name)
            
            # Determine the type of calculation
            calc_type = "expression"
            if re.search(r'CASE\s+WHEN', expression, re.IGNORECASE):
                calc_type = "case_expression"
            elif re.search(r'\bIF\s*\(', expression, re.IGNORECASE):
                calc_type = "if_expression"
            elif re.search(r'\b(minimum|maximum|abs)\s*\(', expression, re.IGNORECASE):
                calc_type = "function"
            elif re.search(r'\b(SUM|COUNT|AVG)\s*\(', expression, re.IGNORECASE):
                calc_type = "aggregate_function"
            
            # Create calculated field object
            field_id = f"{parent_id}:calc:{name}"
            field_obj = self._create_object(
                object_id=field_id,
                name=name,
                parent_id=parent_id,
                properties={
                    "expression": expression,
                    "calculation_type": calc_type,
                    "cognosClass": "calculatedField"
                }
            )
            field_obj.object_type = ObjectType.CALCULATED_FIELD
            calc_fields.append(field_obj)
            
            # Create relationship
            rel = self._create_relationship(
                source_id=parent_id,
                target_id=field_id,
                relationship_type=RelationshipType.CONTAINS
            )
            relationships.append(rel)
            
            # Create depends_on relationships for calculated fields
            if expression:
                # Extract column references from expression
                col_refs = re.findall(r'\[([^\]]+)\]\.\[([^\]]+)\]\.\[([^\]]+)\]', expression)
                for ds, table, col in col_refs:
                    # Try to find the referenced column
                    ref_col_id = f"{parent_id}:col:{table}.{col}"
                    dep_rel = self._create_relationship(
                        source_id=field_id,
                        target_id=ref_col_id,
                        relationship_type=RelationshipType.DEPENDS_ON,
                        properties={
                            "dependency_type": "column_reference",
                            "referenced_path": f"{ds}.{table}.{col}"
                        }
                    )
                    relationships.append(dep_rel)
        
        return calc_fields, relationships

    def _extract_filters(
        self,
        root: Any,
        parent_id: str,
        filter_scope: Optional[str] = None,
    ) -> Tuple[List[ExtractedObject], List[Relationship]]:
        """
        Extract filter definitions (detailFilter, summaryFilter).
        Supports both filterExpression (expression-based) and filterDefinition (e.g. filterInValues).
        Classifies: filter_type (detail/summary), filter_style (expression/definition),
        filter_scope (query_level/report_level/data_module/data_set), is_simple/is_complex.
        """
        filters = []
        relationships = []
        filter_idx = 0

        for filter_elem in root.iter():
            filter_type = None
            if filter_elem.tag.endswith('detailFilter'):
                filter_type = "detail"
            elif filter_elem.tag.endswith('summaryFilter'):
                filter_type = "summary"
            else:
                continue

            filter_idx += 1
            filter_id = f"{parent_id}:filter:{filter_idx}"
            filter_name = f"Filter_{filter_idx}"
            expression = None
            referenced_columns: List[str] = []
            param_refs: List[str] = []
            filter_style = "unknown"
            ref_data_item = None
            filter_definition_summary = None
            post_auto_agg = filter_elem.get("postAutoAggregation", "")

            # 1) filterExpression (expression-based filter)
            expr_elem = filter_elem.find('.//{*}filterExpression')
            if expr_elem is not None and expr_elem.text:
                expression = expr_elem.text.strip()
                referenced_columns = re.findall(r'\[([^\]]+)\]', expression)
                param_refs = re.findall(r'\?([^?]+)\?', expression)
                filter_style = "expression"
                # Simple = short expression, no param refs; complex = param refs or long expression
                is_simple = len(param_refs) == 0 and len(expression) < 120
                is_complex = not is_simple
            else:
                # 2) filterDefinition (e.g. filterInValues, slicer member set)
                def_elem = filter_elem.find('.//{*}filterDefinition')
                if def_elem is not None:
                    filter_style = "definition"
                    in_vals = def_elem.find('.//{*}filterInValues')
                    if in_vals is not None:
                        ref_data_item = in_vals.get("refDataItem")
                        data_type = in_vals.get("dataType", "")
                        vals = in_vals.findall('.//{*}filterValue')
                        val_texts = [v.text.strip() for v in vals if v.text]
                        filter_definition_summary = f"filterInValues refDataItem={ref_data_item or '?'} dataType={data_type} values={len(val_texts)}"
                        expression = f"[{ref_data_item or '?'}] in ({', '.join(val_texts[:5])}{'...' if len(val_texts) > 5 else ''})"
                    else:
                        filter_definition_summary = "filterDefinition (other)"
                    is_simple = True
                    is_complex = False
                else:
                    is_simple = False
                    is_complex = False

            props: Dict[str, Any] = {
                "filter_type": filter_type,
                "filter_style": filter_style,
                "filter_scope": filter_scope,
                "referenced_columns": referenced_columns,
                "parameter_references": param_refs,
                "is_simple": is_simple,
                "is_complex": is_complex,
                "cognosClass": "filter",
            }
            if expression:
                props["expression"] = expression
            if ref_data_item:
                props["ref_data_item"] = ref_data_item
            if filter_definition_summary:
                props["filter_definition_summary"] = filter_definition_summary
            if post_auto_agg:
                props["postAutoAggregation"] = post_auto_agg

            filter_obj = self._create_object(
                object_id=filter_id,
                name=filter_name,
                parent_id=parent_id,
                properties=props,
            )
            filter_obj.object_type = ObjectType.FILTER
            filters.append(filter_obj)

            rel = self._create_relationship(
                source_id=parent_id,
                target_id=filter_id,
                relationship_type=RelationshipType.FILTERS_BY
            )
            relationships.append(rel)

        return filters, relationships

    def _extract_parameters(
        self,
        root: Any,
        parent_id: str
    ) -> Tuple[List[ExtractedObject], List[Relationship]]:
        """
        Extract parameter and prompt definitions.
        """
        params = []
        relationships = []
        seen_params: Set[str] = set()
        
        # Find parameter references in filter expressions
        for filter_elem in root.iter():
            if filter_elem.tag.endswith('filterExpression') and filter_elem.text:
                # Extract ?ParameterName? patterns
                param_refs = re.findall(r'\?([^?]+)\?', filter_elem.text)
                for param_name in param_refs:
                    if param_name in seen_params:
                        continue
                    seen_params.add(param_name)
                    
                    param_id = f"{parent_id}:param:{param_name}"
                    param_obj = self._create_object(
                        object_id=param_id,
                        name=param_name,
                        parent_id=parent_id,
                        properties={
                            "parameter_type": "filter_parameter",
                            "cognosClass": "parameter"
                        }
                    )
                    param_obj.object_type = ObjectType.PARAMETER
                    params.append(param_obj)
                    
                    rel = self._create_relationship(
                        source_id=parent_id,
                        target_id=param_id,
                        relationship_type=RelationshipType.USES_PARAMETER
                    )
                    relationships.append(rel)
        
        # Find reportVariable elements
        for var_elem in root.iter():
            if not var_elem.tag.endswith('reportVariable'):
                continue
            
            var_name = var_elem.get('name')
            var_type = var_elem.get('type')
            
            if not var_name or var_name in seen_params:
                continue
            seen_params.add(var_name)
            
            var_id = f"{parent_id}:var:{var_name}"
            var_obj = self._create_object(
                object_id=var_id,
                name=var_name,
                parent_id=parent_id,
                properties={
                    "variable_type": var_type,
                    "cognosClass": "reportVariable"
                }
            )
            var_obj.object_type = ObjectType.PARAMETER
            params.append(var_obj)
            
            rel = self._create_relationship(
                source_id=parent_id,
                target_id=var_id,
                relationship_type=RelationshipType.USES_PARAMETER
            )
            relationships.append(rel)
        
        return params, relationships

    def _extract_joins(
        self,
        root: Any,
        parent_id: str
    ) -> List[Relationship]:
        """
        Extract join relationships between tables/queries.
        
        Parses joinOperation elements with:
        - joinOperands (participating tables/queries)
        - joinFilter (join conditions)
        - join type (inner, left outer, etc.)
        """
        relationships = []
        seen_joins: Set[str] = set()
        
        # Find all joinOperation elements
        for join_elem in root.iter():
            if not join_elem.tag.endswith('joinOperation'):
                continue
            
            # Get join operands
            operands = []
            for operand in join_elem.iter():
                if operand.tag.endswith('joinOperand'):
                    query_ref = operand.find('.//{*}queryRef')
                    if query_ref is not None:
                        ref_query = query_ref.get('refQuery', '')
                        if ref_query:
                            operands.append(ref_query)
            
            # Create relationships between operands
            if len(operands) >= 2:
                # Get join type if available
                join_type = "inner"  # default
                if join_elem.get('type'):
                    join_type = join_elem.get('type')
                
                # Get join filter/condition
                join_filter = None
                filter_elem = join_elem.find('.//{*}joinFilter')
                if filter_elem is not None and filter_elem.text:
                    join_filter = filter_elem.text.strip()
                
                # Create relationship from first to second operand
                left_table = operands[0]
                right_table = operands[1]
                join_key = f"{left_table}:{right_table}"
                
                if join_key not in seen_joins:
                    seen_joins.add(join_key)
                    
                    # Create query IDs based on parent
                    left_id = f"{parent_id}:query:{left_table}"
                    right_id = f"{parent_id}:query:{right_table}"
                    
                    rel = self._create_relationship(
                        source_id=left_id,
                        target_id=right_id,
                        relationship_type=RelationshipType.JOINS_TO,
                        properties={
                            "join_type": join_type,
                            "join_condition": join_filter,
                            "left_table": left_table,
                            "right_table": right_table
                        }
                    )
                    relationships.append(rel)
        
        return relationships

    def _extract_hierarchies(
        self,
        root: Any,
        parent_id: str
    ) -> Tuple[List[ExtractedObject], List[Relationship]]:
        """
        Extract hierarchy definitions with levels.
        
        Parses hierarchy elements with:
        - name
        - levels with level names and expressions
        - member paths
        """
        hierarchies = []
        relationships = []
        seen_hierarchies: Set[str] = set()
        
        # Find all hierarchy elements
        for hier_elem in root.iter():
            if not hier_elem.tag.endswith('hierarchy'):
                continue
            
            hier_name = hier_elem.get('name')
            if not hier_name or hier_name in seen_hierarchies:
                continue
            seen_hierarchies.add(hier_name)
            
            # Extract levels
            levels = []
            for level_elem in hier_elem.iter():
                if level_elem.tag.endswith('level'):
                    level_name = level_elem.get('name', '')
                    if level_name:
                        levels.append({
                            "name": level_name,
                            "caption": level_elem.get('caption', level_name)
                        })
            
            # Create hierarchy object
            hier_id = f"{parent_id}:hierarchy:{hier_name}"
            hier_obj = self._create_object(
                object_id=hier_id,
                name=hier_name,
                parent_id=parent_id,
                properties={
                    "levels": levels,
                    "level_count": len(levels),
                    "cognosClass": "hierarchy"
                }
            )
            hier_obj.object_type = ObjectType.HIERARCHY
            hierarchies.append(hier_obj)
            
            # Create CONTAINS relationship
            rel = self._create_relationship(
                source_id=parent_id,
                target_id=hier_id,
                relationship_type=RelationshipType.CONTAINS
            )
            relationships.append(rel)
        
        return hierarchies, relationships
    
    def _extract_sorts(
        self,
        root: Any,
        parent_id: str
    ) -> Tuple[List[ExtractedObject], List[Relationship]]:
        """
        Extract sort definitions.
        
        Parses sort elements with:
        - sort direction (ascending, descending)
        - sorted column reference
        """
        sorts = []
        relationships = []
        seen_sorts: Set[str] = set()
        sort_idx = 0
        
        # Find all sort elements (try multiple tag patterns)
        for elem in root.iter():
            tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
            
            # Check for sort or sortItem elements
            if tag_name not in ['sort', 'sortItem']:
                continue
            
            sort_idx += 1
            sort_name = elem.get('name', '') or f"Sort_{sort_idx}"
            
            # Create unique key
            sort_key = f"{sort_name}:{sort_idx}"
            if sort_key in seen_sorts:
                continue
            seen_sorts.add(sort_key)
            
            # Get sort direction
            direction = elem.get('direction', elem.get('sortDirection', 'ascending'))
            
            # Get sorted column reference (try multiple patterns)
            sorted_column = None
            col_ref_elem = elem.find('.//{*}dataItemRef')
            if col_ref_elem is not None:
                sorted_column = col_ref_elem.get('refDataItem', '')
            else:
                # Try direct attribute
                sorted_column = elem.get('refDataItem') or elem.get('dataItem')
            
            # Also check for nested sortItem elements
            sort_items = []
            for sort_item in elem.findall('.//{*}sortItem'):
                item_col = sort_item.get('refDataItem') or sort_item.get('dataItem')
                item_dir = sort_item.get('direction', 'ascending')
                if item_col:
                    sort_items.append({
                        "column": item_col,
                        "direction": item_dir
                    })
            
            # Create sort object
            sort_id = f"{parent_id}:sort:{sort_idx}"
            sort_obj = self._create_object(
                object_id=sort_id,
                name=sort_name,
                parent_id=parent_id,
                properties={
                    "direction": direction,
                    "sorted_column": sorted_column,
                    "sort_items": sort_items if sort_items else None,
                    "cognosClass": "sort"
                }
            )
            sort_obj.object_type = ObjectType.SORT
            sorts.append(sort_obj)
            
            # Create CONTAINS relationship
            rel = self._create_relationship(
                source_id=parent_id,
                target_id=sort_id,
                relationship_type=RelationshipType.CONTAINS
            )
            relationships.append(rel)
            
            # Create REFERENCES relationship to sorted column
            if sorted_column:
                col_id = f"{parent_id}:dataitem:{sorted_column}"
                ref_rel = self._create_relationship(
                    source_id=sort_id,
                    target_id=col_id,
                    relationship_type=RelationshipType.REFERENCES,
                    properties={
                        "reference_type": "sorts_by"
                    }
                )
                relationships.append(ref_rel)
            
            # Create relationships for sort items
            for item in sort_items:
                if item.get("column"):
                    col_id = f"{parent_id}:dataitem:{item['column']}"
                    ref_rel = self._create_relationship(
                        source_id=sort_id,
                        target_id=col_id,
                        relationship_type=RelationshipType.REFERENCES,
                        properties={
                            "reference_type": "sorts_by",
                            "direction": item.get("direction")
                        }
                    )
                    relationships.append(ref_rel)
        
        return sorts, relationships
    
    def _extract_prompts(
        self,
        root: Any,
        parent_id: str
    ) -> Tuple[List[ExtractedObject], List[Relationship]]:
        """
        Extract prompt definitions (different from parameters).
        
        Prompts are user-facing input controls, while parameters are internal variables.
        """
        prompts = []
        relationships = []
        seen_prompts: Set[str] = set()
        
        # Find prompt elements
        for prompt_elem in root.iter():
            if not prompt_elem.tag.endswith('prompt'):
                continue
            
            prompt_name = prompt_elem.get('name', '')
            if not prompt_name or prompt_name in seen_prompts:
                continue
            seen_prompts.add(prompt_name)
            
            # Get prompt type
            prompt_type = prompt_elem.get('type', 'text')
            
            # Get prompt value/expression
            prompt_value = None
            value_elem = prompt_elem.find('.//{*}value')
            if value_elem is not None and value_elem.text:
                prompt_value = value_elem.text.strip()
            
            # Create prompt object
            prompt_id = f"{parent_id}:prompt:{prompt_name}"
            prompt_obj = self._create_object(
                object_id=prompt_id,
                name=prompt_name,
                parent_id=parent_id,
                properties={
                    "prompt_type": prompt_type,
                    "value": prompt_value,
                    "cognosClass": "prompt"
                }
            )
            prompt_obj.object_type = ObjectType.PROMPT
            prompts.append(prompt_obj)
            
            # Create CONTAINS relationship
            rel = self._create_relationship(
                source_id=parent_id,
                target_id=prompt_id,
                relationship_type=RelationshipType.CONTAINS
            )
            relationships.append(rel)
        
        return prompts, relationships
