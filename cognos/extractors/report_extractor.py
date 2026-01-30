"""
Report extractor for Cognos.
"""
from typing import List, Any, Dict, Optional, Set
from datetime import datetime
import logging

from ...core import BaseExtractor, ExtractedObject, Relationship, ParseError, ObjectType, RelationshipType
from ...core.handlers import XmlHandler


class ReportExtractor(BaseExtractor):
    """Extractor for Cognos report objects."""
    
    def __init__(self):
        super().__init__(bi_tool="cognos")
    
    @property
    def object_type(self) -> str:
        return ObjectType.REPORT.value
    
    def extract(
        self,
        source: Any
    ) -> tuple[List[ExtractedObject], List[Relationship], List[ParseError]]:
        """
        Extract report objects from XML element.
        
        Args:
            source: XML element containing report data
        
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
            # Report type from Cognos class: report, interactiveReport, reportView, dataSet2, reportVersion
            cognos_report_class = XmlHandler.get_text(source, "class", default="report").strip() or "report"

            # Get properties
            props_elem = source.find("props")
            properties = {
                "storeID": store_id,
                "cognosClass": cognos_report_class,
                "reportType": cognos_report_class,  # frontend uses reportType / report_type
                "report_type": cognos_report_class,
            }
            
            created_at = None
            modified_at = None
            owner = None
            specification_xml = None
            
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
                
                # Extract report-specific properties
                hidden = XmlHandler.get_text(props_elem, "hidden/value")
                if hidden:
                    properties["hidden"] = hidden.lower() == "true"

                # Execution prompt (report has prompt pages)
                execution_prompt = XmlHandler.get_text(props_elem, "executionPrompt/value")
                if execution_prompt:
                    properties["executionPrompt"] = execution_prompt.lower() == "true"

                # Display and run options
                display_seq = XmlHandler.get_text(props_elem, "displaySequence/value")
                if display_seq is not None:
                    try:
                        properties["displaySequence"] = int(display_seq)
                    except (ValueError, TypeError):
                        pass
                viewed = XmlHandler.get_text(props_elem, "viewed/value")
                if viewed:
                    properties["viewed"] = viewed.lower() == "true"
                for prop_name, key in (
                    ("allowNotification", "allowNotification/value"),
                    ("allowSubscription", "allowSubscription/value"),
                    ("canBurst", "canBurst/value"),
                    ("runInAdvancedViewer", "runInAdvancedViewer/value"),
                ):
                    val = XmlHandler.get_text(props_elem, key)
                    if val:
                        properties[prop_name] = val.lower() == "true"

                # Extract specification for deeper analysis
                spec_elem = props_elem.find("specification/value")
                if spec_elem is not None and spec_elem.text:
                    specification_xml = spec_elem.text
                    # Check if it's base64 encoded
                    import base64
                    try:
                        # Try to decode if it looks like base64
                        if len(specification_xml) > 100 and not specification_xml.strip().startswith('<'):
                            try:
                                decoded = base64.b64decode(specification_xml).decode('utf-8')
                                if decoded.strip().startswith('<'):
                                    specification_xml = decoded
                            except:
                                pass  # Not base64, use as-is
                    except:
                        pass
            
            # Create report object
            report = self._create_object(
                object_id=obj_id,
                name=name,
                parent_id=parent_id,
                properties=properties,
                created_at=created_at,
                modified_at=modified_at,
                owner=owner,
            )
            
            objects.append(report)
            
            # Create parent-child relationship
            if parent_id:
                rel = self._create_relationship(
                    source_id=parent_id,
                    target_id=obj_id,
                    relationship_type=RelationshipType.PARENT_CHILD
                )
                relationships.append(rel)

            # Create USES/REFERENCES from report props (metadataModel, module, paths, deploymentReferences)
            if props_elem is not None:
                for ref_store_id, rel_type, prop_kind in self._collect_report_references(props_elem):
                    if ref_store_id and ref_store_id != obj_id:
                        rel = self._create_relationship(
                            source_id=obj_id,
                            target_id=ref_store_id,
                            relationship_type=rel_type,
                            properties={"source_prop": prop_kind}
                        )
                        relationships.append(rel)
            
            # Deep extraction from specification
            if specification_xml:
                spec_objects, spec_rels, spec_errors = self._extract_specification(
                    specification_xml,
                    report_id=obj_id,
                    report_name=name,
                    props_elem=props_elem,
                    report_object=report,
                )
                objects.extend(spec_objects)
                relationships.extend(spec_rels)
                errors.extend(spec_errors)
            
        except Exception as e:
            error = self._create_error(
                level="error",
                message=f"Failed to extract report: {str(e)}"
            )
            errors.append(error)
        
        self._log_extraction(len(objects), len(errors))
        return objects, relationships, errors

    def _collect_report_references(
        self, props_elem: Any
    ) -> List[tuple[str, RelationshipType, str]]:
        """
        Collect storeIDs from report props for USES/REFERENCES relationships.
        Yields (store_id, rel_type, prop_kind) for metadataModel, metadataModelPackage,
        module (USES), and paths, deploymentReferences (REFERENCES).
        """
        result: List[tuple[str, RelationshipType, str]] = []
        seen: Set[str] = set()

        def add_store_id(store_id: Optional[str], rel_type: RelationshipType, kind: str) -> None:
            if store_id and store_id.strip() and store_id not in seen:
                seen.add(store_id)
                result.append((store_id.strip(), rel_type, kind))

        # USES: metadataModel, metadataModelPackage, module
        for prop_name in ("metadataModel", "metadataModelPackage", "module"):
            val_cont = props_elem.find(f"{prop_name}/value")
            if val_cont is None:
                continue
            for item in val_cont.findall(".//item"):
                sid = XmlHandler.get_text(item, "storeID/value") or XmlHandler.get_text(item, ".//storeID/value")
                if sid:
                    add_store_id(sid, RelationshipType.USES, prop_name)

        # REFERENCES: paths (style/template report), deploymentReferences
        for prop_name in ("paths", "deploymentReferences"):
            val_cont = props_elem.find(f"{prop_name}/value")
            if val_cont is None:
                continue
            for item in val_cont.findall(".//item"):
                # target (paths) or objects (deploymentReferences) contain refs
                container = item.find("target") or item.find("objects")
                if container is None:
                    continue
                for sub in container.findall(".//item"):
                    sid = XmlHandler.get_text(sub, "storeID/value") or XmlHandler.get_text(sub, ".//storeID/value")
                    if sid:
                        add_store_id(sid, RelationshipType.REFERENCES, prop_name)

        return result

    def _extract_specification(
        self,
        xml_content: str,
        report_id: str,
        report_name: str,
        props_elem: Any = None,
        report_object: Optional[ExtractedObject] = None,
    ) -> tuple[List[ExtractedObject], List[Relationship], List[ParseError]]:
        """
        Extract details from report specification XML.

        Finds:
        1. Data Source / Package references (modelPath)
        2. Queries (internal objects)
        3. DataItems with full metadata (type, usage, aggregation)
        4. Calculated fields
        5. Filters
        6. Parameters
        7. reportDataStores (for lineage: report → query → data items)
        """
        objects = []
        relationships = []
        errors = []
        
        try:
            root = XmlHandler.parse_string(xml_content)
            
            # 1. Find Data Sources (relationships to external objects)
            found_model_paths = set()
            seen_ref_ids: Set[str] = set()  # avoid duplicate REFERENCES to same target
            for elem in root.iter():
                if elem.tag.endswith('modelPath') and elem.text:
                    found_model_paths.add(elem.text)
                
                # Also check for references to other reports/objects (dedupe by target)
                if elem.tag.endswith('reportRef') or elem.tag.endswith('objectRef'):
                    ref_id = elem.get('refReport') or elem.get('refObject') or elem.get('ref')
                    if ref_id and ref_id not in seen_ref_ids:
                        seen_ref_ids.add(ref_id)
                        rel = self._create_relationship(
                            source_id=report_id,
                            target_id=ref_id,
                            relationship_type=RelationshipType.REFERENCES,
                            properties={
                                "reference_type": "report_reference",
                                "ref_element": elem.tag.split('}')[-1]
                            }
                        )
                        relationships.append(rel)

            for path in found_model_paths:
                # Check if path is a storeID (numeric) or a model path (string)
                # If it's a storeID, create CONNECTS_TO relationship
                # If it's a model path, create USES relationship
                if path and path.strip():
                    # Try to determine if it's a storeID (all digits) or path
                    if path.isdigit() or (path.startswith('/') and len(path) > 10):
                        # Likely a storeID or full path - try CONNECTS_TO first
                        rel = self._create_relationship(
                            source_id=report_id,
                            target_id=path,
                            relationship_type=RelationshipType.CONNECTS_TO,
                            properties={"dependency_type": "data_source", "path": path}
                        )
                        relationships.append(rel)
                    else:
                        # Model path - create USES relationship
                        rel = self._create_relationship(
                            source_id=report_id,
                            target_id=path,
                            relationship_type=RelationshipType.USES,
                            properties={"dependency_type": "data_source", "path": path}
                        )
                        relationships.append(rel)

            # 2. Find Internal Queries (dedupe by query_id so same name = one object)
            seen_query_ids: Set[str] = set()
            for query_elem in root.findall(".//{*}query"):
                query_name = query_elem.get("name")
                if not query_name:
                    continue
                query_id = f"{report_id}:query:{query_name}"
                if query_id in seen_query_ids:
                    continue
                seen_query_ids.add(query_id)
                
                # Determine query source
                source_type = "unknown"
                sql_content = None
                
                source_elem = query_elem.find(".//{*}source")
                if source_elem is not None:
                    if source_elem.find(".//{*}model") is not None:
                        source_type = "model"
                    elif source_elem.find(".//{*}queryRef") is not None:
                        source_type = "query_ref"
                    elif source_elem.find(".//{*}sqlQuery") is not None:
                        source_type = "sql"
                        sql_elem = source_elem.find(".//{*}sqlQuery")
                        sql_text_elem = sql_elem.find(".//{*}sqlText")
                        if sql_text_elem is not None:
                            sql_content = sql_text_elem.text

                # Create Query Object
                query_obj = self._create_object(
                    object_id=query_id,
                    name=query_name,
                    parent_id=report_id,
                    properties={
                        "source_type": source_type,
                        "sql_content": sql_content,
                        "cognosClass": "query"
                    }
                )
                query_obj.object_type = ObjectType.QUERY
                
                objects.append(query_obj)
                
                rel = self._create_relationship(
                    source_id=report_id,
                    target_id=query_id,
                    relationship_type=RelationshipType.CONTAINS
                )
                relationships.append(rel)
            
            # 3. Use DataModuleExtractor for deep extraction of dataItems, filters, params
            from .data_module_extractor import DataModuleExtractor
            dm_extractor = DataModuleExtractor()
            
            spec_objects, spec_rels, spec_errors = dm_extractor.extract_from_specification(
                xml_content, report_id, report_name, filter_scope="query_level"
            )
            objects.extend(spec_objects)
            relationships.extend(spec_rels)
            errors.extend(spec_errors)

            # 4. Extract Visualizations
            viz_objects, viz_rels, viz_errors = self._extract_visualizations(root, report_id)
            objects.extend(viz_objects)
            relationships.extend(viz_rels)
            errors.extend(viz_errors)
            
            # 5. Extract drill-through references
            drill_through_rels = self._extract_drill_throughs(root, report_id)
            relationships.extend(drill_through_rels)
            
            # 6. Extract sub-report references
            sub_report_rels = self._extract_sub_reports(root, report_id)
            relationships.extend(sub_report_rels)
            
            # 7. Extract pages from report specification
            pages, page_rels = self._extract_pages(root, report_id)
            objects.extend(pages)
            relationships.extend(page_rels)
            
            # 8. Extract hierarchies from queries
            hierarchies, hierarchy_rels = self._extract_hierarchies(root, report_id)
            objects.extend(hierarchies)
            relationships.extend(hierarchy_rels)
            
            # 9. Extract outputs (output formats)
            outputs, output_rels = self._extract_outputs(root, report_id, props_elem)
            objects.extend(outputs)
            relationships.extend(output_rels)
            
            # 10. Extract prompts
            prompts, prompt_rels = self._extract_prompts(root, report_id)
            objects.extend(prompts)
            relationships.extend(prompt_rels)

            # 11. Extract sort definitions (sortList/sortItem)
            sorts, sort_rels = self._extract_sorts(root, report_id)
            objects.extend(sorts)
            relationships.extend(sort_rels)

            # 12. Extract reportDataStores for lineage (report → query → data items)
            data_store_rels, store_list = self._extract_report_data_stores(
                root, report_id, report_object=report_object
            )
            relationships.extend(data_store_rels)
            if report_object is not None and store_list:
                report_object.properties["reportDataStores"] = store_list

        except Exception as e:
            errors.append(self._create_error(
                level="warning",
                message=f"Failed to parse report specification: {str(e)}"
            ))
            
        return objects, relationships, errors

    def _extract_visualizations(
        self,
        root: Any,
        report_id: str
    ) -> tuple[List[ExtractedObject], List[Relationship], List[ParseError]]:
        """
        Extract visualization details from report layout.
        
        Handles:
        1. Modern <visualization> elements with type attribute
        2. Legacy <chart> elements with chartType attribute
        3. Legacy containers: <list>, <crosstab>, <repeater>, etc.
        """
        objects = []
        relationships = []
        errors = []
        
        # Import centralized mapping
        from ..visualization_types import (
            map_dashboard_visid_to_type,
            map_report_element_to_type,
            CHART_TYPE_ATTR_MAP
        )
        
        # Track unique viz IDs to avoid duplicates
        seen_viz_ids = set()
        
        def create_viz_object(viz_id, viz_name, viz_type, query_ref, cognos_class, raw_type=None):
            """Helper to create visualization object and relationships."""
            if viz_id in seen_viz_ids:
                return
            seen_viz_ids.add(viz_id)
            
            viz_obj = self._create_object(
                object_id=viz_id,
                name=viz_name,
                parent_id=report_id,
                properties={
                    "cognosClass": cognos_class,
                    "visualization_type": viz_type,
                    "raw_type": raw_type or cognos_class,
                    "query_ref": query_ref
                }
            )
            viz_obj.object_type = ObjectType.VISUALIZATION
            objects.append(viz_obj)
            
            # Link to Report (CONTAINS relationship)
            relationships.append(self._create_relationship(
                source_id=report_id,
                target_id=viz_id,
                relationship_type=RelationshipType.CONTAINS
            ))
            
            # Link to Query (USES relationship)
            if query_ref:
                query_id = f"{report_id}:query:{query_ref}"
                relationships.append(self._create_relationship(
                    source_id=viz_id,
                    target_id=query_id,
                    relationship_type=RelationshipType.USES,
                    properties={"dependency_type": "data_source"}
                ))

        # 1. Modern Visualizations (<visualization> elements)
        for i, viz_elem in enumerate(root.findall(".//{*}visualization")):
            viz_name = viz_elem.get("name") or f"Visualization {i+1}"
            viz_id = f"{report_id}:viz:{i}"
            raw_type = viz_elem.get("type", "")
            
            # Determine visualization type
            if raw_type:
                viz_type = map_dashboard_visid_to_type(raw_type)
            else:
                # No type attribute - check for specific attributes or nested elements
                if viz_elem.get("refQuery"):
                    viz_type = "Data Visualization"
                else:
                    viz_type = "Visualization"
            
            query_ref = viz_elem.get("refQuery")
            
            create_viz_object(viz_id, viz_name, viz_type, query_ref, "visualization", raw_type)


        # 2. Chart Elements (<chart> with chartType attribute)
        for i, chart_elem in enumerate(root.findall(".//{*}chart")):
            chart_name = chart_elem.get("name") or f"Chart {i+1}"
            viz_id = f"{report_id}:chart:{i}"
            
            # Get chart type from attribute
            chart_type_attr = chart_elem.get("chartType", "")
            if chart_type_attr:
                viz_type = map_report_element_to_type("chart", chart_type_attr)
            else:
                # Try to determine from nested elements
                if chart_elem.find(".//{*}barChart") is not None:
                    viz_type = "Bar Chart"
                elif chart_elem.find(".//{*}lineChart") is not None:
                    viz_type = "Line Chart"
                elif chart_elem.find(".//{*}areaChart") is not None:
                    viz_type = "Area Chart"
                elif chart_elem.find(".//{*}pieChart") is not None:
                    viz_type = "Pie Chart"
                else:
                    viz_type = "Chart"
            
            query_ref = chart_elem.get("refQuery")
            create_viz_object(viz_id, chart_name, viz_type, query_ref, "chart", chart_type_attr)

        # 3. Legacy Containers and Specific Chart Types
        legacy_elements = {
            # Tables and Lists
            "list": "List",
            "crosstab": "CrossTab",
            "repeater": "Repeater",
            "repeaterTable": "Repeater Table",
            "table": "Table",
            "dataTable": "Data Table",
            "singleton": "Singleton",
            
            # Specific Chart Types
            "combinationChart": "Combination Chart",
            "pieChart": "Pie",
            "scatterChart": "Scatter",
            "bubbleChart": "Bubble",
            "gaugeChart": "Gauge",
            "radarChart": "Radar",
            "metricsChart": "Metrics Chart",
            "progressiveChart": "Progressive Chart",
            
            # Maps
            "map": "Legacy Map",
            "mapChart": "Map",
        }
        
        for tag, type_name in legacy_elements.items():
            for i, elem in enumerate(root.findall(f".//{{*}}{tag}")):
                viz_name = elem.get("name") or f"{type_name} {i+1}"
                viz_id = f"{report_id}:{tag}:{i}"
                query_ref = elem.get("refQuery")
                
                create_viz_object(viz_id, viz_name, type_name, query_ref, tag)
                    
        return objects, relationships, errors
    
    def _extract_drill_throughs(
        self,
        root: Any,
        report_id: str
    ) -> List[Relationship]:
        """
        Extract drill-through references to other reports/objects.
        
        Finds drillThrough elements that reference other reports.
        """
        relationships = []
        
        # Find drill-through elements
        for elem in root.iter():
            tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
            
            if tag_name in ['drillThrough', 'drillThroughRef', 'drillThroughAction']:
                # Get target report reference
                target_ref = elem.get('refReport') or elem.get('targetReport') or elem.get('ref')
                
                if target_ref:
                    rel = self._create_relationship(
                        source_id=report_id,
                        target_id=target_ref,
                        relationship_type=RelationshipType.REFERENCES,
                        properties={
                            "reference_type": "drill_through",
                            "action_type": tag_name
                        }
                    )
                    relationships.append(rel)
        
        return relationships
    
    def _extract_sub_reports(
        self,
        root: Any,
        report_id: str
    ) -> List[Relationship]:
        """
        Extract sub-report references.
        
        Finds subReport elements that reference other reports.
        """
        relationships = []
        
        # Find sub-report elements
        for elem in root.iter():
            tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
            
            if tag_name in ['subReport', 'subReportRef', 'reportReference']:
                # Get target report reference
                target_ref = elem.get('refReport') or elem.get('ref') or elem.get('reportRef')
                
                if target_ref:
                    rel = self._create_relationship(
                        source_id=report_id,
                        target_id=target_ref,
                        relationship_type=RelationshipType.REFERENCES,
                        properties={
                            "reference_type": "sub_report",
                            "element_type": tag_name
                        }
                    )
                    relationships.append(rel)
        
        return relationships
    
    def _extract_pages(
        self,
        root: Any,
        report_id: str
    ) -> tuple[List[ExtractedObject], List[Relationship]]:
        """
        Extract page elements from report specification.
        
        Pages are structural elements within reports that contain visualizations and data.
        """
        objects = []
        relationships = []
        seen_pages: Set[str] = set()
        
        # Find page elements in specification
        for elem in root.iter():
            tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
            
            if tag_name in ['page', 'reportPage', 'pageRef']:
                page_name = elem.get('name') or elem.get('ref') or f"Page_{len(seen_pages) + 1}"
                page_id_elem = elem.get('id') or elem.get('ref')
                
                # If no ID, use name or generate one
                if not page_id_elem:
                    page_id_elem = page_name.replace(' ', '_') or f"page_{len(seen_pages) + 1}"
                
                # Create unique key for deduplication
                page_key = f"{page_id_elem}:{page_name}"
                if page_key in seen_pages:
                    continue
                seen_pages.add(page_key)
                
                # Create page object
                page_id = f"{report_id}:page:{page_id_elem}"
                page_obj = self._create_object(
                    object_id=page_id,
                    name=page_name,
                    parent_id=report_id,
                    properties={
                        "cognosClass": tag_name,
                        "original_id": page_id_elem
                    }
                )
                page_obj.object_type = ObjectType.PAGE
                objects.append(page_obj)
                
                # Create CONTAINS relationship (report -> page)
                rel = self._create_relationship(
                    source_id=report_id,
                    target_id=page_id,
                    relationship_type=RelationshipType.CONTAINS,
                    properties={
                        "containment_type": "report_page"
                    }
                )
                relationships.append(rel)
        
        return objects, relationships
    
    def _extract_hierarchies(
        self,
        root: Any,
        report_id: str
    ) -> tuple[List[ExtractedObject], List[Relationship]]:
        """
        Extract hierarchy definitions from report queries.
        
        Finds hierarchies in:
        - dataItemLevelSet elements (dmHierarchy with HUN)
        - dataItemHierarchySet elements
        """
        objects = []
        relationships = []
        seen_hierarchies: Set[str] = set()
        
        # Find hierarchies in queries
        for query_elem in root.findall(".//{*}query"):
            query_name = query_elem.get("name", "Unknown")
            query_id = f"{report_id}:query:{query_name}"
            
            # Find dataItemLevelSet elements (contain hierarchy info)
            for level_set in query_elem.findall(".//{*}dataItemLevelSet"):
                hier_elem = level_set.find(".//{*}dmHierarchy")
                if hier_elem is None:
                    continue
                
                hun_elem = hier_elem.find(".//{*}HUN")
                if hun_elem is None or not hun_elem.text:
                    continue
                
                hun = hun_elem.text.strip()
                if hun in seen_hierarchies:
                    continue
                seen_hierarchies.add(hun)
                
                # Get dimension info
                dim_elem = level_set.find(".//{*}dmDimension")
                dun = None
                dim_caption = None
                if dim_elem is not None:
                    dun_elem = dim_elem.find(".//{*}DUN")
                    if dun_elem is not None and dun_elem.text:
                        dun = dun_elem.text.strip()
                    caption_elem = dim_elem.find(".//{*}itemCaption")
                    if caption_elem is not None and caption_elem.text:
                        dim_caption = caption_elem.text.strip()
                
                # Get level info
                level_elem = level_set.find(".//{*}dmLevel")
                lun = None
                level_caption = None
                if level_elem is not None:
                    lun_elem = level_elem.find(".//{*}LUN")
                    if lun_elem is not None and lun_elem.text:
                        lun = lun_elem.text.strip()
                    caption_elem = level_elem.find(".//{*}itemCaption")
                    if caption_elem is not None and caption_elem.text:
                        level_caption = caption_elem.text.strip()
                
                # Get hierarchy caption
                hier_caption_elem = hier_elem.find(".//{*}itemCaption")
                hier_caption = hier_caption_elem.text.strip() if hier_caption_elem is not None and hier_caption_elem.text else None
                
                # Create hierarchy name from HUN or caption
                hier_name = hier_caption or level_set.get("name") or hun.split(".")[-1] if hun else "Unknown Hierarchy"
                
                # Create hierarchy object
                hier_id = f"{report_id}:hierarchy:{hun}"
                hier_obj = self._create_object(
                    object_id=hier_id,
                    name=hier_name,
                    parent_id=report_id,
                    properties={
                        "HUN": hun,  # Hierarchy Unique Name
                        "DUN": dun,  # Dimension Unique Name
                        "LUN": lun,  # Level Unique Name
                        "dimension_caption": dim_caption,
                        "level_caption": level_caption,
                        "hierarchy_caption": hier_caption,
                        "cognosClass": "hierarchy",
                        "source_query": query_name
                    }
                )
                hier_obj.object_type = ObjectType.HIERARCHY
                objects.append(hier_obj)
                
                # Create CONTAINS relationship (report -> hierarchy)
                rel = self._create_relationship(
                    source_id=report_id,
                    target_id=hier_id,
                    relationship_type=RelationshipType.CONTAINS,
                    properties={
                        "containment_type": "report_hierarchy",
                        "query_name": query_name
                    }
                )
                relationships.append(rel)
                
                # Link to query if available
                if query_id:
                    rel_query = self._create_relationship(
                        source_id=query_id,
                        target_id=hier_id,
                        relationship_type=RelationshipType.USES,
                        properties={"dependency_type": "hierarchy"}
                    )
                    relationships.append(rel_query)
            
            # Find dataItemHierarchySet elements
            for hier_set in query_elem.findall(".//{*}dataItemHierarchySet"):
                hier_elem = hier_set.find(".//{*}dmHierarchy")
                if hier_elem is None:
                    continue
                
                hun_elem = hier_elem.find(".//{*}HUN")
                if hun_elem is None or not hun_elem.text:
                    continue
                
                hun = hun_elem.text.strip()
                if hun in seen_hierarchies:
                    continue
                seen_hierarchies.add(hun)
                
                # Get dimension info
                dim_elem = hier_set.find(".//{*}dmDimension")
                dun = None
                dim_caption = None
                if dim_elem is not None:
                    dun_elem = dim_elem.find(".//{*}DUN")
                    if dun_elem is not None and dun_elem.text:
                        dun = dun_elem.text.strip()
                    caption_elem = dim_elem.find(".//{*}itemCaption")
                    if caption_elem is not None and caption_elem.text:
                        dim_caption = caption_elem.text.strip()
                
                # Get hierarchy caption
                hier_caption_elem = hier_elem.find(".//{*}itemCaption")
                hier_caption = hier_caption_elem.text.strip() if hier_caption_elem is not None and hier_caption_elem.text else None
                
                # Create hierarchy name
                hier_name = hier_caption or hier_set.get("name") or hun.split(".")[-1] if hun else "Unknown Hierarchy"
                
                # Create hierarchy object
                hier_id = f"{report_id}:hierarchy:{hun}"
                hier_obj = self._create_object(
                    object_id=hier_id,
                    name=hier_name,
                    parent_id=report_id,
                    properties={
                        "HUN": hun,
                        "DUN": dun,
                        "dimension_caption": dim_caption,
                        "hierarchy_caption": hier_caption,
                        "cognosClass": "hierarchy",
                        "source_query": query_name,
                        "root_members_only": hier_set.get("rootMembersOnly", "false").lower() == "true"
                    }
                )
                hier_obj.object_type = ObjectType.HIERARCHY
                objects.append(hier_obj)
                
                # Create relationships
                rel = self._create_relationship(
                    source_id=report_id,
                    target_id=hier_id,
                    relationship_type=RelationshipType.CONTAINS,
                    properties={
                        "containment_type": "report_hierarchy",
                        "query_name": query_name
                    }
                )
                relationships.append(rel)
        
        return objects, relationships
    
    def _extract_outputs(
        self,
        root: Any,
        report_id: str,
        props_elem: Any
    ) -> tuple[List[ExtractedObject], List[Relationship]]:
        """
        Extract output formats from report.
        
        Finds outputs in:
        - defaultPortalAction property
        - runOptionStringArray with outputFormat
        - Output elements in specification
        """
        objects = []
        relationships = []
        seen_outputs: Set[str] = set()
        
        # Extract from props (defaultPortalAction)
        if props_elem is not None:
            default_action = XmlHandler.get_text(props_elem, "defaultPortalAction/value")
            if default_action and default_action not in seen_outputs:
                seen_outputs.add(default_action)
                output_id = f"{report_id}:output:{default_action}"
                output_obj = self._create_object(
                    object_id=output_id,
                    name=f"Output: {default_action}",
                    parent_id=report_id,
                    properties={
                        "output_type": default_action,
                        "is_default": True,
                        "cognosClass": "output"
                    }
                )
                output_obj.object_type = ObjectType.OUTPUT
                objects.append(output_obj)
                
                rel = self._create_relationship(
                    source_id=report_id,
                    target_id=output_id,
                    relationship_type=RelationshipType.CONTAINS,
                    properties={"containment_type": "report_output", "is_default": True}
                )
                relationships.append(rel)
            
            # Extract run options (outputFormat)
            run_options = props_elem.findall(".//{*}runOptionStringArray")
            for run_opt in run_options:
                name_elem = run_opt.find(".//{*}name")
                if name_elem is None or name_elem.text != "outputFormat":
                    continue
                
                value_elems = run_opt.findall(".//{*}value")
                for value_elem in value_elems:
                    if value_elem.text and value_elem.text not in seen_outputs:
                        output_format = value_elem.text.strip()
                        seen_outputs.add(output_format)
                        output_id = f"{report_id}:output:{output_format}"
                        output_obj = self._create_object(
                            object_id=output_id,
                            name=f"Output: {output_format}",
                            parent_id=report_id,
                            properties={
                                "output_type": output_format,
                                "is_default": False,
                                "cognosClass": "output"
                            }
                        )
                        output_obj.object_type = ObjectType.OUTPUT
                        objects.append(output_obj)
                        
                        rel = self._create_relationship(
                            source_id=report_id,
                            target_id=output_id,
                            relationship_type=RelationshipType.CONTAINS,
                            properties={"containment_type": "report_output", "is_default": False}
                        )
                        relationships.append(rel)
        
        # Extract from specification (output elements)
        for output_elem in root.findall(".//{*}output"):
            output_name = output_elem.get("name") or "Output"
            output_type = output_elem.get("type") or output_elem.get("format") or "unknown"
            output_key = f"{output_type}:{output_name}"
            
            if output_key not in seen_outputs:
                seen_outputs.add(output_key)
                output_id = f"{report_id}:output:{output_key}"
                output_obj = self._create_object(
                    object_id=output_id,
                    name=output_name,
                    parent_id=report_id,
                    properties={
                        "output_type": output_type,
                        "cognosClass": "output"
                    }
                )
                output_obj.object_type = ObjectType.OUTPUT
                objects.append(output_obj)
                
                rel = self._create_relationship(
                    source_id=report_id,
                    target_id=output_id,
                    relationship_type=RelationshipType.CONTAINS,
                    properties={"containment_type": "report_output"}
                )
                relationships.append(rel)
        
        return objects, relationships
    
    def _extract_prompts(
        self,
        root: Any,
        report_id: str
    ) -> tuple[List[ExtractedObject], List[Relationship]]:
        """
        Extract prompt pages and prompt queries from report.
        
        Finds prompts in:
        - promptPages elements
        - Queries named "Prompt Query" or containing prompt definitions
        """
        objects = []
        relationships = []
        seen_prompt_ids: Set[str] = set()

        def add_prompt_page(page_id: str, page_name: str) -> None:
            if page_id in seen_prompt_ids:
                return
            seen_prompt_ids.add(page_id)
            prompt_obj = self._create_object(
                object_id=page_id,
                name=page_name,
                parent_id=report_id,
                properties={
                    "prompt_type": "page",
                    "cognosClass": "prompt"
                }
            )
            prompt_obj.object_type = ObjectType.PROMPT
            objects.append(prompt_obj)
            relationships.append(
                self._create_relationship(
                    source_id=report_id,
                    target_id=page_id,
                    relationship_type=RelationshipType.CONTAINS,
                    properties={"containment_type": "report_prompt"}
                )
            )

        # Extract prompt pages from promptPages container
        prompt_pages = root.findall(".//{*}promptPages")
        for prompt_page_container in prompt_pages:
            pages = prompt_page_container.findall(".//{*}page")
            for page_elem in pages:
                page_name = page_elem.get("name") or "Prompt Page"
                page_id = f"{report_id}:prompt:{page_name}"
                add_prompt_page(page_id, page_name)
        # Also extract pages under reportPages whose name contains "Prompt"
        # (Cognos stores prompt pages as reportPages with names like "Text Prompt Page")
        for report_pages in root.findall(".//{*}reportPages"):
            for page_elem in report_pages.findall(".//{*}page"):
                page_name = page_elem.get("name") or ""
                if "prompt" in page_name.lower():
                    page_id = f"{report_id}:prompt:{page_name}"
                    add_prompt_page(page_id, page_name)
        
        # Extract prompt queries (queries that are used for prompts)
        for query_elem in root.findall(".//{*}query"):
            query_name = query_elem.get("name", "")
            if "prompt" in query_name.lower() or "Prompt" in query_name:
                query_id = f"{report_id}:query:{query_name}"
                
                # Check if this query is used for prompts
                prompt_obj = self._create_object(
                    object_id=f"{report_id}:prompt:query:{query_name}",
                    name=f"Prompt: {query_name}",
                    parent_id=report_id,
                    properties={
                        "prompt_type": "query",
                        "query_name": query_name,
                        "cognosClass": "prompt"
                    }
                )
                prompt_obj.object_type = ObjectType.PROMPT
                objects.append(prompt_obj)
                
                rel = self._create_relationship(
                    source_id=report_id,
                    target_id=prompt_obj.object_id,
                    relationship_type=RelationshipType.CONTAINS,
                    properties={"containment_type": "report_prompt"}
                )
                relationships.append(rel)
                
                # Link to query
                rel_query = self._create_relationship(
                    source_id=prompt_obj.object_id,
                    target_id=query_id,
                    relationship_type=RelationshipType.USES,
                    properties={"dependency_type": "prompt_query"}
                )
                relationships.append(rel_query)
        
        return objects, relationships

    def _extract_sorts(
        self,
        root: Any,
        report_id: str
    ) -> tuple[List[ExtractedObject], List[Relationship]]:
        """
        Extract sort definitions from report specification.
        Finds sortList/sortItem with refDataItem and sortOrder (e.g. ascending/descending).
        """
        objects = []
        relationships = []
        seen_sort_ids: Set[str] = set()

        for sort_list in root.findall(".//{*}sortList"):
            list_name = sort_list.get("name")  # optional context
            for i, sort_item in enumerate(sort_list.findall(".//{*}sortItem")):
                ref_data_item = sort_item.get("refDataItem")
                sort_order = sort_item.get("sortOrder") or sort_item.get("order") or "ascending"
                if not ref_data_item:
                    continue
                sort_key = f"{report_id}:sort:{ref_data_item}:{sort_order}:{i}"
                if sort_key in seen_sort_ids:
                    continue
                seen_sort_ids.add(sort_key)
                sort_id = f"{report_id}:sort:{ref_data_item}:{i}"
                sort_obj = self._create_object(
                    object_id=sort_id,
                    name=f"Sort: {ref_data_item} ({sort_order})",
                    parent_id=report_id,
                    properties={
                        "refDataItem": ref_data_item,
                        "sortOrder": sort_order,
                        "list_name": list_name,
                        "cognosClass": "sort"
                    }
                )
                sort_obj.object_type = ObjectType.SORT
                objects.append(sort_obj)
                relationships.append(
                    self._create_relationship(
                        source_id=report_id,
                        target_id=sort_id,
                        relationship_type=RelationshipType.CONTAINS,
                        properties={"containment_type": "report_sort"}
                    )
                )
        return objects, relationships

    def _extract_report_data_stores(
        self,
        root: Any,
        report_id: str,
        report_object: Optional[ExtractedObject] = None,
    ) -> tuple[List[Relationship], List[Dict[str, Any]]]:
        """
        Extract reportDataStores from report specification for lineage.
        Each store binds a query (refQuery) to a set of data items (refDataItem).
        Creates USES relationships from report to each query used by a store.
        Returns (relationships, store_list) for attachment to report.properties["reportDataStores"].
        """
        relationships: List[Relationship] = []
        store_list: List[Dict[str, Any]] = []
        seen_query_rels: Set[str] = set()

        for stores_elem in root.findall(".//{*}reportDataStores"):
            for store_elem in stores_elem.findall(".//{*}reportDataStore"):
                store_name = store_elem.get("name") or "dataStore"
                ref_query = None
                data_items: List[Dict[str, str]] = []

                ds_source = store_elem.find(".//{*}dsSource")
                if ds_source is not None:
                    # dsV5ListQuery, dsV5CrosstabQuery, etc. have refQuery
                    for query_elem in ds_source.findall(".//*"):
                        if query_elem.get("refQuery"):
                            ref_query = query_elem.get("refQuery")
                            break
                    if ref_query is None:
                        for q in ds_source:
                            ref_query = q.get("refQuery")
                            if ref_query:
                                break
                    # dsV5DataItems / dsV5DataItem
                    for di_elem in ds_source.findall(".//{*}dsV5DataItem"):
                        ref_di = di_elem.get("refDataItem")
                        if ref_di:
                            data_items.append({
                                "refDataItem": ref_di,
                                "dsColumnType": di_elem.get("dsColumnType") or "",
                            })

                store_list.append({
                    "name": store_name,
                    "refQuery": ref_query or "",
                    "dataItems": data_items,
                })

                # Report → query lineage (USES)
                if ref_query:
                    query_id = f"{report_id}:query:{ref_query}"
                    rel_key = f"{report_id}:{query_id}"
                    if rel_key not in seen_query_rels:
                        seen_query_rels.add(rel_key)
                        relationships.append(
                            self._create_relationship(
                                source_id=report_id,
                                target_id=query_id,
                                relationship_type=RelationshipType.USES,
                                properties={
                                    "dependency_type": "report_data_store",
                                    "store_name": store_name,
                                    "data_item_count": len(data_items),
                                },
                            )
                        )

        return relationships, store_list

