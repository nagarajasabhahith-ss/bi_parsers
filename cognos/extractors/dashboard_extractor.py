"""
Dashboard extractor for Cognos.

Extracts dashboard (exploration) objects and their embedded visualizations
from Cognos export files.
"""
from typing import List, Any, Dict, Optional, Tuple
from datetime import datetime
import json
import logging

from ...core import BaseExtractor, ExtractedObject, Relationship, ParseError, ObjectType, RelationshipType
from ...core.handlers import XmlHandler
from ..visualization_types import map_dashboard_visid_to_type


logger = logging.getLogger(__name__)


class DashboardExtractor(BaseExtractor):
    """Extractor for Cognos dashboard/exploration objects."""
    
    def __init__(self):
        super().__init__(bi_tool="cognos")
    
    @property
    def object_type(self) -> str:
        return ObjectType.DASHBOARD.value
    
    def extract(
        self,
        source: Any
    ) -> tuple[List[ExtractedObject], List[Relationship], List[ParseError]]:
        """
        Extract dashboard objects from XML element.
        
        Args:
            source: XML element containing dashboard data
        
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
                "cognosClass": obj_class
            }
            
            created_at = None
            modified_at = None
            owner = None
            specification_json = None
            
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
                
                # Extract dashboard-specific properties
                hidden = XmlHandler.get_text(props_elem, "hidden/value")
                if hidden:
                    properties["hidden"] = hidden.lower() == "true"
                
                # Extract specification JSON for visualization parsing
                spec_elem = props_elem.find("specification/value")
                if spec_elem is not None and spec_elem.text:
                    specification_json = spec_elem.text
            
            # Create dashboard object
            dashboard = self._create_object(
                object_id=obj_id,
                name=name,
                parent_id=parent_id,
                properties=properties,
                created_at=created_at,
                modified_at=modified_at,
                owner=owner,
            )
            
            objects.append(dashboard)
            
            # Create parent-child relationship (folder -> dashboard)
            if parent_id:
                rel = self._create_relationship(
                    source_id=parent_id,
                    target_id=obj_id,
                    relationship_type=RelationshipType.PARENT_CHILD
                )
                relationships.append(rel)
            
            # Extract visualizations and tabs from specification JSON
            if specification_json:
                # Extract tabs first (they contain visualizations)
                tab_objects, tab_rels, tab_errors = self._extract_tabs(
                    specification_json,
                    dashboard_id=obj_id,
                    dashboard_name=name
                )
                objects.extend(tab_objects)
                relationships.extend(tab_rels)
                errors.extend(tab_errors)
                
                # Extract visualizations
                viz_objects, viz_rels, viz_errors = self._extract_visualizations(
                    specification_json,
                    dashboard_id=obj_id,
                    dashboard_name=name
                )
                objects.extend(viz_objects)
                relationships.extend(viz_rels)
                errors.extend(viz_errors)
            
        except Exception as e:
            error = self._create_error(
                level="error",
                message=f"Failed to extract dashboard: {str(e)}"
            )
            errors.append(error)
        
        self._log_extraction(len(objects), len(errors))
        return objects, relationships, errors
    
    def _extract_visualizations(
        self,
        json_content: str,
        dashboard_id: str,
        dashboard_name: str
    ) -> tuple[List[ExtractedObject], List[Relationship], List[ParseError]]:
        """
        Extract visualization widgets from dashboard JSON specification.
        
        The JSON structure contains:
        - layout: Page/tab structure with widget references
        - widgets: Dictionary of widget definitions with visId
        - dataSources: Data source references for each visualization
        
        Args:
            json_content: The dashboard specification JSON string
            dashboard_id: Parent dashboard ID
            dashboard_name: Parent dashboard name for context
            
        Returns:
            Tuple of (objects, relationships, errors)
        """
        objects = []
        relationships = []
        errors = []
        
        try:
            spec = json.loads(json_content)
        except json.JSONDecodeError as e:
            errors.append(self._create_error(
                level="warning",
                message=f"Failed to parse dashboard JSON: {str(e)}"
            ))
            return objects, relationships, errors
        
        # Extract widgets dictionary
        widgets = spec.get("widgets", {})
        
        # Extract data sources for linking and embedded calculations
        data_sources = {}
        for ds in spec.get("dataSources", {}).get("sources", []):
            ds_id = ds.get("id")
            if ds_id:
                data_sources[ds_id] = {
                    "assetId": ds.get("assetId"),
                    "name": ds.get("name"),
                    "type": ds.get("type"),
                    "shaping": ds.get("shaping", {})
                }
                
                # Extract embedded calculations from moserJSON
                shaping = ds.get("shaping", {})
                moser_json = shaping.get("moserJSON", {})
                
                if moser_json:
                    # Extract USES relationship to base module via useSpec
                    use_specs = moser_json.get("useSpec", [])
                    for use_spec in use_specs:
                        ref_store_id = use_spec.get("storeID")
                        if ref_store_id:
                            rel = Relationship(
                                source_id=dashboard_id,
                                target_id=ref_store_id,
                                relationship_type=RelationshipType.USES,
                                properties={
                                    "dependency_type": "data_source",
                                    "ref_type": use_spec.get("type", "module"),
                                    "identifier": use_spec.get("identifier", "")
                                }
                            )
                            relationships.append(rel)
                    
                    # Extract embedded calculations (keep as CALCULATED_FIELD; data_usage in properties)
                    calculations = moser_json.get("calculation", [])
                    for calc in calculations:
                        calc_identifier = calc.get("identifier", "unknown")
                        calc_id = f"{dashboard_id}:emb_calc:{calc_identifier}"
                        calc_name = calc.get("label", calc_identifier)
                        expression = calc.get("expression", "")
                        usage = calc.get("usage", "")
                        # Map usage to data_usage for downstream (measure/dimension/attribute)
                        data_usage = "unknown"
                        if usage in ("fact", "measure") or usage == 2:
                            data_usage = "measure"
                        elif usage in ("attribute", "dimension") or usage in (0, 1):
                            data_usage = "dimension" if usage in (1, "dimension") else "attribute"
                        
                        calc_obj = ExtractedObject(
                            object_id=calc_id,
                            object_type=ObjectType.CALCULATED_FIELD,
                            name=calc_name,
                            parent_id=dashboard_id,
                            properties={
                                "expression": expression,
                                "usage": usage,
                                "data_usage": data_usage,
                                "datatype": calc.get("datatype", ""),
                                "aggregate": calc.get("regularAggregate", ""),
                                "cognosClass": "embeddedCalculation",
                                "source": "moserJSON"
                            },
                            bi_tool=self.bi_tool
                        )
                        objects.append(calc_obj)
                        
                        # Create CONTAINS relationship
                        rel = Relationship(
                            source_id=dashboard_id,
                            target_id=calc_id,
                            relationship_type=RelationshipType.CONTAINS
                        )
                        relationships.append(rel)
        
        # Build widget-to-tab mapping from layout
        widget_to_tab = {}
        layout = spec.get("layout", {})
        tabs_data = layout.get("tabs", [])
        if not tabs_data and isinstance(layout, dict):
            for key in ["tabPages", "pages", "sections"]:
                if key in layout:
                    tabs_data = layout[key]
                    break
            
            # Check for tabs in layout.items where items have type="container" and title
            if not tabs_data and "items" in layout:
                items = layout.get("items", [])
                if isinstance(items, list):
                    # Filter for items that are containers with titles (these are tabs)
                    container_items = [
                        item for item in items
                        if isinstance(item, dict) and 
                        item.get("type") == "container" and 
                        ("title" in item or item.get("name"))
                    ]
                    if container_items:
                        tabs_data = container_items
        
        if isinstance(tabs_data, dict):
            tabs_data = [tabs_data]
        
        def extract_widget_ids_from_items(items_data, tab_id):
            """Recursively extract widget IDs from nested items structure and map to tab."""
            if isinstance(items_data, list):
                for item in items_data:
                    if isinstance(item, dict):
                        # If item is a widget, map its ID to the tab
                        if item.get("type") == "widget":
                            widget_id = item.get("id")
                            if widget_id:
                                widget_to_tab[str(widget_id)] = tab_id
                        # Recursively check nested items
                        if "items" in item:
                            extract_widget_ids_from_items(item["items"], tab_id)
            elif isinstance(items_data, dict):
                if items_data.get("type") == "widget":
                    widget_id = items_data.get("id")
                    if widget_id:
                        widget_to_tab[str(widget_id)] = tab_id
                if "items" in items_data:
                    extract_widget_ids_from_items(items_data["items"], tab_id)
        
        if isinstance(tabs_data, list):
            for tab_data in tabs_data:
                tab_id_raw = tab_data.get("id") or tab_data.get("identifier", "")
                if tab_id_raw:
                    tab_id = f"{dashboard_id}:tab:{tab_id_raw}"
                    tab_widgets = tab_data.get("widgets", [])
                    if isinstance(tab_widgets, list):
                        for widget_ref in tab_widgets:
                            widget_to_tab[str(widget_ref)] = tab_id
                    elif isinstance(tab_widgets, dict):
                        for widget_ref in tab_widgets.keys():
                            widget_to_tab[str(widget_ref)] = tab_id
                    
                    # Also check layout within tab
                    tab_layout = tab_data.get("layout", {})
                    if isinstance(tab_layout, dict):
                        layout_widgets = tab_layout.get("widgets", [])
                        if layout_widgets:
                            for widget_ref in layout_widgets:
                                widget_to_tab[str(widget_ref)] = tab_id
                    
                    # Extract widgets from nested items structure
                    tab_items = tab_data.get("items", [])
                    if tab_items:
                        extract_widget_ids_from_items(tab_items, tab_id)
        
        # Process each widget
        for widget_id, widget_data in widgets.items():
            try:
                viz_obj, viz_rels = self._process_widget(
                    widget_id=widget_id,
                    widget_data=widget_data,
                    dashboard_id=dashboard_id,
                    data_sources=data_sources,
                    widget_to_tab=widget_to_tab
                )
                
                if viz_obj:
                    objects.append(viz_obj)
                    relationships.extend(viz_rels)
                    
            except Exception as e:
                logger.debug(f"Error processing widget {widget_id}: {e}")
                continue
        
        return objects, relationships, errors
    
    def _process_widget(
        self,
        widget_id: str,
        widget_data: Dict[str, Any],
        dashboard_id: str,
        data_sources: Dict[str, Any],
        widget_to_tab: Optional[Dict[str, str]] = None
    ) -> tuple[Optional[ExtractedObject], List[Relationship]]:
        """
        Process a single widget and create visualization object with relationships.
        
        Args:
            widget_id: Widget ID from JSON
            widget_data: Widget data dictionary
            dashboard_id: Parent dashboard ID
            data_sources: Data sources available in dashboard
            
        Returns:
            Tuple of (visualization object, relationships)
        """
        relationships = []
        
        # Get visId to determine chart type
        vis_id = widget_data.get("visId", "")
        widget_type = widget_data.get("type", "")
        
        # Skip non-visualization widgets (e.g., text, images)
        if widget_type not in ("live", "local", "datadriven"):
            # Could be a text widget or other non-data widget
            if not vis_id:
                return None, []
        
        # Map visId to human-readable type
        viz_type = map_dashboard_visid_to_type(vis_id)
        
        # Get widget name
        name_data = widget_data.get("name", {})
        if isinstance(name_data, dict):
            # translationTable format
            trans_table = name_data.get("translationTable", {})
            widget_name = trans_table.get("Default", trans_table.get("en-us", f"{viz_type} Widget"))
        elif isinstance(name_data, str):
            widget_name = name_data
        else:
            widget_name = f"{viz_type} Widget"
        
        if not widget_name or widget_name == "{}":
            widget_name = f"{viz_type} Widget"
        
        # Create unique object ID
        viz_obj_id = f"{dashboard_id}:widget:{widget_id}"
        
        # Extract data items (columns/measures used)
        data_items = []
        slot_mapping = widget_data.get("slotmapping", {})
        
        for slot in slot_mapping.get("slots", []):
            slot_name = slot.get("name", "")
            for data_item_id in slot.get("dataItems", []):
                data_items.append({
                    "slot": slot_name,
                    "dataItemId": data_item_id
                })
        
        # Extract data view references
        data_view_refs = []
        data_views = widget_data.get("data", {}).get("dataViews", [])
        for dv in data_views:
            model_ref = dv.get("modelRef")
            if model_ref:
                data_view_refs.append(model_ref)
            
            # Also capture individual data items with their full paths
            for item in dv.get("dataItems", []):
                item_id = item.get("itemId", "")
                item_label = item.get("itemLabel", "")
                if item_id:
                    data_items.append({
                        "itemId": item_id,
                        "itemLabel": item_label,
                        "modelRef": model_ref
                    })
        
        # Build properties
        properties = {
            "cognosClass": "widget",
            "visualization_type": viz_type,
            "visId": vis_id,
            "widget_type": widget_type,
            "data_items": data_items,
        }
        
        # Create visualization object
        viz_obj = ExtractedObject(
            object_id=viz_obj_id,
            object_type=ObjectType.VISUALIZATION,
            name=widget_name,
            parent_id=dashboard_id,
            properties=properties,
            bi_tool=self.bi_tool
        )
        
        # Determine parent: tab if widget is in a tab, otherwise dashboard
        parent_id = dashboard_id
        if widget_to_tab and widget_id in widget_to_tab:
            parent_id = widget_to_tab[widget_id]
            # Update visualization parent_id to point to tab
            viz_obj.parent_id = parent_id
        
        # Create CONTAINS relationship (dashboard/tab -> visualization)
        rel_contains = Relationship(
            source_id=parent_id,
            target_id=viz_obj_id,
            relationship_type=RelationshipType.CONTAINS,
            properties={
                "containment_type": "tab_visualization" if parent_id != dashboard_id else "dashboard_visualization"
            }
        )
        relationships.append(rel_contains)
        
        # Also create relationship from dashboard if visualization is in a tab
        if parent_id != dashboard_id:
            rel_dashboard = Relationship(
                source_id=dashboard_id,
                target_id=viz_obj_id,
                relationship_type=RelationshipType.CONTAINS,
                properties={
                    "containment_type": "dashboard_visualization",
                    "via_tab": True
                }
            )
            relationships.append(rel_dashboard)
        
        # Create USES relationships for data sources
        for model_ref in data_view_refs:
            if model_ref in data_sources:
                ds_info = data_sources[model_ref]
                asset_id = ds_info.get("assetId")
                if asset_id:
                    rel_uses = Relationship(
                        source_id=viz_obj_id,
                        target_id=asset_id,
                        relationship_type=RelationshipType.USES,
                        properties={
                            "dependency_type": "data_source",
                            "data_source_name": ds_info.get("name"),
                            "data_source_type": ds_info.get("type")
                        }
                    )
                    relationships.append(rel_uses)
        
        return viz_obj, relationships
    
    def _extract_tabs(
        self,
        json_content: str,
        dashboard_id: str,
        dashboard_name: str
    ) -> tuple[List[ExtractedObject], List[Relationship], List[ParseError]]:
        """
        Extract tab elements from dashboard JSON specification.
        
        Tabs are structural elements within dashboards that organize visualizations.
        The JSON structure typically has:
        - layout.tabs: Array of tab objects with id, name, and widget references
        
        Args:
            json_content: The dashboard specification JSON string
            dashboard_id: Parent dashboard ID
            dashboard_name: Parent dashboard name for context
            
        Returns:
            Tuple of (objects, relationships, errors)
        """
        objects = []
        relationships = []
        errors = []
        
        try:
            spec = json.loads(json_content)
        except json.JSONDecodeError as e:
            errors.append(self._create_error(
                level="warning",
                message=f"Failed to parse dashboard JSON for tabs: {str(e)}"
            ))
            return objects, relationships, errors
        
        # Extract layout structure
        layout = spec.get("layout", {})
        
        # Check for tabs in layout
        tabs_data = layout.get("tabs", [])
        if not tabs_data and isinstance(layout, dict):
            # Sometimes tabs might be at root level or in a different structure
            # Check for tab-like structures
            for key in ["tabPages", "pages", "sections"]:
                if key in layout:
                    tabs_data = layout[key]
                    break
            
            # Check for tabs in layout.items where items have type="container" and title
            # This is a common structure where tabs are containers within the layout
            if not tabs_data and "items" in layout:
                items = layout.get("items", [])
                if isinstance(items, list):
                    # Filter for items that are containers with titles (these are tabs)
                    container_items = [
                        item for item in items
                        if isinstance(item, dict) and 
                        item.get("type") == "container" and 
                        ("title" in item or item.get("name"))
                    ]
                    if container_items:
                        tabs_data = container_items
        
        # If tabs_data is a dict, convert to list
        if isinstance(tabs_data, dict):
            tabs_data = [tabs_data]
        
        if not isinstance(tabs_data, list):
            # No tabs found
            return objects, relationships, errors
        
        # Extract widget references for linking tabs to visualizations
        widgets = spec.get("widgets", {})
        widget_to_tab = {}  # Map widget_id -> tab_id
        
        # Process each tab
        for tab_idx, tab_data in enumerate(tabs_data):
            try:
                # Get tab identifier
                tab_id_raw = tab_data.get("id") or tab_data.get("identifier") or str(tab_idx)
                
                # Get tab name - can be in "name" or "title" field
                tab_name = None
                tab_name_data = tab_data.get("name") or tab_data.get("title", {})
                
                if isinstance(tab_name_data, dict):
                    trans_table = tab_name_data.get("translationTable", {})
                    tab_name = trans_table.get("Default") or trans_table.get("en-us") or trans_table.get("en")
                    if not tab_name and trans_table:
                        # Get first available translation
                        tab_name = next(iter(trans_table.values()), None)
                elif isinstance(tab_name_data, str):
                    tab_name = tab_name_data
                
                if not tab_name or tab_name == "{}":
                    tab_name = f"Tab {tab_idx + 1}"
                
                # Create unique tab object ID
                tab_id = f"{dashboard_id}:tab:{tab_id_raw}"
                
                # Extract widget references from this tab
                widget_refs = []
                
                # Check for widgets in tab structure
                tab_widgets = tab_data.get("widgets", [])
                if isinstance(tab_widgets, list):
                    widget_refs.extend(tab_widgets)
                elif isinstance(tab_widgets, dict):
                    widget_refs.extend(tab_widgets.keys())
                
                # Also check for widget references in layout structure
                layout_widgets = tab_data.get("layout", {}).get("widgets", [])
                if layout_widgets:
                    widget_refs.extend(layout_widgets)
                
                # Recursively extract widget IDs from nested items structure
                # Widgets can be nested in items arrays with type="widget"
                def extract_widget_ids_from_items(items_data):
                    """Recursively extract widget IDs from nested items structure."""
                    widget_ids = []
                    if isinstance(items_data, list):
                        for item in items_data:
                            if isinstance(item, dict):
                                # If item is a widget, extract its ID
                                if item.get("type") == "widget":
                                    widget_id = item.get("id")
                                    if widget_id:
                                        widget_ids.append(widget_id)
                                # Recursively check nested items
                                if "items" in item:
                                    widget_ids.extend(extract_widget_ids_from_items(item["items"]))
                    elif isinstance(items_data, dict):
                        if items_data.get("type") == "widget":
                            widget_id = items_data.get("id")
                            if widget_id:
                                widget_ids.append(widget_id)
                        if "items" in items_data:
                            widget_ids.extend(extract_widget_ids_from_items(items_data["items"]))
                    return widget_ids
                
                # Extract widgets from tab's items structure
                tab_items = tab_data.get("items", [])
                if tab_items:
                    extracted_widget_ids = extract_widget_ids_from_items(tab_items)
                    widget_refs.extend(extracted_widget_ids)
                
                # Create tab object
                tab_obj = self._create_object(
                    object_id=tab_id,
                    name=tab_name,
                    parent_id=dashboard_id,
                    properties={
                        "cognosClass": "tab",
                        "original_id": tab_id_raw,
                        "widget_count": len(widget_refs),
                        "widget_refs": widget_refs[:10]  # Store first 10 for reference
                    }
                )
                tab_obj.object_type = ObjectType.TAB
                objects.append(tab_obj)
                
                # Create CONTAINS relationship (dashboard -> tab)
                rel = self._create_relationship(
                    source_id=dashboard_id,
                    target_id=tab_id,
                    relationship_type=RelationshipType.CONTAINS,
                    properties={
                        "containment_type": "dashboard_tab",
                        "tab_index": tab_idx
                    }
                )
                relationships.append(rel)
                
                # Create relationships from tab to visualizations (widgets)
                for widget_ref in widget_refs:
                    widget_id_str = str(widget_ref)
                    viz_obj_id = f"{dashboard_id}:widget:{widget_id_str}"
                    
                    # Check if visualization exists (will be created by _extract_visualizations)
                    # Create CONTAINS relationship (tab -> visualization)
                    rel_viz = self._create_relationship(
                        source_id=tab_id,
                        target_id=viz_obj_id,
                        relationship_type=RelationshipType.CONTAINS,
                        properties={
                            "containment_type": "tab_visualization",
                            "widget_ref": widget_id_str
                        }
                    )
                    relationships.append(rel_viz)
                    
                    # Track widget to tab mapping
                    widget_to_tab[widget_id_str] = tab_id
                    
            except Exception as e:
                logger.debug(f"Error extracting tab {tab_idx}: {e}")
                errors.append(self._create_error(
                    level="warning",
                    message=f"Failed to extract tab {tab_idx}: {str(e)}"
                ))
                continue
        
        # Widget-to-tab mapping is handled via relationships created above
        # No need to store separately as relationships provide the linkage
        
        return objects, relationships, errors
    