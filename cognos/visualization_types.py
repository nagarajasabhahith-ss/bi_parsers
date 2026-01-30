"""
Visualization type mappings for Cognos.

This module provides comprehensive mapping of Cognos visualization IDs (visId)
to human-readable chart type names. Used by DashboardExtractor and ReportExtractor.
"""
from typing import Optional
import re


# Dashboard Widget visId to Chart Type mapping
# These are found in exploration/dashboard JSON specifications
DASHBOARD_VIS_ID_MAP = {
    # Data Tables / Grids
    "JQGrid": "Data Table",
    "list": "List",
    "crosstab": "CrossTab",
    "summary": "Summary",
    "dataPlayer": "Data Player",
    
    # Column Charts
    "com.ibm.vis.rave2bundlecolumn": "Clustered Column",
    "com.ibm.vis.rave2bundlestackedcolumn": "Stacked Column", 
    "com.ibm.vis.rave2bundlefloatingcolumn": "Floating Column",
    
    # Bar Charts
    "com.ibm.vis.rave2bundlebar": "Clustered Bar",
    "com.ibm.vis.rave2bundlestackedbar": "Stacked Bar",
    "com.ibm.vis.rave2bundlefloatingbar": "Floating Bar",
    
    # Line Charts
    "com.ibm.vis.rave2line": "Line",
    "com.ibm.vis.raveline": "Line",
    "com.ibm.vis.rave2bundleline": "Line",
    "com.ibm.vis.rave2smoothline": "Smooth Line",
    "com.ibm.vis.rave2steppedline": "Stepped Line",
    
    # Area Charts
    "com.ibm.vis.rave2bundlearea": "Area",
    "com.ibm.vis.rave2bundlestackedarea": "Stacked Area",
    "com.ibm.vis.rave2smootharea": "Smooth Area",
    "com.ibm.vis.rave2steppedarea": "Stepped Area",
    
    # Pie & Donut
    "com.ibm.vis.rave2bundlepie": "Pie",
    "com.ibm.vis.rave2bundledonut": "Donut",
    "com.ibm.vis.ravepie": "Pie",
    
    # Combination Charts
    "com.ibm.vis.rave2bundlecomposite": "Clustered Combination",
    "com.ibm.vis.rave2bundlestackedcomposite": "Stacked Combination",
    "com.ibm.vis.rave2bundlelineandcolumn": "Line and Column",
    
    # Scatter & Bubble
    "com.ibm.vis.ravescatter": "Scatter",
    "com.ibm.vis.rave2scatter": "Scatter",
    "com.ibm.vis.ravebubble": "Bubble",
    "com.ibm.vis.rave2bubble": "Bubble",
    "com.ibm.vis.rave2point": "Point",
    
    # Specialty Charts
    "com.ibm.vis.rave2bundlebullet": "Bullet",
    "com.ibm.vis.rave2bundlewordcloud": "Word Cloud",
    "com.ibm.vis.rave2bundletreemap": "TreeMap",
    "com.ibm.vis.rave2bundleradialbar": "Radial",
    "com.ibm.vis.rave2radar": "Radar",
    "com.ibm.vis.rave2bundlehierarchicalpackedbubble": "Packed Bubble",
    "com.ibm.vis.rave2hierarchybubble": "Hierarchy Bubble",
    
    # Maps
    "com.ibm.vis.rave2polygonmap": "Map",
    "com.ibm.vis.rave2bundletiledmap": "Tiled Map",
    "com.ibm.vis.rave2bundlemap": "Map",
    "com.ibm.vis.legacymap": "Legacy Map",
    
    # Advanced Visualizations
    "com.ibm.vis.sunburst": "Sunburst",
    "com.ibm.vis.rave2network": "Network",
    "com.ibm.vis.rave2marimekko": "Marimekko",
    "com.ibm.vis.decisiontree": "Decision Tree",
    "com.ibm.vis.driveranalysis": "Driver Analysis",
    "com.ibm.vis.spiral": "Spiral",
    "com.ibm.vis.rave2comet": "Comet",
    "com.ibm.vis.rave2heat": "Heatmap",
    "com.ibm.vis.rave2boxplot": "Box Plot",
    "com.ibm.vis.rave2tornado": "Tornado",
    "com.ibm.vis.rave2waterfall": "Waterfall",
    "com.ibm.vis.rave2gantt": "Gantt",
    
    # KPI & Summary
    "com.ibm.vis.kpi": "KPI",
    "com.ibm.vis.rave2kpi": "KPI",
    
    # Controls
    "dropdown": "Drop-down List",
    "com.ibm.vis.dropdown": "Drop-down List",
    
    # Repeaters
    "repeater": "Repeater",
    "repeaterTable": "Repeater Table",
    "singleton": "Singleton",
}


# Report Specification element to Chart Type mapping
# These are found in report XML specifications (escaped XML)
REPORT_ELEMENT_MAP = {
    # Tables & Lists
    "list": "List",
    "crosstab": "CrossTab", 
    "table": "Table",
    "repeaterTable": "Repeater Table",
    "repeater": "Repeater",
    "dataTable": "Data Table",
    "singleton": "Singleton",
    
    # Charts (generic - need chartType attribute for specifics)
    "chart": "Chart",
    "combinationChart": "Combination Chart",
    "pieChart": "Pie Chart",
    "scatterChart": "Scatter Chart",
    "bubbleChart": "Bubble Chart",
    "gaugeChart": "Gauge Chart",
    "metricsChart": "Metrics Chart",
    "map": "Legacy Map",
    
    # Modern visualizations  
    "visualization": "Visualization",
}


# Chart type attribute mapping (for <chart chartType="...">)
CHART_TYPE_ATTR_MAP = {
    "bar": "Bar",
    "stackedBar": "Stacked Bar",
    "clusteredBar": "Clustered Bar",
    "column": "Column",
    "stackedColumn": "Stacked Column",
    "clusteredColumn": "Clustered Column",
    "line": "Line",
    "smoothLine": "Smooth Line",
    "steppedLine": "Stepped Line",
    "area": "Area",
    "stackedArea": "Stacked Area",
    "smoothArea": "Smooth Area",
    "steppedArea": "Stepped Area",
    "pie": "Pie",
    "donut": "Donut",
    "scatter": "Scatter",
    "bubble": "Bubble",
    "radar": "Radar",
    "gauge": "Gauge",
    "bullet": "Bullet",
    "waterfall": "Waterfall",
    "pareto": "Pareto",
    "progressiveColumn": "Progressive Column",
    "progressiveBar": "Progressive Bar",
    "combination": "Combination",
    "marimekko": "Marimekko",
    "point": "Point",
    "quadrant": "Quadrant",
}


def map_dashboard_visid_to_type(vis_id: str) -> str:
    """
    Map a dashboard widget visId to a human-readable chart type.
    
    Args:
        vis_id: The visId from dashboard JSON specification
        
    Returns:
        Human-readable chart type name
    """
    if not vis_id:
        return "Unknown"
    
    # Direct lookup
    if vis_id in DASHBOARD_VIS_ID_MAP:
        return DASHBOARD_VIS_ID_MAP[vis_id]
    
    # Fallback: clean up the visId
    # Remove com.ibm.vis. prefix and rave2bundle prefix
    clean = vis_id
    clean = clean.replace("com.ibm.vis.", "")
    clean = clean.replace("rave2bundle", "")
    clean = clean.replace("rave2", "")
    clean = clean.replace("rave", "")
    
    # Convert camelCase to Title Case with spaces
    clean = re.sub(r'([a-z])([A-Z])', r'\1 \2', clean)
    
    if clean:
        return clean.title()
    
    return "Custom Viz"


def map_report_element_to_type(element_tag: str, chart_type_attr: Optional[str] = None) -> str:
    """
    Map a report XML element to a human-readable chart type.
    
    Args:
        element_tag: The XML element tag (e.g., 'list', 'chart', 'crosstab')
        chart_type_attr: Optional chartType attribute value for chart elements
        
    Returns:
        Human-readable chart type name
    """
    # If it's a chart with a chartType attribute, use that
    if element_tag == "chart" and chart_type_attr:
        if chart_type_attr in CHART_TYPE_ATTR_MAP:
            return CHART_TYPE_ATTR_MAP[chart_type_attr]
        # Fallback: clean up the attribute
        clean = re.sub(r'([a-z])([A-Z])', r'\1 \2', chart_type_attr)
        return clean.title() + " Chart"
    
    # Direct element lookup
    if element_tag in REPORT_ELEMENT_MAP:
        return REPORT_ELEMENT_MAP[element_tag]
    
    # Fallback
    clean = re.sub(r'([a-z])([A-Z])', r'\1 \2', element_tag)
    return clean.title()


# All supported visualization types (for reference/validation)
ALL_VISUALIZATION_TYPES = sorted(set(
    list(DASHBOARD_VIS_ID_MAP.values()) + 
    list(REPORT_ELEMENT_MAP.values()) +
    list(CHART_TYPE_ATTR_MAP.values())
))
