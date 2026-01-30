"""
XML file handler for parsing BI exports.
"""
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Union, Optional, Iterator, Dict, Any
import logging


logger = logging.getLogger(__name__)


class XmlHandler:
    """Handler for XML file operations with streaming support."""
    
    @staticmethod
    def parse(xml_path: Union[str, Path]) -> ET.ElementTree:
        """
        Parse an XML file into an ElementTree.
        
        Args:
            xml_path: Path to XML file
        
        Returns:
            ElementTree object
        
        Raises:
            FileNotFoundError: If xml_path doesn't exist
            ET.ParseError: If XML is malformed
        """
        xml_path = Path(xml_path)
        
        if not xml_path.exists():
            raise FileNotFoundError(f"XML file not found: {xml_path}")
        
        try:
            tree = ET.parse(xml_path)
            logger.debug(f"Parsed XML file: {xml_path.name}")
            return tree
        except ET.ParseError as e:
            logger.error(f"Failed to parse XML: {xml_path}, error: {e}")
            raise
    
    @staticmethod
    def parse_string(xml_string: str) -> ET.Element:
        """
        Parse an XML string into an Element.
        
        Args:
            xml_string: XML content as string
        
        Returns:
            Root Element
        
        Raises:
            ET.ParseError: If XML is malformed
        """
        try:
            return ET.fromstring(xml_string)
        except ET.ParseError as e:
            logger.error(f"Failed to parse XML string: {e}")
            raise
    
    @staticmethod
    def get_root(tree: ET.ElementTree) -> ET.Element:
        """
        Get the root element from an ElementTree.
        
        Args:
            tree: ElementTree object
        
        Returns:
            Root Element
        """
        return tree.getroot()
    
    @staticmethod
    def find_all(
        root: ET.Element,
        xpath: str,
        namespaces: Optional[Dict[str, str]] = None
    ) -> list[ET.Element]:
        """
        Find all elements matching an XPath expression.
        
        Args:
            root: Root element to search from
            xpath: XPath expression
            namespaces: Optional namespace mappings
        
        Returns:
            List of matching elements
        """
        if namespaces:
            return root.findall(xpath, namespaces)
        return root.findall(xpath)
    
    @staticmethod
    def find_one(
        root: ET.Element,
        xpath: str,
        namespaces: Optional[Dict[str, str]] = None
    ) -> Optional[ET.Element]:
        """
        Find first element matching an XPath expression.
        
        Args:
            root: Root element to search from
            xpath: XPath expression
            namespaces: Optional namespace mappings
        
        Returns:
            First matching element or None
        """
        if namespaces:
            return root.find(xpath, namespaces)
        return root.find(xpath)
    
    @staticmethod
    def get_text(
        element: ET.Element,
        xpath: Optional[str] = None,
        default: str = "",
        namespaces: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Get text content from an element or child element.
        
        Args:
            element: Element to get text from
            xpath: Optional XPath to child element
            default: Default value if element not found or has no text
            namespaces: Optional namespace mappings
        
        Returns:
            Text content or default
        """
        if xpath:
            child = XmlHandler.find_one(element, xpath, namespaces)
            if child is not None and child.text:
                return child.text.strip()
            return default
        
        if element is not None and element.text:
            return element.text.strip()
        return default
    
    @staticmethod
    def get_attribute(
        element: ET.Element,
        attr_name: str,
        default: Optional[str] = None
    ) -> Optional[str]:
        """
        Get attribute value from an element.
        
        Args:
            element: Element to get attribute from
            attr_name: Attribute name
            default: Default value if attribute not found
        
        Returns:
            Attribute value or default
        """
        return element.get(attr_name, default)
    
    @staticmethod
    def iter_elements(
        xml_path: Union[str, Path],
        tag: str
    ) -> Iterator[ET.Element]:
        """
        Iterate over elements with a specific tag (streaming).
        
        Useful for large XML files to avoid loading entire file into memory.
        
        Args:
            xml_path: Path to XML file
            tag: Tag name to iterate over
        
        Yields:
            Elements matching the tag
        
        Raises:
            FileNotFoundError: If xml_path doesn't exist
            ET.ParseError: If XML is malformed
        """
        xml_path = Path(xml_path)
        
        if not xml_path.exists():
            raise FileNotFoundError(f"XML file not found: {xml_path}")
        
        try:
            for event, elem in ET.iterparse(xml_path, events=('end',)):
                if elem.tag == tag or elem.tag.endswith(f'}}{tag}'):
                    yield elem
                    # Clear element to free memory
                    elem.clear()
        except ET.ParseError as e:
            logger.error(f"Failed to parse XML: {xml_path}, error: {e}")
            raise
    
    @staticmethod
    def element_to_dict(element: ET.Element) -> Dict[str, Any]:
        """
        Convert an XML element to a dictionary.
        
        Args:
            element: Element to convert
        
        Returns:
            Dictionary representation of the element
        """
        result = {}
        
        # Add attributes
        if element.attrib:
            result['@attributes'] = dict(element.attrib)
        
        # Add text content
        if element.text and element.text.strip():
            result['@text'] = element.text.strip()
        
        # Add children
        for child in element:
            child_dict = XmlHandler.element_to_dict(child)
            tag = child.tag.split('}')[-1]  # Remove namespace
            
            if tag in result:
                # Multiple children with same tag
                if not isinstance(result[tag], list):
                    result[tag] = [result[tag]]
                result[tag].append(child_dict)
            else:
                result[tag] = child_dict
        
        return result
    
    @staticmethod
    def extract_namespaces(root: ET.Element) -> Dict[str, str]:
        """
        Extract namespace mappings from an XML element.
        
        Args:
            root: Root element
        
        Returns:
            Dictionary of namespace prefix to URI mappings
        """
        namespaces = {}
        for prefix, uri in ET.iterparse(
            Path(root.tag),
            events=('start-ns',)
        ):
            namespaces[prefix] = uri
        return namespaces
