"""
JSON file handler for parsing BI exports.
"""
import json
from pathlib import Path
from typing import Union, Any, Dict, List
import logging


logger = logging.getLogger(__name__)


class JsonHandler:
    """Handler for JSON file operations."""
    
    @staticmethod
    def load(json_path: Union[str, Path]) -> Union[Dict, List]:
        """
        Load and parse a JSON file.
        
        Args:
            json_path: Path to JSON file
        
        Returns:
            Parsed JSON data (dict or list)
        
        Raises:
            FileNotFoundError: If json_path doesn't exist
            json.JSONDecodeError: If JSON is malformed
        """
        json_path = Path(json_path)
        
        if not json_path.exists():
            raise FileNotFoundError(f"JSON file not found: {json_path}")
        
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.debug(f"Loaded JSON file: {json_path.name}")
                return data
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {json_path}, error: {e}")
            raise
    
    @staticmethod
    def parse_string(json_string: str) -> Union[Dict, List]:
        """
        Parse a JSON string.
        
        Args:
            json_string: JSON content as string
        
        Returns:
            Parsed JSON data (dict or list)
        
        Raises:
            json.JSONDecodeError: If JSON is malformed
        """
        try:
            return json.loads(json_string)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON string: {e}")
            raise
    
    @staticmethod
    def save(
        data: Union[Dict, List],
        json_path: Union[str, Path],
        indent: int = 2
    ) -> None:
        """
        Save data to a JSON file.
        
        Args:
            data: Data to save (dict or list)
            json_path: Path to save to
            indent: Indentation level for pretty printing
        """
        json_path = Path(json_path)
        json_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
            logger.debug(f"Saved JSON file: {json_path.name}")
    
    @staticmethod
    def get_value(
        data: Dict,
        key_path: str,
        default: Any = None,
        separator: str = "."
    ) -> Any:
        """
        Get a nested value from a dictionary using a key path.
        
        Args:
            data: Dictionary to search in
            key_path: Dot-separated path to value (e.g., "user.profile.name")
            default: Default value if key path not found
            separator: Separator for key path (default: ".")
        
        Returns:
            Value at key path or default
        
        Examples:
            >>> data = {"user": {"profile": {"name": "John"}}}
            >>> JsonHandler.get_value(data, "user.profile.name")
            "John"
        """
        keys = key_path.split(separator)
        current = data
        
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        
        return current
    
    @staticmethod
    def is_valid(json_string: str) -> bool:
        """
        Check if a string is valid JSON.
        
        Args:
            json_string: String to validate
        
        Returns:
            True if valid JSON, False otherwise
        """
        try:
            json.loads(json_string)
            return True
        except (json.JSONDecodeError, TypeError):
            return False
    
    @staticmethod
    def pretty_print(data: Union[Dict, List]) -> str:
        """
        Convert data to a pretty-printed JSON string.
        
        Args:
            data: Data to format
        
        Returns:
            Pretty-printed JSON string
        """
        return json.dumps(data, indent=2, ensure_ascii=False)
