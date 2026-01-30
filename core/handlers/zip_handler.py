"""
ZIP file handler for extracting BI exports.
"""
import zipfile
import tempfile
import shutil
from pathlib import Path
from typing import Union, List, Optional
import logging


logger = logging.getLogger(__name__)


class ZipHandler:
    """Handler for ZIP file operations."""
    
    @staticmethod
    def extract(
        zip_path: Union[str, Path],
        extract_to: Optional[Union[str, Path]] = None,
        cleanup: bool = True
    ) -> Path:
        """
        Extract a ZIP file to a directory.
        
        Args:
            zip_path: Path to ZIP file
            extract_to: Directory to extract to (creates temp dir if None)
            cleanup: Whether to cleanup on exit (only for temp dirs)
        
        Returns:
            Path to extraction directory
        
        Raises:
            FileNotFoundError: If zip_path doesn't exist
            zipfile.BadZipFile: If file is not a valid ZIP
        """
        zip_path = Path(zip_path)
        
        if not zip_path.exists():
            raise FileNotFoundError(f"ZIP file not found: {zip_path}")
        
        # Create extraction directory
        if extract_to is None:
            extract_dir = Path(tempfile.mkdtemp(prefix="bi_parser_"))
            logger.info(f"Created temp extraction dir: {extract_dir}")
        else:
            extract_dir = Path(extract_to)
            extract_dir.mkdir(parents=True, exist_ok=True)
        
        # Extract ZIP
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
                logger.info(f"Extracted {zip_path.name} to {extract_dir}")
        except zipfile.BadZipFile as e:
            logger.error(f"Invalid ZIP file: {zip_path}")
            raise
        
        return extract_dir
    
    @staticmethod
    def list_contents(zip_path: Union[str, Path]) -> List[str]:
        """
        List contents of a ZIP file.
        
        Args:
            zip_path: Path to ZIP file
        
        Returns:
            List of file paths in the ZIP
        
        Raises:
            FileNotFoundError: If zip_path doesn't exist
            zipfile.BadZipFile: If file is not a valid ZIP
        """
        zip_path = Path(zip_path)
        
        if not zip_path.exists():
            raise FileNotFoundError(f"ZIP file not found: {zip_path}")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            return zip_ref.namelist()
    
    @staticmethod
    def is_zip(file_path: Union[str, Path]) -> bool:
        """
        Check if a file is a valid ZIP file.
        
        Args:
            file_path: Path to file
        
        Returns:
            True if valid ZIP, False otherwise
        """
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                return True
        except (zipfile.BadZipFile, FileNotFoundError):
            return False
    
    @staticmethod
    def extract_file(
        zip_path: Union[str, Path],
        file_name: str,
        extract_to: Optional[Union[str, Path]] = None
    ) -> Path:
        """
        Extract a single file from a ZIP archive.
        
        Args:
            zip_path: Path to ZIP file
            file_name: Name of file to extract (relative path in ZIP)
            extract_to: Directory to extract to (creates temp dir if None)
        
        Returns:
            Path to extracted file
        
        Raises:
            FileNotFoundError: If zip_path doesn't exist or file not in ZIP
            zipfile.BadZipFile: If file is not a valid ZIP
        """
        zip_path = Path(zip_path)
        
        if not zip_path.exists():
            raise FileNotFoundError(f"ZIP file not found: {zip_path}")
        
        # Create extraction directory
        if extract_to is None:
            extract_dir = Path(tempfile.mkdtemp(prefix="bi_parser_"))
        else:
            extract_dir = Path(extract_to)
            extract_dir.mkdir(parents=True, exist_ok=True)
        
        # Extract specific file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            try:
                zip_ref.extract(file_name, extract_dir)
                extracted_path = extract_dir / file_name
                logger.debug(f"Extracted {file_name} from {zip_path.name}")
                return extracted_path
            except KeyError:
                raise FileNotFoundError(
                    f"File '{file_name}' not found in ZIP: {zip_path}"
                )
    
    @staticmethod
    def cleanup(directory: Union[str, Path]) -> None:
        """
        Remove a directory and all its contents.
        
        Args:
            directory: Path to directory to remove
        """
        directory = Path(directory)
        if directory.exists() and directory.is_dir():
            shutil.rmtree(directory)
            logger.debug(f"Cleaned up directory: {directory}")
