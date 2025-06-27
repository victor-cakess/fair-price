"""
FAIR-PRICE Data Extraction Module

Web scraping and data extraction for Brazilian Health Economics data.
Downloads CSV files from OpenDataSUS website with robust error handling.
"""

import requests
from bs4 import BeautifulSoup
import re
import time
import zipfile
import io
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging

from .config import Config, WebScrapingConfig, FileConfig
from .utils import (
    FileUtils, ProgressTracker, LoggerSetup, StatusPrinter,
    retry_on_exception, timing_decorator
)


class HealthDataExtractor:
    """
    Main class for extracting Brazilian Health Economics data
    """
    
    def __init__(self, output_dir: Optional[Path] = None):
        """
        Initialize the extractor
        
        Args:
            output_dir: Directory to save downloaded files (defaults to config)
        """
        self.output_dir = output_dir or Config.RAW_DATA_DIR
        self.logger = LoggerSetup.get_project_logger()
        
        # Ensure output directory exists
        FileUtils.ensure_directory(self.output_dir)
        
        # Setup HTTP session
        self.session = requests.Session()
        self.session.headers.update(WebScrapingConfig.REQUEST_HEADERS)
        
        self.logger.info(f"HealthDataExtractor initialized - Output: {self.output_dir}")
    
    @retry_on_exception(
        max_retries=WebScrapingConfig.MAX_RETRIES,
        delay_seconds=WebScrapingConfig.RETRY_DELAY_SECONDS,
        exceptions=(requests.RequestException, ConnectionError)
    )
    def _make_request(self, url: str) -> requests.Response:
        """
        Make HTTP request with error handling
        
        Args:
            url: URL to request
            
        Returns:
            Response object
            
        Raises:
            requests.RequestException: If request fails after retries
        """
        response = self.session.get(
            url, 
            timeout=WebScrapingConfig.TIMEOUT_SECONDS,
            stream=True
        )
        response.raise_for_status()
        return response
    
    def discover_csv_links(self, years: Optional[List[int]] = None) -> Dict[int, str]:
        """
        Discover CSV download links from OpenDataSUS website
        
        Args:
            years: List of years to find (defaults to available years)
            
        Returns:
            Dictionary mapping year to download URL
        """
        years = years or WebScrapingConfig.AVAILABLE_YEARS
        
        StatusPrinter.print_section("Discovering CSV download links")
        self.logger.info(f"Searching for CSV links for years: {years}")
        
        try:
            # Get main BPS page
            response = self._make_request(WebScrapingConfig.BPS_URL)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            csv_links = {}
            all_links = soup.find_all('a', href=True)
            
            # Look for direct S3 CSV download links
            for link in all_links:
                href = link.get('href')
                
                if self._is_csv_download_link(href):
                    year = self._extract_year_from_url(href)
                    if year and year in years:
                        csv_links[year] = href
                        StatusPrinter.print_info(f"Found {year}: {href}")
            
            # Fallback: construct URLs if not found
            if not csv_links:
                StatusPrinter.print_warning("No links found on page, constructing direct URLs")
                csv_links = self._construct_direct_urls(years)
            
            StatusPrinter.print_success(f"Found {len(csv_links)} download links")
            return csv_links
            
        except Exception as e:
            self.logger.error(f"Failed to discover CSV links: {e}")
            StatusPrinter.print_warning("Discovery failed, using direct URL construction")
            return self._construct_direct_urls(years)
    
    def _is_csv_download_link(self, href: str) -> bool:
        """Check if link is a CSV download URL"""
        if not href:
            return False
        
        return (
            's3.sa-east-1.amazonaws.com' in href and
            '/BPS/csv/' in href and
            '.csv.zip' in href
        )
    
    def _extract_year_from_url(self, url: str) -> Optional[int]:
        """Extract year from CSV download URL"""
        year_match = re.search(r'/(202[0-4])\.csv\.zip', url)
        if year_match:
            return int(year_match.group(1))
        return None
    
    def _construct_direct_urls(self, years: List[int]) -> Dict[int, str]:
        """Construct direct S3 URLs for given years"""
        csv_links = {}
        for year in years:
            url = f"{WebScrapingConfig.S3_BASE_URL}/{year}.csv.zip"
            csv_links[year] = url
            StatusPrinter.print_info(f"Constructed {year}: {url}")
        return csv_links
    
    @timing_decorator
    def download_csv(self, year: int, url: str) -> Optional[Path]:
        """
        Download and extract CSV file for a specific year
        
        Args:
            year: Year of the data
            url: Download URL
            
        Returns:
            Path to downloaded CSV file, None if failed
        """
        filename = f"{year}.csv"
        output_path = self.output_dir / filename
        
        # Skip if already exists and is valid
        if self._is_existing_file_valid(output_path):
            size_mb = FileUtils.get_file_size_mb(output_path)
            StatusPrinter.print_success(f"{filename} already exists ({size_mb:.1f}MB)")
            return output_path
        
        try:
            StatusPrinter.print_info(f"Downloading {filename} from {url}")
            
            # Download the file
            if url.endswith('.zip'):
                return self._download_zip_file(year, url, output_path)
            else:
                return self._download_direct_csv(url, output_path)
                
        except Exception as e:
            self.logger.error(f"Download failed for {year}: {e}")
            StatusPrinter.print_error(f"Download failed: {e}")
            return None
    
    def _is_existing_file_valid(self, file_path: Path) -> bool:
        """Check if existing file is valid"""
        if not file_path.exists():
            return False
        
        return FileUtils.is_file_valid_size(file_path)
    
    def _download_zip_file(self, year: int, url: str, output_path: Path) -> Optional[Path]:
        """Download and extract ZIP file"""
        StatusPrinter.print_info("Extracting ZIP file...")
        
        response = self._make_request(url)
        
        # Download ZIP content with progress tracking
        zip_content = io.BytesIO()
        downloaded_mb = 0
        last_progress_mb = 0
        
        for chunk in response.iter_content(chunk_size=WebScrapingConfig.DOWNLOAD_CHUNK_SIZE):
            if chunk:
                zip_content.write(chunk)
                downloaded_mb += len(chunk) / (1024 * 1024)
                
                # Show progress every 0.1MB
                if downloaded_mb - last_progress_mb >= 0.1:
                    print(f"   📊 Downloaded: {downloaded_mb:.1f}MB")
                    last_progress_mb = downloaded_mb
        
        StatusPrinter.print_success(f"ZIP downloaded: {downloaded_mb:.1f}MB")
        
        # Extract CSV from ZIP
        return self._extract_csv_from_zip(zip_content, output_path, year)
    
    def _extract_csv_from_zip(self, zip_content: io.BytesIO, output_path: Path, year: int) -> Optional[Path]:
        """Extract CSV file from ZIP content"""
        try:
            zip_content.seek(0)
            with zipfile.ZipFile(zip_content, 'r') as zip_file:
                csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                
                if not csv_files:
                    StatusPrinter.print_error("No CSV file found in ZIP")
                    return None
                
                csv_filename = csv_files[0]
                StatusPrinter.print_info(f"Extracting: {csv_filename}")
                
                with zip_file.open(csv_filename) as csv_file:
                    with open(output_path, 'wb') as output_file:
                        output_file.write(csv_file.read())
                
                # Validate extracted file
                if self._validate_downloaded_file(output_path):
                    size_mb = FileUtils.get_file_size_mb(output_path)
                    StatusPrinter.print_success(f"Successfully saved: {output_path.name} ({size_mb:.1f}MB)")
                    return output_path
                else:
                    StatusPrinter.print_error("Extracted file failed validation")
                    return None
                    
        except Exception as e:
            self.logger.error(f"ZIP extraction failed: {e}")
            StatusPrinter.print_error(f"ZIP extraction failed: {e}")
            return None
    
    def _download_direct_csv(self, url: str, output_path: Path) -> Optional[Path]:
        """Download CSV file directly (not zipped)"""
        response = self._make_request(url)
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=WebScrapingConfig.DOWNLOAD_CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
        
        if self._validate_downloaded_file(output_path):
            size_mb = FileUtils.get_file_size_mb(output_path)
            StatusPrinter.print_success(f"Successfully saved: {output_path.name} ({size_mb:.1f}MB)")
            return output_path
        else:
            StatusPrinter.print_error("Downloaded file failed validation")
            return None
    
    def _validate_downloaded_file(self, file_path: Path) -> bool:
        """Validate downloaded file"""
        if not file_path.exists():
            return False
        
        # Check file size
        if not FileUtils.is_file_valid_size(file_path):
            size_mb = FileUtils.get_file_size_mb(file_path)
            self.logger.warning(f"File size outside valid range: {size_mb:.3f}MB")
            if size_mb < FileConfig.MIN_FILE_SIZE_MB:
                file_path.unlink()  # Delete invalid file
                return False
        
        # Quick content validation - check if file starts like a CSV
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                first_line = f.readline().strip()
                # Should have some commas or semicolons (CSV headers)
                if ',' in first_line or ';' in first_line:
                    return True
                else:
                    self.logger.warning(f"File doesn't appear to be CSV: {first_line[:100]}")
                    return False
        except Exception as e:
            self.logger.warning(f"Content validation failed: {e}")
            return True  # Don't fail on encoding issues, let standardization handle it
    
    @timing_decorator
    def download_all_years(self, years: Optional[List[int]] = None) -> Dict[int, Path]:
        """
        Download CSV files for multiple years
        
        Args:
            years: List of years to download (defaults to available years)
            
        Returns:
            Dictionary mapping year to downloaded file path
        """
        years = years or WebScrapingConfig.AVAILABLE_YEARS
        
        StatusPrinter.print_header(f"FAIR-PRICE Data Extraction - {len(years)} Years")
        
        # Discover download links
        csv_links = self.discover_csv_links(years)
        
        if not csv_links:
            StatusPrinter.print_error("No download links found")
            return {}
        
        # Download files
        downloaded_files = {}
        progress = ProgressTracker(len(csv_links), "Downloading files")
        
        for i, (year, url) in enumerate(csv_links.items(), 1):
            StatusPrinter.print_section(f"Processing year {year}")
            
            file_path = self.download_csv(year, url)
            if file_path:
                downloaded_files[year] = file_path
            
            progress.update(1, f"Year {year}")
            
            # Be respectful - pause between downloads
            if i < len(csv_links):
                StatusPrinter.print_info(f"Pausing {WebScrapingConfig.PAUSE_BETWEEN_DOWNLOADS} seconds...")
                time.sleep(WebScrapingConfig.PAUSE_BETWEEN_DOWNLOADS)
        
        progress.finish("All downloads completed")
        
        # Summary
        self._print_download_summary(downloaded_files, csv_links)
        
        return downloaded_files
    
    def _print_download_summary(self, downloaded_files: Dict[int, Path], requested_links: Dict[int, str]) -> None:
        """Print download summary"""
        StatusPrinter.print_header("Download Summary")
        
        total_size_mb = 0
        success_count = len(downloaded_files)
        total_count = len(requested_links)
        
        StatusPrinter.print_info(f"Output directory: {self.output_dir}")
        StatusPrinter.print_success(f"Successfully downloaded: {success_count}/{total_count} files")
        
        if downloaded_files:
            StatusPrinter.print_section("Downloaded files")
            for year in sorted(downloaded_files.keys()):
                file_path = downloaded_files[year]
                size_mb = FileUtils.get_file_size_mb(file_path)
                total_size_mb += size_mb
                print(f"   📄 {year}.csv: {size_mb:.1f}MB")
            
            print(f"\n💾 Total size: {total_size_mb:.1f}MB")
        
        # Show failed downloads
        failed_years = set(requested_links.keys()) - set(downloaded_files.keys())
        if failed_years:
            StatusPrinter.print_section("Failed downloads")
            for year in sorted(failed_years):
                StatusPrinter.print_error(f"{year}.csv - Download failed")
    
    def validate_all_files(self, file_paths: Optional[Dict[int, Path]] = None) -> Dict[int, bool]:
        """
        Validate all downloaded files
        
        Args:
            file_paths: Dictionary of year to file path (defaults to scanning output dir)
            
        Returns:
            Dictionary mapping year to validation result
        """
        if file_paths is None:
            # Scan output directory for CSV files
            csv_files = FileUtils.list_csv_files(self.output_dir)
            file_paths = {}
            for csv_file in csv_files:
                year_match = re.search(r'(\d{4})\.csv$', csv_file.name)
                if year_match:
                    year = int(year_match.group(1))
                    file_paths[year] = csv_file
        
        StatusPrinter.print_section("Validating downloaded files")
        
        validation_results = {}
        for year, file_path in file_paths.items():
            try:
                # Use DataFrameUtils to safely read the file
                from .utils import DataFrameUtils
                df = DataFrameUtils.safe_read_csv(file_path, nrows=3)
                
                if df is not None:
                    validation_results[year] = True
                    StatusPrinter.print_success(f"{year}: {len(df.columns)} columns")
                else:
                    validation_results[year] = False
                    StatusPrinter.print_error(f"{year}: Failed to read file")
                    
            except Exception as e:
                validation_results[year] = False
                StatusPrinter.print_error(f"{year}: Validation failed - {e}")
        
        return validation_results
    
    def get_current_files(self) -> Dict[int, Path]:
        """
        Get currently available files in the output directory
        
        Returns:
            Dictionary mapping year to file path
        """
        csv_files = FileUtils.list_csv_files(self.output_dir)
        current_files = {}
        
        for csv_file in csv_files:
            year_match = re.search(r'(\d{4})\.csv$', csv_file.name)
            if year_match:
                year = int(year_match.group(1))
                current_files[year] = csv_file
        
        return current_files


# Convenience functions for easy usage
def download_single_year(year: int, output_dir: Optional[Path] = None) -> Optional[Path]:
    """
    Download data for a single year
    
    Args:
        year: Year to download
        output_dir: Output directory (defaults to config)
        
    Returns:
        Path to downloaded file, None if failed
    """
    extractor = HealthDataExtractor(output_dir)
    csv_links = extractor.discover_csv_links([year])
    
    if year in csv_links:
        return extractor.download_csv(year, csv_links[year])
    else:
        StatusPrinter.print_error(f"No download link found for year {year}")
        return None


def download_latest_year(output_dir: Optional[Path] = None) -> Optional[Path]:
    """
    Download the latest available year
    
    Args:
        output_dir: Output directory (defaults to config)
        
    Returns:
        Path to downloaded file, None if failed
    """
    latest_year = max(WebScrapingConfig.AVAILABLE_YEARS)
    return download_single_year(latest_year, output_dir)


def download_all_available_years(output_dir: Optional[Path] = None) -> Dict[int, Path]:
    """
    Download all available years
    
    Args:
        output_dir: Output directory (defaults to config)
        
    Returns:
        Dictionary mapping year to downloaded file path
    """
    extractor = HealthDataExtractor(output_dir)
    return extractor.download_all_years()


# Export main classes and functions
__all__ = [
    'HealthDataExtractor',
    'download_single_year',
    'download_latest_year', 
    'download_all_available_years'
]