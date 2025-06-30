"""
Data extraction module for Fair-Price Brazilian Health Data Pipeline

Handles web scraping of OpenDataSUS portal to download CSV files using BeautifulSoup.
Implements robust extraction with retry logic, progress tracking, and intelligent 
file management for Brazilian health economics data.

Target: https://opendatasus.saude.gov.br/dataset/bps
"""

import requests
from bs4 import BeautifulSoup
import re
import time
import zipfile
import io
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import json

# Import our configuration and logging
from config.settings import get_config
from utils.logger import get_extraction_logger, log_data_operation, log_progress


class OpenDataSUSExtractor:
    """
    Web scraper for OpenDataSUS BPS (Banco de Preços em Saúde) CSV files
    
    Handles:
    - BeautifulSoup-based web scraping of the BPS dataset page
    - ZIP file download and extraction from S3 URLs
    - Intelligent file management and duplicate prevention
    - Progress tracking and error recovery
    - Brazilian health data specific parsing
    """
    
    def __init__(self):
        self.config = get_config()
        self.logger = get_extraction_logger()
        
        # Setup extraction configuration
        self.opendatasus_config = self.config.opendatasus_config
        self.base_url = self.opendatasus_config["base_url"]
        self.bps_url = self.opendatasus_config["bps_url"]
        self.output_dir = self.config.raw_data_dir
        
        # Setup HTTP session with proper headers
        self.session = requests.Session()
        self.session.headers.update(self.opendatasus_config["headers"])
        
        self.logger.info(f"🏥 OpenDataSUS Extractor initialized")
        self.logger.info(f"📁 Output directory: {self.output_dir}")
        self.logger.info(f"🎯 Target years: {self.config.target_years}")
    
    def validate_connection(self) -> bool:
        """
        Validate connection to OpenDataSUS portal
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.logger.info("🔍 Validating connection to OpenDataSUS...")
            
            response = self.session.get(
                self.bps_url, 
                timeout=self.opendatasus_config["timeout"]
            )
            response.raise_for_status()
            
            # Check if we got a valid HTML page
            if "dataset/bps" in response.url and len(response.content) > 1000:
                self.logger.info("✅ Connection to OpenDataSUS validated successfully")
                return True
            else:
                self.logger.error(f"❌ Invalid response from OpenDataSUS: {response.url}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Connection validation failed: {str(e)}")
            return False
    
    @log_data_operation(get_extraction_logger(), "CSV download links discovery")
    def discover_csv_download_links(self, years: Optional[List[int]] = None) -> Dict[int, str]:
        """
        Scrape the BPS page to discover CSV download links
        
        Args:
            years: Years to look for (default: from config)
            
        Returns:
            Dictionary mapping year to download URL
        """
        if years is None:
            years = self.config.target_years
        
        self.logger.info(f"🔍 Scraping BPS page for CSV links...")
        
        try:
            # Get the main BPS dataset page
            response = self.session.get(
                self.bps_url, 
                timeout=self.opendatasus_config["timeout"]
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find CSV download links using multiple strategies
            csv_links = {}
            all_links = soup.find_all('a', href=True)
            
            self.logger.info(f"📋 Found {len(all_links)} total links on page")
            
            # Strategy 1: Look for direct S3 CSV download links
            for link in all_links:
                href = link.get('href')
                
                # Check for S3 CSV links
                if (href and 
                    's3.sa-east-1.amazonaws.com' in href and 
                    '/BPS/csv/' in href and 
                    '.csv.zip' in href):
                    
                    # Extract year from URL
                    year_match = re.search(self.config.csv_patterns["year_pattern"], href)
                    if year_match:
                        year = int(year_match.group(1))
                        if year in years:
                            csv_links[year] = href
                            self.logger.info(f"   📅 Found {year}: {href}")
            
            # Strategy 2: Look for year-specific dataset links and follow them
            if len(csv_links) < len(years):
                self.logger.info("🔍 Searching for year-specific dataset links...")
                
                # Find links that contain years in text
                for link in all_links:
                    link_text = link.get_text(strip=True)
                    href = link.get('href')
                    
                    # Look for patterns like "Banco de Preço de Saúde - 2024"
                    for year in years:
                        if (str(year) in link_text and 
                            ('banco' in link_text.lower() or 'bps' in link_text.lower() or 'preço' in link_text.lower()) and
                            year not in csv_links):
                            
                            # Try to find the actual CSV download from this page
                            if href and not href.startswith('http'):
                                href = self.base_url + href
                            
                            csv_url = self._find_csv_from_dataset_page(href, year)
                            if csv_url:
                                csv_links[year] = csv_url
                                self.logger.info(f"   📅 Found {year} via dataset page: {csv_url}")
            
            # Strategy 3: Use known S3 URL pattern (fallback from your working code)
            if len(csv_links) < len(years):
                missing_years = [year for year in years if year not in csv_links]
                self.logger.warning(f"⚠️  Missing links for years: {missing_years}")
                self.logger.info("🔧 Using known S3 URL pattern...")
                
                for year in missing_years:
                    # This is the pattern that worked in your extraction notebook
                    fallback_url = f"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/BPS/csv/{year}.csv.zip"
                    csv_links[year] = fallback_url
                    self.logger.info(f"   🔨 Constructed {year}: {fallback_url}")
            
            self.logger.info(f"✅ Discovered {len(csv_links)} CSV download links")
            return csv_links
            
        except Exception as e:
            self.logger.error(f"❌ Failed to discover CSV links: {str(e)}")
            raise
    
    def _find_csv_from_dataset_page(self, dataset_url: str, year: int) -> Optional[str]:
        """
        Follow a dataset page link to find the actual CSV download URL
        
        Args:
            dataset_url: URL of the dataset page
            year: Year we're looking for
            
        Returns:
            CSV download URL if found, None otherwise
        """
        try:
            if not dataset_url or 'http' not in dataset_url:
                return None
                
            self.logger.debug(f"   🔍 Checking dataset page for {year}: {dataset_url}")
            
            response = self.session.get(dataset_url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for CSV download links on this page
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                
                if (href and 
                    's3.sa-east-1.amazonaws.com' in href and 
                    '.csv.zip' in href and 
                    str(year) in href):
                    return href
            
            return None
            
        except Exception as e:
            self.logger.debug(f"   ⚠️  Failed to check dataset page for {year}: {str(e)}")
            return None
    
    def _validate_csv_url(self, year: int, url: str) -> bool:
        """
        Validate that a CSV URL is accessible
        
        Args:
            year: Year of the CSV
            url: URL to validate
            
        Returns:
            True if URL is accessible, False otherwise
        """
        try:
            # Make a HEAD request to check if URL exists
            response = self.session.head(url, timeout=10)
            
            if response.status_code == 200:
                # Check content type if available
                content_type = response.headers.get('content-type', '').lower()
                content_length = response.headers.get('content-length')
                
                self.logger.debug(f"   📊 {year} URL validation: {response.status_code}, "
                                f"type: {content_type}, size: {content_length}")
                return True
            else:
                self.logger.warning(f"   ⚠️  {year} URL returned status: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.warning(f"   ❌ {year} URL validation failed: {str(e)}")
            return False
    
    @log_data_operation(get_extraction_logger(), "CSV file download and extraction")
    def download_and_extract_csv(self, year: int, url: str) -> Optional[Path]:
        """
        Download and extract a single CSV file from ZIP
        
        Args:
            year: Year of the data
            url: Download URL
            
        Returns:
            Path to extracted CSV file or None if failed
        """
        filename = self.config.get_raw_csv_filename(year)
        output_path = self.output_dir / filename
        
        # Skip if already exists and is reasonable size
        if output_path.exists():
            size_mb = output_path.stat().st_size / (1024 * 1024)
            if size_mb > 0.1:  # At least 100KB
                self.logger.info(f"   ✅ {filename} already exists ({size_mb:.1f}MB)")
                return output_path
        
        try:
            self.logger.info(f"📥 Downloading {filename} from {url}")
            
            # Download with progress tracking
            response = self.session.get(url, stream=True, timeout=self.opendatasus_config["timeout"])
            response.raise_for_status()
            
            # Handle ZIP file extraction
            if url.endswith('.zip'):
                self._extract_zip_to_csv(response, year, output_path)
            else:
                # Direct CSV download (if any)
                self._save_direct_csv(response, output_path)
            
            # Validate downloaded file
            if output_path.exists():
                size_mb = output_path.stat().st_size / (1024 * 1024)
                
                if size_mb < 0.01:  # Less than 10KB is probably an error
                    self.logger.error(f"   ❌ File too small: {size_mb:.3f}MB")
                    output_path.unlink()
                    return None
                
                self.logger.info(f"   ✅ Successfully saved: {filename} ({size_mb:.1f}MB)")
                return output_path
            else:
                self.logger.error(f"   ❌ File was not created: {filename}")
                return None
                
        except Exception as e:
            self.logger.error(f"   ❌ Download failed for {filename}: {str(e)}")
            return None
    
    def _extract_zip_to_csv(self, response: requests.Response, year: int, output_path: Path):
        """
        Extract CSV from ZIP response
        
        Args:
            response: HTTP response containing ZIP data
            year: Year for logging
            output_path: Where to save the extracted CSV
        """
        self.logger.info(f"   📦 Extracting ZIP file for {year}...")
        
        # Download ZIP content to memory with progress
        zip_content = io.BytesIO()
        downloaded_mb = 0
        
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                zip_content.write(chunk)
                downloaded_mb += len(chunk) / (1024 * 1024)
                
                # Log progress every 1MB
                if int(downloaded_mb) % 1 == 0 and downloaded_mb > 0:
                    self.logger.debug(f"   📊 Downloaded: {downloaded_mb:.1f}MB")
        
        self.logger.info(f"   ✅ ZIP downloaded: {downloaded_mb:.1f}MB")
        
        # Extract CSV from ZIP
        zip_content.seek(0)
        with zipfile.ZipFile(zip_content, 'r') as zip_file:
            csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
            
            if not csv_files:
                raise Exception("No CSV file found in ZIP")
            
            csv_filename = csv_files[0]
            self.logger.info(f"   📄 Extracting: {csv_filename}")
            
            with zip_file.open(csv_filename) as csv_file:
                with open(output_path, 'wb') as output_file:
                    output_file.write(csv_file.read())
    
    def _save_direct_csv(self, response: requests.Response, output_path: Path):
        """
        Save CSV directly from response
        
        Args:
            response: HTTP response containing CSV data
            output_path: Where to save the CSV
        """
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    
    def check_existing_files(self) -> Dict[int, Dict[str, any]]:
        """
        Check what files already exist and their status
        
        Returns:
            Dictionary with file status for each year
        """
        file_status = {}
        
        for year in self.config.target_years:
            filename = self.config.get_raw_csv_filename(year)
            file_path = self.output_dir / filename
            
            if file_path.exists():
                stat = file_path.stat()
                file_status[year] = {
                    'exists': True,
                    'size_mb': stat.st_size / (1024 * 1024),
                    'modified': datetime.fromtimestamp(stat.st_mtime),
                    'path': file_path
                }
            else:
                file_status[year] = {
                    'exists': False,
                    'size_mb': 0,
                    'modified': None,
                    'path': file_path
                }
        
        return file_status
    
    @log_data_operation(get_extraction_logger(), "Complete multi-year extraction")
    def extract_all_years(self, years: Optional[List[int]] = None, force_redownload: bool = False) -> Dict[int, Optional[Path]]:
        """
        Extract CSV files for all specified years
        
        Args:
            years: Years to extract (default: from config)
            force_redownload: Whether to redownload existing files
            
        Returns:
            Dictionary mapping year to file path (None if failed)
        """
        if years is None:
            years = self.config.target_years
        
        self.logger.info(f"🚀 Starting extraction for years: {years}")
        
        # Validate connection first
        if not self.validate_connection():
            raise Exception("Cannot connect to OpenDataSUS portal")
        
        # Check existing files
        if not force_redownload:
            existing_files = self.check_existing_files()
            self.logger.info("📋 Existing files status:")
            for year, status in existing_files.items():
                if year in years:
                    if status['exists']:
                        self.logger.info(f"   ✅ {year}: {status['size_mb']:.1f}MB (modified: {status['modified'].strftime('%Y-%m-%d %H:%M')})")
                    else:
                        self.logger.info(f"   ❌ {year}: Not found")
        
        # Discover download links
        csv_links = self.discover_csv_download_links(years)
        
        if not csv_links:
            raise Exception("No CSV download links discovered")
        
        # Validate URLs before downloading
        self.logger.info("🔍 Validating download URLs...")
        valid_links = {}
        for year, url in csv_links.items():
            if self._validate_csv_url(year, url):
                valid_links[year] = url
            else:
                self.logger.warning(f"⚠️  Skipping invalid URL for {year}")
        
        if not valid_links:
            raise Exception("No valid download URLs found")
        
        # Download files with progress tracking
        downloaded_files = {}
        
        with log_progress(self.logger, len(valid_links), "Downloading CSV files") as progress:
            for i, (year, url) in enumerate(valid_links.items(), 1):
                
                # Skip if file exists and not forcing redownload
                if not force_redownload:
                    filename = self.config.get_raw_csv_filename(year)
                    existing_path = self.output_dir / filename
                    if existing_path.exists() and existing_path.stat().st_size > 100000:  # > 100KB
                        self.logger.info(f"   ⏭️  Skipping {year} (file exists)")
                        downloaded_files[year] = existing_path
                        progress(i)
                        continue
                
                # Download the file
                file_path = self.download_and_extract_csv(year, url)
                if file_path:
                    downloaded_files[year] = file_path
                
                # Be respectful to the server
                delay = self.opendatasus_config["request_delay"]
                if i < len(valid_links):  # Don't delay after last download
                    self.logger.debug(f"   ⏸️  Pausing {delay} seconds...")
                    time.sleep(delay)
                
                progress(i)
        
        # Summary
        success_count = len(downloaded_files)
        total_count = len(years)
        
        self.logger.info(f"✅ Extraction completed: {success_count}/{total_count} files")
        
        if downloaded_files:
            total_size = sum(
                path.stat().st_size / (1024 * 1024) 
                for path in downloaded_files.values() 
                if path and path.exists()
            )
            self.logger.info(f"💾 Total downloaded: {total_size:.1f}MB")
            
            # Log individual file info
            for year, path in downloaded_files.items():
                if path and path.exists():
                    size_mb = path.stat().st_size / (1024 * 1024)
                    self.logger.info(f"   📄 {year}: {size_mb:.1f}MB")
        
        return downloaded_files
    
    def get_extraction_summary(self) -> Dict[str, any]:
        """
        Get summary of current extraction status
        
        Returns:
            Dictionary with extraction summary
        """
        file_status = self.check_existing_files()
        
        total_files = len(self.config.target_years)
        existing_files = sum(1 for status in file_status.values() if status['exists'])
        total_size_mb = sum(status['size_mb'] for status in file_status.values())
        
        summary = {
            'total_target_files': total_files,
            'existing_files': existing_files,
            'missing_files': total_files - existing_files,
            'total_size_mb': total_size_mb,
            'completion_percentage': (existing_files / total_files) * 100 if total_files > 0 else 0,
            'files_by_year': file_status,
            'output_directory': str(self.output_dir),
            'last_check': datetime.now().isoformat()
        }
        
        return summary
    
    def save_extraction_metadata(self, downloaded_files: Dict[int, Optional[Path]]):
        """
        Save metadata about the extraction process
        
        Args:
            downloaded_files: Dictionary of downloaded files by year
        """
        metadata = {
            'extraction_timestamp': datetime.now().isoformat(),
            'target_years': self.config.target_years,
            'successful_downloads': list(downloaded_files.keys()),
            'failed_downloads': [year for year in self.config.target_years if year not in downloaded_files],
            'file_details': {},
            'extractor_config': {
                'base_url': self.base_url,
                'bps_url': self.bps_url,
                'output_dir': str(self.output_dir)
            }
        }
        
        # Add file details
        for year, path in downloaded_files.items():
            if path and path.exists():
                stat = path.stat()
                metadata['file_details'][year] = {
                    'filename': path.name,
                    'size_bytes': stat.st_size,
                    'size_mb': stat.st_size / (1024 * 1024),
                    'created': datetime.fromtimestamp(stat.st_ctime).isoformat(),
                    'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
                }
        
        # Save metadata
        metadata_path = self.output_dir / 'extraction_metadata.json'
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"💾 Extraction metadata saved: {metadata_path}")


# Convenience functions for easy usage
def extract_health_data(years: Optional[List[int]] = None, force_redownload: bool = False) -> Dict[int, Optional[Path]]:
    """
    Simple function to extract Brazilian health data CSV files
    
    Args:
        years: Years to extract (default: 2020-2024)
        force_redownload: Whether to redownload existing files
        
    Returns:
        Dictionary mapping year to downloaded file path
    """
    extractor = OpenDataSUSExtractor()
    return extractor.extract_all_years(years, force_redownload)


def get_extraction_status() -> Dict[str, any]:
    """
    Get current extraction status summary
    
    Returns:
        Dictionary with extraction summary
    """
    extractor = OpenDataSUSExtractor()
    return extractor.get_extraction_summary()


def validate_extraction_setup() -> bool:
    """
    Validate that extraction can run successfully
    
    Returns:
        True if setup is valid, False otherwise
    """
    try:
        extractor = OpenDataSUSExtractor()
        return extractor.validate_connection()
    except Exception:
        return False


# Example usage and testing
if __name__ == "__main__":
    # Test the extraction setup
    print("🏥 Fair-Price Brazilian Health Data Extractor")
    print("=" * 60)
    
    # Validate setup
    print("🔍 Validating extraction setup...")
    if validate_extraction_setup():
        print("✅ Extraction setup validated")
        
        # Get current status
        status = get_extraction_status()
        print(f"\n📊 Current status:")
        print(f"   Files: {status['existing_files']}/{status['total_target_files']}")
        print(f"   Size: {status['total_size_mb']:.1f}MB")
        print(f"   Completion: {status['completion_percentage']:.1f}%")
        
        # Test extraction (just discovery, no download)
        extractor = OpenDataSUSExtractor()
        links = extractor.discover_csv_download_links([2024])  # Test with just 2024
        print(f"\n🔗 Test link discovery: {len(links)} links found")
        
    else:
        print("❌ Extraction setup validation failed")
    
    print("\n🎯 Ready for extraction! Use extract_health_data() to download files.")