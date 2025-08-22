"""
External data fetching strategy implementation.
Supports Google Sheets, Google Drive, and FTP data sources.
"""

import ftplib
import io
from abc import ABC, abstractmethod
from typing import Any, Dict, List

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from .client_config import get_client_config
from .config import get_ftp_config, get_google_api_config
from .logging_config import get_logger

logger = get_logger(__name__)


class ExternalDataFetcher(ABC):
    """Abstract base class for external data fetchers."""

    @abstractmethod
    def fetch_data(self, client: str, data_type: str) -> pd.DataFrame:
        """
        Fetch data for a specific client and data type.

        Args:
            client: Client name
            data_type: Type of data to fetch

        Returns:
            DataFrame with fetched data
        """
        pass


class GoogleSheetsFetcher(ExternalDataFetcher):
    """Fetcher for Google Sheets data."""

    def __init__(self):
        """Initialize Google Sheets client."""
        self.credentials = self._get_credentials()
        self.client = gspread.authorize(self.credentials)

    def _get_credentials(self) -> Credentials:
        """Get Google API credentials."""
        config = get_google_api_config()

        # Convert private key format
        if config.get("private_key"):
            config["private_key"] = config["private_key"].replace("\\n", "\n")

        return Credentials.from_service_account_info(
            config,
            scopes=[
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ],
        )

    def fetch_data(self, client: str, data_type: str) -> pd.DataFrame:
        """
        Fetch data from Google Sheets.

        Args:
            client: Client name (should be mato_grosso for sheets)
            data_type: students or teachers

        Returns:
            DataFrame with sheet data
        """
        config = get_client_config(client)

        if config.source_type != "google_sheets":
            raise ValueError(f"Client {client} is not configured for Google Sheets")

        if not hasattr(config, "sheets_config") or config.sheets_config is None:
            raise ValueError(f"No sheets_config configured for {client}")

        sheet_id = config.sheets_config.get(data_type)
        if not sheet_id:
            raise ValueError(f"No sheet ID configured for {client} {data_type}")

        logger.info(
            f"Fetching Google Sheet data for {client} {data_type}, sheet_id: {sheet_id}"
        )

        try:
            worksheet = self.client.open_by_key(sheet_id).sheet1
            data = worksheet.get_all_records()

            df = pd.DataFrame(data)
            logger.info(f"Fetched {len(df)} records from Google Sheets")

            return df

        except Exception as e:
            logger.error(f"Failed to fetch Google Sheets data: {str(e)}")
            raise


class GoogleDriveFetcher(ExternalDataFetcher):
    """Fetcher for Google Drive files."""

    def __init__(self):
        """Initialize Google Drive client."""
        self.credentials = self._get_credentials()
        self.service = build("drive", "v3", credentials=self.credentials)

    def _get_credentials(self) -> Credentials:
        """Get Google API credentials."""
        config = get_google_api_config()

        # Convert private key format
        if config.get("private_key"):
            config["private_key"] = config["private_key"].replace("\\n", "\n")

        return Credentials.from_service_account_info(
            config, scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )

    def fetch_data(self, client: str, data_type: str) -> pd.DataFrame:
        """
        Fetch data from Google Drive files.

        Args:
            client: Client name (should be parana for drive)
            data_type: students or teachers

        Returns:
            DataFrame with file data
        """
        config = get_client_config(client)

        if config.source_type != "google_drive":
            raise ValueError(f"Client {client} is not configured for Google Drive")

        drive_config = getattr(config, "drive_config", None)
        if not drive_config or not isinstance(drive_config, dict):
            raise ValueError(f"No Drive configuration for {client} {data_type}")

        folder_id = drive_config.get("folder_id")
        pattern = drive_config.get(f"{data_type}_pattern")

        if not folder_id or not pattern:
            raise ValueError(f"No Drive configuration for {client} {data_type}")

        logger.info(
            f"Fetching Google Drive data for {client} {data_type}, pattern: {pattern}"
        )

        try:
            # Find files matching pattern in folder
            query = f"'{folder_id}' in parents and name contains '{pattern}'"
            results = self.service.files().list(q=query).execute()
            files = results.get("files", [])

            if not files:
                raise ValueError(
                    f"No files found matching pattern '{pattern}' in folder {folder_id}"
                )

            # Use the first matching file (most recent would be better with ordering)
            file_id = files[0]["id"]
            logger.info(f"Found file: {files[0]['name']}")

            # Download file content
            file_content = self.service.files().get_media(fileId=file_id).execute()

            # Convert to DataFrame (assuming CSV format)
            df = pd.read_csv(io.StringIO(file_content.decode("utf-8")))
            logger.info(f"Fetched {len(df)} records from Google Drive file")

            return df

        except Exception as e:
            logger.error(f"Failed to fetch Google Drive data: {str(e)}")
            raise


class FTPFetcher(ExternalDataFetcher):
    """Fetcher for FTP server data."""

    def __init__(self):
        """Initialize FTP configuration."""
        self.config = get_ftp_config()
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate FTP configuration."""
        required_fields = ["host", "user", "password"]
        missing_fields = [
            field for field in required_fields if not self.config.get(field)
        ]

        if missing_fields:
            raise ValueError(f"Missing FTP configuration: {', '.join(missing_fields)}")

    def fetch_data(self, client: str, data_type: str) -> pd.DataFrame:
        """
        Fetch data from FTP server.

        Args:
            client: Client name (should be goias for FTP)
            data_type: students, teachers, or teachers_with_gls

        Returns:
            DataFrame with file data
        """
        config = get_client_config(client)

        if config.source_type != "ftp":
            raise ValueError(f"Client {client} is not configured for FTP")

        logger.info(f"Fetching FTP data for {client} {data_type}")

        try:
            with ftplib.FTP() as ftp:
                ftp.connect(self.config["host"], self.config["port"])
                ftp.login(self.config["user"], self.config["password"])

                # Navigate to specified folder if configured
                if self.config.get("folder"):
                    ftp.cwd(self.config["folder"])
                    logger.info(f"Changed to FTP folder: {self.config['folder']}")

                # List files in the directory
                file_list = ftp.nlst()
                logger.info(f"Found {len(file_list)} files on FTP server")

                # Find matching files based on patterns
                if not hasattr(config, "ftp_config") or config.ftp_config is None:
                    raise ValueError(
                        f"No FTP configuration found for {client} {data_type}"
                    )

                matching_files = self._find_matching_files(
                    file_list, data_type, config.ftp_config
                )

                if not matching_files:
                    raise ValueError(f"No files found matching pattern for {data_type}")

                # Download the first matching file
                target_file = matching_files[0]
                logger.info(f"Downloading file: {target_file}")

                file_data = io.BytesIO()
                ftp.retrbinary(f"RETR {target_file}", file_data.write)
                file_data.seek(0)

                # Convert to DataFrame (assuming CSV format) - encoding should be latin -1
                # otherwise it will return 'utf-8' codec can't decode byte 0xf3 in position 29: invalid continuation
                # and also sep = ';' because the file is separated by semicolon
                encoding = "latin-1"
                sep = ";"
                df = pd.read_csv(file_data, encoding=encoding, sep=sep)
                logger.info(f"Fetched {len(df)} records from FTP file")

                return df

        except Exception as e:
            logger.error(f"Failed to fetch FTP data: {str(e)}")
            raise

    def _find_matching_files(
        self, file_list: List[str], data_type: str, ftp_config: Dict[str, Any]
    ) -> List[str]:
        """Find files matching the pattern for a specific data type."""
        if data_type == "students":
            pattern = ftp_config.get("students_pattern")
            if pattern is None:
                return []
            return [f for f in file_list if pattern.lower() in f.lower()]

        elif data_type == "teachers":
            exclude_patterns = ftp_config.get("teachers_pattern", {}).get("exclude", [])
            matching_files = []

            for filename in file_list:
                filename_lower = filename.lower()
                should_exclude = any(
                    pattern.lower() in filename_lower for pattern in exclude_patterns
                )
                if not should_exclude:
                    matching_files.append(filename)

            return matching_files

        elif data_type == "teachers_with_gls":
            exclude_patterns = ftp_config.get("teachers_with_gls_pattern", {}).get("exclude", [])
            matching_files = []

            for filename in file_list:
                filename_lower = filename.lower()
                should_exclude = any(
                    pattern.lower() in filename_lower for pattern in exclude_patterns
                )
                if not should_exclude:
                    matching_files.append(filename)

            return matching_files

        return []


class DataFetcherFactory:
    """Factory for creating appropriate data fetchers."""

    @staticmethod
    def get_fetcher(source_type: str) -> ExternalDataFetcher:
        """
        Get the appropriate data fetcher for a source type.

        Args:
            source_type: Type of data source

        Returns:
            ExternalDataFetcher instance
        """
        if source_type == "google_sheets":
            return GoogleSheetsFetcher()
        elif source_type == "google_drive":
            return GoogleDriveFetcher()
        elif source_type == "ftp":
            return FTPFetcher()
        else:
            raise ValueError(f"Unsupported source type: {source_type}")


def fetch_external_data(client: str, data_type: str) -> pd.DataFrame:
    """
    Fetch external data for a specific client and data type.

    Args:
        client: Client name
        data_type: Type of data to fetch

    Returns:
        DataFrame with external data
    """
    config = get_client_config(client)
    fetcher = DataFetcherFactory.get_fetcher(config.source_type)

    return fetcher.fetch_data(client, data_type)
