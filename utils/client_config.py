"""
Client configuration management for different data sources and mappings.
Defines client-specific settings and Snowflake company name mappings.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from utils.config import get_client_specific_config


@dataclass
class ClientConfig:
    """Configuration for a specific client."""

    name: str
    source_type: str  # "google_sheets", "google_drive", "ftp"
    data_types: List[str]
    snowflake_company: str

    # Source-specific configurations
    sheets_config: Optional[Dict[str, str]] = None
    drive_config: Optional[Dict[str, str]] = None
    ftp_config: Optional[Dict[str, Any]] = None


CLIENT_CONFIGS = {
    "mato_grosso": ClientConfig(**get_client_specific_config("mato_grosso")),
    "parana": ClientConfig(**get_client_specific_config("parana")),
    "goias": ClientConfig(**get_client_specific_config("goias")),
}


def get_client_config(client: str) -> ClientConfig:
    """
    Get configuration for a specific client.

    Args:
        client: Client name (mato_grosso, parana, goias)

    Returns:
        ClientConfig object

    Raises:
        ValueError: If client is not supported
    """
    if client not in CLIENT_CONFIGS:
        supported_clients = list(CLIENT_CONFIGS.keys())
        raise ValueError(
            f"Unsupported client: {client}. Supported clients: {supported_clients}"
        )

    return CLIENT_CONFIGS[client]


def get_snowflake_company_name(client: str) -> str:
    """
    Get the Snowflake company name for a client.

    Args:
        client: Client name

    Returns:
        Company name used in Snowflake queries
    """
    config = get_client_config(client)
    return config.snowflake_company


def get_supported_clients() -> Dict[str, Dict[str, Any]]:
    """
    Get information about all supported clients.

    Returns:
        Dictionary with client information for API responses
    """
    clients_info = {}

    for client_name, config in CLIENT_CONFIGS.items():
        clients_info[client_name] = {
            "data_types": config.data_types,
            "source": config.source_type,
        }

    return clients_info


def validate_client_data_type(client: str, data_type: str) -> None:
    """
    Validate that a data type is supported for a client.

    Args:
        client: Client name
        data_type: Data type to validate

    Raises:
        ValueError: If client or data_type is not supported
    """
    config = get_client_config(client)

    if data_type not in config.data_types:
        raise ValueError(
            f"Unsupported data_type '{data_type}' for client '{client}'. "
            f"Supported data_types: {config.data_types}"
        )


def get_join_column_priority() -> List[str]:
    """
    Get the priority list for automatic join column detection.

    Returns:
        List of column name patterns to search for (case-insensitive)
    """
    return ["email"]  # More columns can be added in the future


def find_email_column(columns: List[str]) -> str:
    """
    Find the email column from a list of column names.

    Args:
        columns: List of column names

    Returns:
        Name of the email column

    Raises:
        ValueError: If no email column is found
    """
    priority_patterns = get_join_column_priority()

    for pattern in priority_patterns:
        for col in columns:
            if pattern.lower() in col.lower():
                return col

    raise ValueError(f"No email column found in columns: {columns}")
