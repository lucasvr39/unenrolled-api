"""
Environment configuration management for the Unenrolled API.
Handles loading and validation of environment variables for different services.
"""

import os
from typing import Any, Dict

from dotenv import load_dotenv

load_dotenv()


def get_snowflake_config() -> Dict[str, Any]:
    """Get Snowflake connection configuration from environment variables."""
    return {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "default_table": os.getenv("SNOWFLAKE_DEFAULT_TABLE"),
    }


def get_google_api_config() -> Dict[str, Any]:
    """Get Google API credentials configuration."""
    return {
        "type": "service_account",
        "project_id": "hardy-lightning-445314-c5",
        "private_key_id": os.getenv("GOOGLE_PRIVATE_KEY_ID"),
        "private_key": os.getenv("GOOGLE_PRIVATE_KEY"),
        "client_email": os.getenv("GOOGLE_CLIENT_EMAIL"),
        "client_id": os.getenv("GOOGLE_CLIENT_ID"),
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": os.getenv("GOOGLE_CLIENT_X509_CERT_URL"),
        "universe_domain": "googleapis.com",
    }


def get_ftp_config() -> Dict[str, Any]:
    """Get FTP connection configuration."""
    return {
        "host": os.getenv("FTP_HOST"),
        "user": os.getenv("FTP_USER"),
        "password": os.getenv("FTP_PASSWORD"),
        "port": int(os.getenv("FTP_PORT", "21")),
    }


def get_client_specific_config(client: str) -> Dict[str, Any]:
    """Get client-specific configuration from environment variables."""
    config = {}

    if client == "mato_grosso":
        config.update(
            {
                "name": "mato_grosso",
                "source_type": "google_sheets",
                "data_types": ["students", "teachers"],
                "snowflake_company": "SEDUC-MT: Mato Grosso",
                "sheets_config": {
                    "students": os.getenv("MATO_GROSSO_STUDENTS_SHEET_ID"),
                    "teachers": os.getenv("MATO_GROSSO_TEACHERS_SHEET_ID"),
                },
            }
        )
    elif client == "parana":
        config.update(
            {
                "name": "parana",
                "source_type": "google_drive",
                "data_types": ["students", "teachers"],
                "snowflake_company": "SEED-PR: Parana",
                "drive_config": {
                    "folder_id": os.getenv("PARANA_DRIVE_FOLDER_ID"),
                    "students_pattern": "CARGA_ESTUDANTES",
                    "teachers_pattern": "CARGA_PROFESSORES",
                },
            }
        )
    elif client == "goias":
        config.update(
            {
                "name": "goias",
                "source_type": "ftp",
                "data_types": [
                    "students",
                    "teachers",
                    "teachers_tec",
                    "teachers_with_gls",
                    "other_components",
                ],
                "snowflake_company": "SEDUC-GO: Goias",
                "ftp_config": {
                    "folder": "ftp_goenglish",
                    "students_pattern": "relatorio_go_english_alunos",
                    "teachers_pattern": "relatorio_go_english_professores_sem_aula_ao_vivo",
                    "other_components_pattern": "relatorio_go_english_servidores",
                    "teachers_tec_pattern": "relatorio_go_english_goias_tec_ao_vivo",
                    "teachers_with_gls_pattern": "relatorio_go_english_professores",
                },
            },
        )

    return config


def validate_config() -> None:
    """Validate that required environment variables are set."""
    snowflake_config = get_snowflake_config()
    required_snowflake = [
        "account",
        "user",
        "password",
        "warehouse",
        "database",
        "default_table",
    ]

    missing_vars = []
    for var in required_snowflake:
        if not snowflake_config.get(var):
            missing_vars.append(f"SNOWFLAKE_{var.upper()}")

    google_config = get_google_api_config()
    required_google = [
        "private_key_id",
        "private_key",
        "client_email",
        "client_id",
        "client_x509_cert_url",
    ]

    for var in required_google:
        if not google_config.get(var):
            missing_vars.append(f"GOOGLE_{var.upper()}")

    if missing_vars:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )
