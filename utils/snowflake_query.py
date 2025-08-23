"""
Snowflake database client for querying enrollment data.
Handles connection management and executes the standard enrollment query.
"""

from contextlib import contextmanager
from typing import List, Optional

import pandas as pd
import snowflake.connector

from .config import get_snowflake_config
from .logging_config import get_logger

logger = get_logger(__name__)

# Module-level cache for enrollment data
_cached_enrollment_data: Optional[pd.DataFrame] = None


class SnowflakeClient:
    """Client for connecting to Snowflake and executing enrollment queries."""

    def __init__(self):
        """Initialize the Snowflake client with configuration."""
        self.config = get_snowflake_config()
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate that all required Snowflake configuration is present."""
        required_fields = [
            "account",
            "user",
            "password",
            "warehouse",
            "database",
            "default_table",
        ]
        missing_fields = [
            field for field in required_fields if not self.config.get(field)
        ]

        if missing_fields:
            raise ValueError(
                f"Missing Snowflake configuration: {', '.join(missing_fields)}"
            )

    @contextmanager
    def _get_connection(self):
        """Context manager for Snowflake connections."""
        connection = None
        try:
            logger.debug(f"Connecting to Snowflake account: {self.config['account']}")
            connection = snowflake.connector.connect(
                account=self.config["account"],
                user=self.config["user"],
                password=self.config["password"],
                warehouse=self.config["warehouse"],
                database=self.config["database"],
            )
            yield connection
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise
        finally:
            if connection:
                connection.close()
                logger.debug("Snowflake connection closed")

    def query_enrollment_by_client(
        self, client_name: str, table_name: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Query Snowflake for enrollment data filtered by client company name.

        Args:
            client_name: The company name to filter by in Snowflake
            table_name: Optional table name, uses default if not specified

        Returns:
            DataFrame with enrollment data containing Email column

        Raises:
            Exception: If query execution fails
        """
        if not table_name:
            table_name = self.config["default_table"]

        query = f"""
        SELECT DISTINCT 
            "Email"
        FROM 
            {table_name}
        WHERE 
            "Company" = %(client)s
        AND
            UPPER("Course status") = 'ACTIVE'
        """

        logger.info(
            f"Querying Snowflake for client: {client_name}, table: {table_name}"
        )

        with self._get_connection() as connection:
            cursor = None
            try:
                cursor = connection.cursor()
                cursor.execute(query, {"client": client_name})

                # Fetch results and convert to DataFrame
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                df = pd.DataFrame(results, columns=columns)
                logger.info(f"Retrieved {len(df)} enrollment records from Snowflake")

                return df

            except Exception as e:
                logger.error(f"Failed to execute Snowflake query: {str(e)}")
                raise
            finally:
                if cursor is not None:
                    cursor.close()

    def test_connection(self) -> bool:
        """
        Test the Snowflake connection.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self._get_connection() as connection:
                cursor = connection.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                logger.info("Snowflake connection test successful")
                return result is not None and result[0] == 1
        except Exception as e:
            logger.error(f"Snowflake connection test failed: {str(e)}")
            return False

    def query_all_companies(self, company_names: List[str], table_name: Optional[str] = None) -> pd.DataFrame:
        """
        Query Snowflake for enrollment data for all specified companies in a single query.

        Args:
            company_names: List of company names to filter by in Snowflake
            table_name: Optional table name, uses default if not specified

        Returns:
            DataFrame with enrollment data containing Email and Company columns

        Raises:
            Exception: If query execution fails
        """
        if not table_name:
            table_name = self.config["default_table"]

        # Create placeholders for IN clause
        placeholders = ", ".join([f"%({i})s" for i in range(len(company_names))])
        
        query = f"""
        SELECT DISTINCT 
            "Email",
            "Company"
        FROM 
            {table_name}
        WHERE 
            "Company" IN ({placeholders})
        AND
            UPPER("Course status") = 'ACTIVE'
        """

        # Create parameters dict for query
        params = {str(i): company for i, company in enumerate(company_names)}

        logger.info(
            f"Querying Snowflake for all companies: {company_names}, table: {table_name}"
        )

        with self._get_connection() as connection:
            cursor = None
            try:
                cursor = connection.cursor()
                cursor.execute(query, params)

                # Fetch results and convert to DataFrame
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                df = pd.DataFrame(results, columns=columns)
                logger.info(f"Retrieved {len(df)} enrollment records from Snowflake for all companies")

                return df

            except Exception as e:
                logger.error(f"Failed to execute Snowflake query for all companies: {str(e)}")
                raise
            finally:
                if cursor is not None:
                    cursor.close()


def get_cached_enrollment_data() -> pd.DataFrame:
    """
    Get enrollment data for all companies, using module-level cache if available.
    
    Returns:
        DataFrame with enrollment data for all configured companies
        
    Raises:
        Exception: If data fetching fails
    """
    global _cached_enrollment_data
    
    if _cached_enrollment_data is None or _cached_enrollment_data.empty:
        logger.info("Cache empty, fetching enrollment data for all companies")
        
        # Import here to avoid circular imports
        from .client_config import CLIENT_CONFIGS
        
        # Get all company names from client configurations
        all_companies = [config.snowflake_company for config in CLIENT_CONFIGS.values()]
        
        # Fetch data using SnowflakeClient
        client = SnowflakeClient()
        _cached_enrollment_data = client.query_all_companies(all_companies)
        
        logger.info("Successfully cached enrollment data for all companies")
    else:
        logger.debug("Using cached enrollment data")
    
    return _cached_enrollment_data.copy()


def get_client_enrollment_data(client_company_name: str) -> pd.DataFrame:
    """
    Get enrollment data for a specific client company from cached data.
    
    Args:
        client_company_name: The company name used in Snowflake queries
        
    Returns:
        DataFrame with enrollment data filtered for the specific client
        
    Raises:
        Exception: If data fetching fails
    """
    all_data = get_cached_enrollment_data()
    client_data = all_data[all_data['Company'] == client_company_name].copy()
    
    logger.debug(f"Filtered {len(client_data)} enrollment records for company: {client_company_name}")
    return client_data


def clear_enrollment_cache() -> None:
    """Clear the module-level enrollment data cache."""
    global _cached_enrollment_data
    _cached_enrollment_data = None
    logger.info("Enrollment data cache cleared")
