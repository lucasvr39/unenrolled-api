"""
Core business logic for identifying unenrolled users.
Implements anti-join operations using pandas to find users in external data but not in Snowflake.
"""

from datetime import datetime
from typing import Any, Dict

import pandas as pd

from .client_config import find_email_column, get_snowflake_company_name
from .fetch_external import fetch_external_data
from .logging_config import get_logger
from .snowflake_query import get_client_enrollment_data

logger = get_logger(__name__)


def find_unenrolled_users(client: str, data_type: str) -> Dict[str, Any]:
    """
    Find users that exist in external data source but are not enrolled in Snowflake.

    Args:
        client: Client name (mato_grosso, parana, goias)
        data_type: Type of data (students, teachers, teachers_with_gls)

    Returns:
        Dictionary containing results and metadata
    """
    logger.info(
        f"Finding unenrolled users for client: {client}, data_type: {data_type}"
    )

    try:
        # Fetch external data
        logger.info("Fetching external data...")
        external_data = fetch_external_data(client, data_type)
        logger.info(f"External data shape: {external_data.shape}")

        # Apply EJA filtering for Goias students only
        # TODO Check below further
        # ? Should this logic be moved to fetch_external_data function or encapsulated elsewhere
        if client == "goias" and data_type == "students":
            logger.info("Applying EJA filtering for Goias students...")
            initial_count = len(external_data)

            # Check if "Composição" column exists
            if "Composição" in external_data.columns:
                # Filter out rows containing "EJA" in the "Composição" column
                external_data = external_data[
                    ~external_data["Composição"]
                    .astype(str)
                    .str.upper()
                    .str.contains("EJA", na=False)
                ]
                filtered_count = len(external_data)
                logger.info(
                    f"EJA filtering completed: {initial_count - filtered_count} rows removed "
                    f"({filtered_count} remaining)"
                )
            else:
                logger.warning(
                    "Composição column not found in external data, skipping EJA filtering"
                )

        # Fetch Snowflake enrollment data using cached data
        logger.info("Fetching Snowflake enrollment data...")
        company_name = get_snowflake_company_name(client)
        enrollment_data = get_client_enrollment_data(company_name)
        logger.info(f"Enrollment data shape: {enrollment_data.shape}")

        # Find email columns for joining
        external_email_col = find_email_column(external_data.columns.tolist())
        enrollment_email_col = find_email_column(enrollment_data.columns.tolist())

        logger.info(
            f"Using join columns - External: '{external_email_col}', Enrollment: '{enrollment_email_col}'"
        )

        # Perform anti-join to find unenrolled users
        unenrolled_df = perform_anti_join(
            external_data, enrollment_data, external_email_col, enrollment_email_col
        )

        # Convert to list of dictionaries for JSON response
        unenrolled_users = unenrolled_df.to_dict("records")

        # Prepare response
        result = {
            "status": "success",
            "total_unenrolled_users": len(unenrolled_users),
            "unenrolled_users": unenrolled_users,
            "timestamp": datetime.now().isoformat(),
            "metadata": {
                "client": client,
                "data_type": data_type,
                "external_records_total": len(external_data),
                "enrolled_records_total": len(enrollment_data),
                "join_column_external": external_email_col,
                "join_column_enrollment": enrollment_email_col,
                "snowflake_company": company_name,
            },
        }

        logger.info(f"Found {len(unenrolled_users)} unenrolled users")
        return result

    except Exception as e:
        logger.error(f"Error finding unenrolled users: {str(e)}")
        try:
            company_name_error = get_snowflake_company_name(client)
        except Exception:
            company_name_error = f"Unknown (client: {client})"

        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat(),
            "metadata": {
                "client": client,
                "data_type": data_type,
                "company": company_name_error,
            },
        }


def perform_anti_join(
    external_df: pd.DataFrame,
    enrollment_df: pd.DataFrame,
    external_col: str,
    enrollment_col: str,
) -> pd.DataFrame:
    """
    Perform anti-join operation to find records in external_df not in enrollment_df.

    Args:
        external_df: DataFrame with external data
        enrollment_df: DataFrame with enrollment data
        external_col: Column name for joining in external_df
        enrollment_col: Column name for joining in enrollment_df

    Returns:
        DataFrame with unenrolled users (records in external but not in enrollment)
    """
    logger.debug(
        f"Performing anti-join on {len(external_df)} external vs {len(enrollment_df)} enrollment records"
    )

    # Clean and normalize email columns for better matching
    external_df_clean = external_df.copy()
    enrollment_df_clean = enrollment_df.copy()

    # STEP 1: Filter out null values BEFORE converting to string
    # This prevents NaN from becoming the literal string "nan"
    external_df_clean = external_df_clean[external_df_clean[external_col].notna()]
    enrollment_df_clean = enrollment_df_clean[
        enrollment_df_clean[enrollment_col].notna()
    ]

    logger.debug(
        f"After null filtering - External: {len(external_df_clean)}, Enrollment: {len(enrollment_df_clean)}"
    )

    # STEP 2: Normalize emails (lowercase, convert to string, strip whitespace)
    external_df_clean[external_col] = (
        external_df_clean[external_col].astype(str).str.lower().str.strip()
    )
    enrollment_df_clean[enrollment_col] = (
        enrollment_df_clean[enrollment_col].astype(str).str.lower().str.strip()
    )

    # STEP 3: Remove ALL types of whitespace (including non-breaking spaces, tabs, etc.)
    external_df_clean[external_col] = external_df_clean[external_col].str.replace(
        r"\s+", "", regex=True
    )
    enrollment_df_clean[enrollment_col] = enrollment_df_clean[enrollment_col].str.replace(
        r"\s+", "", regex=True
    )

    # STEP 4: Filter out empty strings after normalization
    external_df_clean = external_df_clean[external_df_clean[external_col] != ""]
    enrollment_df_clean = enrollment_df_clean[enrollment_df_clean[enrollment_col] != ""]

    logger.debug(
        f"After empty string filtering - External: {len(external_df_clean)}, Enrollment: {len(enrollment_df_clean)}"
    )

    # STEP 5: Deduplicate emails (CRITICAL - prevents false positives)
    external_initial = len(external_df_clean)
    enrollment_initial = len(enrollment_df_clean)

    external_df_clean = external_df_clean.drop_duplicates(
        subset=[external_col], keep="first"
    )
    enrollment_df_clean = enrollment_df_clean.drop_duplicates(
        subset=[enrollment_col], keep="first"
    )

    external_dupes = external_initial - len(external_df_clean)
    enrollment_dupes = enrollment_initial - len(enrollment_df_clean)

    if external_dupes > 0:
        logger.info(f"Removed {external_dupes} duplicate emails from external data")
    if enrollment_dupes > 0:
        logger.info(f"Removed {enrollment_dupes} duplicate emails from enrollment data")

    # STEP 6: Keep only the email column from enrollment to prevent column pollution
    enrollment_df_clean = enrollment_df_clean[[enrollment_col]]

    logger.info(
        f"Ready for anti-join - External: {len(external_df_clean)}, Enrollment: {len(enrollment_df_clean)}"
    )

    # Perform left merge with indicator
    merged = external_df_clean.merge(
        enrollment_df_clean,
        left_on=external_col,
        right_on=enrollment_col,
        how="left",
        indicator=True,
        suffixes=("", "_enrolled"),
    )

    # Filter for records that exist only in external data (left_only)
    unenrolled = merged[merged["_merge"] == "left_only"].copy()

    # Remove the merge indicator and duplicate enrollment columns
    unenrolled = unenrolled.drop("_merge", axis=1)
    enrollment_cols_to_drop = [
        col for col in unenrolled.columns if col.endswith("_enrolled")
    ]
    unenrolled = unenrolled.drop(enrollment_cols_to_drop, axis=1)

    logger.debug(f"Anti-join completed: {len(unenrolled)} unenrolled users found")

    return unenrolled


def get_data_summary(
    external_df: pd.DataFrame, enrollment_df: pd.DataFrame, unenrolled_df: pd.DataFrame
) -> Dict[str, Any]:
    """
    Generate summary statistics for the data processing.

    Args:
        external_df: External data DataFrame
        enrollment_df: Enrollment data DataFrame
        unenrolled_df: Unenrolled users DataFrame

    Returns:
        Dictionary with summary statistics
    """
    total_external = len(external_df)
    total_enrolled = len(enrollment_df)
    total_unenrolled = len(unenrolled_df)

    return {
        "total_external_records": total_external,
        "total_enrolled_records": total_enrolled,
        "total_unenrolled_users": total_unenrolled,
        "enrollment_rate": (
            round((total_external - total_unenrolled) / total_external * 100, 2)
            if total_external > 0
            else 0
        ),
        "unenrollment_rate": (
            round(total_unenrolled / total_external * 100, 2)
            if total_external > 0
            else 0
        ),
    }
