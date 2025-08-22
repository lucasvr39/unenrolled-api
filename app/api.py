"""
FastAPI application for the Unenrolled Users API.
Provides endpoints for finding unenrolled users and listing supported clients.
"""

from datetime import datetime
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Query

from utils.client_config import get_supported_clients, validate_client_data_type
from utils.config import validate_config
from utils.logging_config import get_logger
from utils.unenrolled_users import find_unenrolled_users

logger = get_logger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Unenrolled Users API",
    description="API for finding users in external data sources who are not enrolled in Snowflake",
    version="1.0.0",
)


@app.on_event("startup")
async def startup_event():
    """Validate configuration on startup."""
    try:
        validate_config()
        logger.info("Configuration validation successful")
    except Exception as e:
        logger.error(f"Configuration validation failed: {str(e)}")
        raise


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Unenrolled Users API",
        "version": "1.0.0",
        "status": "active",
        "timestamp": datetime.now().isoformat(),
        "endpoints": {
            "unenrolled": "/unenrolled?account=<account>&client=<client>&data_type=<data_type>",
            "clients": "/clients",
        },
    }


@app.get("/unenrolled")
async def get_unenrolled_users(
    client: str = Query(..., description="Client name (mato_grosso, parana, goias)"),
    data_type: str = Query(
        ..., description="Data type (students, teachers, teachers_with_gls)"
    ),
) -> Dict[str, Any]:
    """
    Find unenrolled users for a specific client and data type.

    Args:
        client: Client name (mato_grosso, parana, goias)
        data_type: Type of data (students, teachers, teachers_with_gls)

    Returns:
        Dictionary with unenrolled users and metadata

    Raises:
        HTTPException: If client/data_type combination is invalid or processing fails
    """
    logger.info(
        f"Processing unenrolled users request: client={client}, data_type={data_type}"
    )

    try:
        # Validate client and data_type combination
        validate_client_data_type(client, data_type)

        # Process the request
        result = find_unenrolled_users(client, data_type)

        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result["message"])

        return result

    except ValueError as e:
        logger.warning(f"Validation error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error processing unenrolled users request: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/clients")
async def get_clients() -> Dict[str, Any]:
    """
    Get information about supported clients and their data types.

    Returns:
        Dictionary with client information
    """
    logger.info("Processing clients request")

    try:
        clients_info = get_supported_clients()

        return {
            "status": "success",
            "clients": clients_info,
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error(f"Error retrieving clients information: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "unenrolled-users-api",
    }


# Error handlers
@app.exception_handler(404)
async def not_found_handler(request, exc):
    """Custom 404 handler."""
    return {
        "status": "error",
        "message": "Endpoint not found",
        "timestamp": datetime.now().isoformat(),
        "available_endpoints": ["/", "/unenrolled", "/clients", "/health"],
    }


@app.exception_handler(500)
async def internal_error_handler(request, exc):
    """Custom 500 handler."""
    logger.error(f"Internal server error: {str(exc)}")
    return {
        "status": "error",
        "message": "Internal server error",
        "timestamp": datetime.now().isoformat(),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.api:app", host="0.0.0.0", port=8000, reload=True)
    uvicorn.run("app.api:app", host="0.0.0.0", port=8000, reload=True)
    uvicorn.run("app.api:app", host="0.0.0.0", port=8000, reload=True)
    uvicorn.run("app.api:app", host="0.0.0.0", port=8000, reload=True)
    uvicorn.run("app.api:app", host="0.0.0.0", port=8000, reload=True)
    uvicorn.run("app.api:app", host="0.0.0.0", port=8000, reload=True)
