# Unenrolled Users API

This project provides a FastAPI application to find users from external data sources who are not enrolled in Snowflake.

## Setup and Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd unenrolled-api
    ```

2.  **Create a virtual environment:**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Environment Variables:**
    Create a `.env` file in the root directory and add the necessary environment variables. You can use the `docker-compose.yml` file as a reference for the required variables.

## Development Setup

To run the application in development mode with auto-reload, use the following command:

```bash
python dev.py
```

The application will be available at `http://127.0.0.1:8000`.

## Running with Docker

1.  **Build and run the container:**
    ```bash
    docker-compose up --build
    ```

The API will be accessible at `http://localhost:8000`.

## API Endpoints and Usage

### Get Unenrolled Users

-   **Endpoint:** `/unenrolled`
-   **Method:** `GET`
-   **Query Parameters:**
    -   `account`: Snowflake account filter (string, required)
    -   `client`: Client name (e.g., `mato_grosso`, `parana`, `goias`) (string, required)
    -   `data_type`: Data type (e.g., `students`, `teachers`) (string, required)

**Example Request using an API Platform (e.g., Postman):**

-   **URL:** `http://localhost:8000/unenrolled`
-   **Method:** `GET`
-   **Params:**
    -   `company`: `snowflake_company_account`
    -   `client`: `parana`
    -   `data_type`: `students`

### Get Supported Clients

-   **Endpoint:** `/clients`
-   **Method:** `GET`

**Example Request using an API Platform (e.g., Postman):**

-   **URL:** `http://localhost:8000/clients`
-   **Method:** `GET`

## cURL Examples

### Get Unenrolled Users

```bash
curl -X GET "http://localhost:8000/unenrolled?account=your_snowflake_account&client=parana&data_type=students" -H "accept: application/json"
```

### Get Supported Clients

```bash
curl -X GET "http://localhost:8000/clients" -H "accept: application/json"
```
