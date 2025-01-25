# Data Processor Component

The **Data Processor Component** is a Spring Boot application designed to process CSV files, validate and transform their data, and store the processed data in a MySQL database. It also includes a Dead-Letter Queue (DLQ) mechanism to handle failed files and a scheduled cleanup task for temporary files.

---

## Features

1. **CSV File Processing**:
    - Validates CSV files for correct headers and row structure.
    - Transforms CSV data into a format suitable for database storage.
    - Saves transformed data to a MySQL database.

2. **Dead-Letter Queue (DLQ)**:
    - Moves failed files (e.g., invalid CSV files) to a DLQ directory for further investigation.
    - Saves error details (e.g., exception messages) alongside the failed files.

3. **Scheduled Cleanup**:
    - Automatically deletes temporary files older than 24 hours.
    - Cleans up DLQ files older than 7 days.

4. **Logging**:
    - Detailed logging for debugging and monitoring.
    - Logs are saved to `application.log`.

---

## Prerequisites

Before running the application, ensure you have the following installed:

- **Java 17** or higher.
- **MySQL** (or another compatible database).
- **Maven** (for building the project).

