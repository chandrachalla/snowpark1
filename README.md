# My Project

This project fetches a CSV file from an S3 bucket and processes it using PySpark.

## Structure

- `fetch_csv.py`: Fetches CSV from S3.
- `spark_processing.py`: Processes the CSV using PySpark.
- `.github/workflows/main.yml`: GitHub Actions workflow for CI/CD.

## Requirements

- Python 3.9+
- Java 11+
- AWS credentials configured for Boto3
- PySpark installed

## Usage

1. Set up your AWS credentials.
2. Push your changes to the `main` branch to trigger the workflow.
