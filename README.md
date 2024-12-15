# Weather Source Data Pipeline

A modular data pipeline for fetching and processing weather data from Weather Source APIs. The pipeline supports both historical weather data and weather forecasts, with options to store data locally or in AWS S3.

## Features

- Fetch historical weather data and forecasts from Weather Source APIs
- Modular architecture with clear separation of concerns
- Support for both local and S3 storage
- Configurable data formats (Parquet and CSV)
- Comprehensive error handling and logging
- Docker support for containerization
- CI/CD with GitHub Actions

## Project Structure

```
.
├── src/
│   ├── handler/           # Pipeline orchestration
│   ├── service/           # API integration
│   ├── parser/            # Data transformation
│   ├── loader/            # Data storage (S3 and local)
│   ├── helper/            # Utility functions
│   ├── main.py           # CLI entry point
│   ├── demo.py           # Demo script
│   ├── requirements.txt   # Python dependencies
│   └── .env              # Environment variables
├── deploy/
│   └── docker/           # Docker configuration
├── .github/
│   └── workflows/        # GitHub Actions workflows
└── README.md
```

## Prerequisites

- Python 3.8+
- Weather Source API key
- AWS credentials (if using S3 storage)
- Docker (for containerization)