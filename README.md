# Weather Source Data Pipeline

A modular data pipeline for fetching and processing weather data from Weather Source APIs. The pipeline supports both historical weather data and weather forecasts, with options to store data locally or in AWS S3.

## Features

- Fetch historical weather data and forecasts from Weather Source APIs
- Modular architecture with clear separation of concerns
- Support for both local and S3 storage
- Configurable data formats (Parquet and CSV)
- Comprehensive error handling and logging
- Field group support for optimized data retrieval
- Docker support for containerization
- CI/CD with GitHub Actions
- Automated testing with pytest

## Project Structure

```
.
├── src/
│   ├── handler/           # Pipeline orchestration
│   │   └── weather_handler.py
│   ├── service/           # API integration
│   │   └── weather_api.py
│   ├── parser/            # Data transformation
│   │   └── weather_parser.py
│   ├── loader/            # Data storage
│   │   ├── local.py      # Local file system storage
│   │   └── s3.py         # AWS S3 storage
│   ├── helper/           # Utility functions
│   │   └── utils.py
│   ├── main.py          # CLI entry point
│   └── demo.py          # Demo script
├── tests/
│   ├── unit/           # Unit tests
│   │   ├── test_service.py
│   │   ├── test_parser.py
│   │   └── test_loader.py
│   ├── integration/    # Integration tests
│   │   └── test_pipeline.py
│   └── conftest.py    # Test configuration
├── deploy/
│   └── docker/        # Docker configuration
├── .github/
│   └── workflows/     # GitHub Actions workflows
└── README.md
```

## Prerequisites

- Python 3.13+
- Weather Source API key
- AWS credentials (if using S3 storage)
- Docker (for containerization)

## Installation

### Local Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd weather-source-etl
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

3. Install dependencies:
```bash
pip install -e .
```

4. Copy the environment template and fill in your credentials:
```bash
cp src/.env.example src/.env
```

### Docker Installation

1. Build the Docker image:
```bash
docker build -f deploy/docker/Dockerfile -t weather-pipeline .
```

2. Set up environment variables in a .env file:
```bash
cp src/.env.example src/.env
```

Required environment variables:
- `WEATHER_SOURCE_API_KEY`: Your Weather Source API key
- `AWS_ACCESS_KEY_ID`: AWS access key (for S3 storage)
- `AWS_SECRET_ACCESS_KEY`: AWS secret key (for S3 storage)
- `AWS_REGION`: AWS region (default: us-east-1)
- `S3_BUCKET_NAME`: S3 bucket name (for S3 storage)
- `DATA_OUTPUT_PATH`: Local storage path (default: data)
- `LOG_LEVEL`: Logging level (default: INFO)

## Usage

### Command Line Interface

#### Local Usage

The pipeline can be run from the command line with various options:

```bash
# Process historical data
python src/main.py --latitude 40.7128 --longitude -74.0060 --data-type historical --start-date 2023-12-01 --end-date 2023-12-07

# Process forecast data
python src/main.py --latitude 40.7128 --longitude -74.0060 --data-type forecast --start-date 2023-12-15 --end-date 2023-12-18
```

#### Docker Usage

Run the pipeline using Docker:

```bash
# Process historical data
docker run --rm --env-file src/.env weather-pipeline "python src/main.py --latitude 40.7128 --longitude -74.0060 --data-type historical --start-date 2023-12-01 --end-date 2023-12-07"

# Process forecast data
docker run --rm --env-file src/.env weather-pipeline "python src/main.py --latitude 40.7128 --longitude -74.0060 --data-type forecast --start-date 2023-12-15 --end-date 2023-12-18"
```

### Command Line Arguments

- `--latitude`: Location latitude (required)
- `--longitude`: Location longitude (required)
- `--data-type`: Type of weather data to process (historical/forecast, required)
- `--start-date`: Start date for data collection (YYYY-MM-DD)
- `--end-date`: End date for data collection (YYYY-MM-DD)
- `--fields`: Comma-separated list of fields to retrieve
- `--file-format`: Output file format (parquet/csv, default: parquet)
- `--use-s3`: Use S3 for data storage instead of local storage

### Available Field Groups

Historical Data Fields:
- `popular`: Most commonly used fields
- `all`: All available fields
- `allTemp`: All temperature-related fields
- `allPrecip`: All precipitation-related fields
- `allWind`: All wind-related fields
- Custom fields: Specify individual fields (e.g., "temp,precip,relHum")

Forecast Data Fields:
- Similar field groups as historical data
- Additional forecast-specific fields (e.g., precipProb, snowfallProb)

### Python API

You can also use the pipeline programmatically:

```python
from src.handler.weather_handler import WeatherDataHandler

# Initialize handler
handler = WeatherDataHandler(use_s3=False)  # Use local storage

# Process historical data
historical_path = handler.process_historical_data(
    latitude=40.7128,
    longitude=-74.0060,
    start_date="2023-12-01",
    end_date="2023-12-07",
    fields="temp,precip,relHum",
    file_format="parquet"
)

# Process forecast data
forecast_path = handler.process_forecast_data(
    latitude=40.7128,
    longitude=-74.0060,
    start_date="2023-12-15",
    end_date="2023-12-18",
    fields="temp,precip,precipProb",
    file_format="parquet"
)
```

## Testing

### Local Testing

Run the test suite locally:

```bash
# Run all tests
pytest tests/

# Run specific test files
pytest tests/unit/test_service.py
pytest tests/integration/test_pipeline.py

# Run with coverage report
pytest --cov=src tests/
```

### Docker Testing

Run tests using Docker:

```bash
# Run all tests
docker run --rm weather-pipeline "pytest tests/"

# Run specific test files
docker run --rm weather-pipeline "pytest tests/unit/test_service.py"
docker run --rm weather-pipeline "pytest tests/integration/test_pipeline.py"

# Run with coverage report
docker run --rm weather-pipeline "pytest --cov=src tests/"
```

## Development

1. Install development dependencies:
```bash
pip install -e .[dev]
```

2. Install pre-commit hooks:
```bash
pre-commit install
```

3. Run code quality checks:
```bash
# Format code
black src/ tests/
isort src/ tests/

# Run linters
flake8 src/ tests/
mypy src/ tests/
```

## CI/CD Pipeline

The project uses GitHub Actions for continuous integration and deployment:

1. On pull requests to `dev`:
   - Runs all tests
   - Generates coverage report

2. On push to `master`:
   - Runs all tests
   - Builds Docker image
   - Pushes image to Amazon ECR

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'feat: add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
