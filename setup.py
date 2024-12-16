from setuptools import setup, find_packages

setup(
    name="weather-source-etl",
    version="1.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "numpy>=1.26.4",
        "pandas>=2.2.0",
        "pytz>=2024.1",
        "python-dateutil>=2.8.2",
        "typing-extensions>=4.9.0",
        "pyarrow>=14.0.1",
        "fastparquet>=2024.2.0",
        "requests>=2.31.0",
        "urllib3>=2.0.0",
        "python-dotenv>=1.0.0",
        "click>=8.1.7",
        "boto3>=1.34.0",
        "botocore>=1.34.0",
    ],
    python_requires=">=3.13",
) 