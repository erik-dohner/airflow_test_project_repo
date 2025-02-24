from setuptools import setup, find_packages

setup(
    name="api_library", 
    version="0.1.0",
    packages=find_packages(where="."), 
    install_requires=[
        "google-cloud-bigquery>=3.19.0",
        "pandas>=1.5.0",
        "dbt-core", 
        "dbt-bigquery", 
        "requests"
    ],
    python_requires=">=3.8",
)
