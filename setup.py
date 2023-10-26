from setuptools import find_packages, setup

setup(
    name="caleg_sementara",
    packages=find_packages(exclude=["caleg_sementara_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": [
        "dagster-webserver", 
        "pytest", 
        "dagster-duckdb",
        "dagster-duckdb-pandas",
        "aio_http", 
        "nest_asyncio", 
        "beautifulsoup4",
        "html5lib",
        "lxml",
        "xlrd",
        "openpyxl"
        ]},
)
