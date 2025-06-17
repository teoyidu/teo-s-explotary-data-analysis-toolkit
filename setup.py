"""
Setup script for the Data Quality Framework package
"""

from setuptools import setup, find_packages

setup(
    name="data-quality-framework",
    version="1.0.0",
    description="A comprehensive data quality framework for PySpark",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyspark>=3.4.0",
        "pandas>=1.3.0",
        "pyarrow>=7.0.0",
        "jsonschema>=4.0.0",
        "psutil>=5.9.0"
    ],
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    entry_points={
        "console_scripts": [
            "data-quality=data_quality.__main__:main",
        ],
    },
) 