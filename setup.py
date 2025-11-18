"""
Setup configuration for RocketQuant-AI.

This setup.py provides backward compatibility with older pip versions
that don't support editable installs with only pyproject.toml.
"""
from setuptools import setup, find_packages

setup(
    name="rocketquant-ai",
    version="0.1.0",
    description="RocketQuant AI - Financial Data Platform for Quantitative Investing",
    python_requires=">=3.10",
    packages=find_packages(include=["utils*", "price*", "earnings*"]),
    install_requires=[
        "aiohttp>=3.8.0",
        "duckdb>=0.8.0",
        "numpy>=1.21.0",
        "pandas>=1.3.0",
        "pyarrow>=10.0.0",
        "sec-edgar-downloader>=5.0.0",
        "secedgar>=0.6.0",
        "xlsxwriter>=3.0.0",
        "tqdm>=4.60.0",
        "pytest>=6.2.4",
    ],
    extras_require={
        "dev": [
            "ipykernel>=7.1.0",
            "pytest>=7.0.0",
        ],
    },
)

