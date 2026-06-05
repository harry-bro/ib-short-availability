"""
ETL pipeline for IB securities lending data.

Modules:
- extract: Monitor FTP, parse events, and download raw data
- transform: Normalize and enrich data with market information  
- load: Build final analysis datasets
"""

__version__ = "1.0.0"