.. pyGWRetrieval documentation master file

Welcome to pyGWRetrieval's documentation!
=========================================

**pyGWRetrieval** is a Python package for retrieving and analyzing groundwater level 
data from the USGS National Water Information System (NWIS).

.. image:: https://img.shields.io/pypi/v/pyGWRetrieval.svg
   :target: https://pypi.org/project/pyGWRetrieval/
   :alt: PyPI Version

.. image:: https://img.shields.io/pypi/pyversions/pyGWRetrieval.svg
   :target: https://pypi.org/project/pyGWRetrieval/
   :alt: Python Versions

.. image:: https://github.com/example/pyGWRetrieval/workflows/CI/badge.svg
   :target: https://github.com/example/pyGWRetrieval/actions
   :alt: CI Status

Features
--------

* 🌍 **Flexible Spatial Inputs**: Query by zip code, GeoJSON, shapefile, or specific site numbers
* 📊 **Temporal Aggregation**: Aggregate data to monthly, annual, growing season, or custom periods
* 📈 **Visualization**: Built-in plotting for time series analysis
* 💾 **Multiple Export Formats**: Save data as CSV or Parquet files
* 🔧 **Trend Analysis**: Calculate linear trends for water level changes

Quick Start
-----------

.. code-block:: python

   from pyGWRetrieval import GroundwaterRetrieval

   # Initialize with date range
   gw = GroundwaterRetrieval(
       start_date='2020-01-01',
       end_date='2023-12-31'
   )

   # Get data by zip code with buffer
   data = gw.get_data_by_zipcode('89701', buffer_miles=10)

   # Save to CSV
   gw.to_csv('groundwater_data.csv')

Contents
--------

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   installation
   quickstart

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api_reference

.. toctree::
   :maxdepth: 1
   :caption: Development

   changelog


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
