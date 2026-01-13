# Quick Start Guide

This guide will help you get started with pyGWRetrieval quickly.

## Basic Concepts

pyGWRetrieval retrieves groundwater level data from the USGS National Water Information System (NWIS). The package supports various ways to specify your area of interest:

1. **Zip Code** - Specify a US zip code with a buffer distance
2. **GeoJSON** - Use a GeoJSON file with polygons or points
3. **Shapefile** - Use a shapefile with polygons or points
4. **State Code** - Retrieve data for an entire state
5. **Site Numbers** - Query specific USGS monitoring sites

### Data Sources

The package can retrieve data from three USGS data sources:

| Source | Description | Best For |
|--------|-------------|----------|
| `gwlevels` | Field measurements (discrete manual readings) | Long-term trends, calibration |
| `dv` | Daily values (daily summaries from sensors) | Regular monitoring |
| `iv` | Instantaneous values (15-60 min sensor data) | Real-time analysis |

By default, only `gwlevels` is retrieved for backward compatibility.

## Your First Query

### Query by Zip Code

The simplest way to get started is querying by zip code:

```python
from pyGWRetrieval import GroundwaterRetrieval

# Create a retrieval instance with a date range
# Default: only field measurements (gwlevels)
gw = GroundwaterRetrieval(
    start_date='2015-01-01',
    end_date='2023-12-31'
)

# Get groundwater data within 10 miles of Carson City, NV (89701)
data = gw.get_data_by_zipcode('89701', buffer_miles=10)

# View the data
print(f"Retrieved {len(data)} records from {data['site_no'].nunique()} wells")
print(data.head())

# Save to CSV
gw.to_csv('carson_city_gw_data.csv')
```

### Query with Multiple Data Sources

To retrieve data from all USGS sources:

```python
from pyGWRetrieval import GroundwaterRetrieval

# Retrieve from ALL data sources (gwlevels, dv, iv)
gw = GroundwaterRetrieval(
    start_date='2020-01-01',
    data_sources='all'  # or ['gwlevels', 'dv', 'iv']
)

data = gw.get_data_by_zipcode('89701', buffer_miles=10)

# Data includes 'data_source' column to identify origin
print(data.groupby('data_source').size())
```

### Query by Shapefile

If you have a shapefile defining your study area:

```python
from pyGWRetrieval import GroundwaterRetrieval

gw = GroundwaterRetrieval(start_date='2020-01-01')

# For polygon shapefiles (e.g., basin boundary)
data = gw.get_data_by_shapefile('my_basin.shp')

# For point shapefiles (e.g., well locations), add a buffer
data = gw.get_data_by_shapefile('well_points.shp', buffer_miles=5)
```

### Query Multiple Zip Codes from CSV

If you have a CSV file with multiple zip codes:

```python
from pyGWRetrieval import GroundwaterRetrieval

gw = GroundwaterRetrieval(start_date='2020-01-01')

# Specify the column name containing zip codes
data = gw.get_data_by_zipcodes_csv(
    'my_locations.csv',
    zipcode_column='zip',  # Column name in your CSV
    buffer_miles=10
)

# Results include 'source_zipcode' to identify origin
print(f"Data from {data['source_zipcode'].nunique()} zip codes")
print(data.groupby('source_zipcode')['site_no'].nunique())

# Or get separate DataFrames per zip code
data_dict = gw.get_data_by_zipcodes_csv(
    'my_locations.csv',
    zipcode_column='zip',
    merge_results=False
)
for zipcode, df in data_dict.items():
    print(f"{zipcode}: {len(df)} records, {df['site_no'].nunique()} wells")

# Save data to separate files per zip code
saved_files = gw.save_data_per_zipcode(
    'output_by_zipcode/',
    file_format='csv',
    prefix='gw_data'
)
# Creates: output_by_zipcode/gw_data_89701.csv, etc.
```

### Query by GeoJSON

Using a GeoJSON file:

```python
from pyGWRetrieval import GroundwaterRetrieval

gw = GroundwaterRetrieval()

# Load from GeoJSON
data = gw.get_data_by_geojson('study_area.geojson')

# Save as Parquet (efficient format for large datasets)
gw.to_parquet('groundwater_data.parquet')
```

### Query by State

To get all groundwater data for a state (may be large!):

```python
from pyGWRetrieval import GroundwaterRetrieval

# Query for a recent period to limit data size
gw = GroundwaterRetrieval(start_date='2023-01-01')

# Get data for Nevada
data = gw.get_data_by_state('NV')
```

## Working with Retrieved Data

### View Summary Statistics

```python
# Get summary of retrieved data
summary = gw.get_data_summary()
print(summary)

# Access wells as GeoDataFrame
wells = gw.get_wells_geodataframe()
print(f"Found {len(wells)} monitoring wells")
```

### Export Data

```python
# Save data to CSV
gw.to_csv('output/groundwater_levels.csv')

# Save data to Parquet
gw.to_parquet('output/groundwater_levels.parquet')

# Save well locations to GeoJSON
gw.save_wells_to_file('output/wells.geojson')

# Save well locations to Shapefile
gw.save_wells_to_file('output/wells.shp', driver='ESRI Shapefile')
```

## Temporal Aggregation

Raw data is typically at daily resolution. Use `TemporalAggregator` to aggregate:

```python
from pyGWRetrieval import GroundwaterRetrieval, TemporalAggregator

# Get raw data
gw = GroundwaterRetrieval()
data = gw.get_data_by_zipcode('89701', buffer_miles=15)

# Create aggregator
aggregator = TemporalAggregator(data)

# Monthly mean values
monthly = aggregator.to_monthly(agg_func='mean')
print("Monthly data:")
print(monthly.head(10))

# Annual median values
annual = aggregator.to_annual(agg_func='median')
print("\nAnnual data:")
print(annual.head())

# Growing season (April-September)
growing_season = aggregator.to_growing_season(start_month=4, end_month=9)
print("\nGrowing season data:")
print(growing_season.head())

# Water year (October-September)
water_year = aggregator.to_annual(water_year=True)
print("\nWater year data:")
print(water_year.head())
```

### Custom Period Aggregation

```python
# Summer months only (June, July, August)
summer = aggregator.to_custom_period(
    months=[6, 7, 8],
    period_name='summer'
)

# Winter months (December, January, February)
winter = aggregator.to_custom_period(
    months=[12, 1, 2],
    period_name='winter'
)
```

## Basic Visualization

### Time Series Plot

```python
from pyGWRetrieval import GroundwaterRetrieval, GroundwaterPlotter
import matplotlib.pyplot as plt

# Get data
gw = GroundwaterRetrieval()
data = gw.get_data_by_zipcode('89701', buffer_miles=10)

# Create plotter
plotter = GroundwaterPlotter(data)

# Plot time series for all wells (up to 10)
fig = plotter.plot_time_series()
plt.show()

# Plot specific wells
wells_to_plot = data['site_no'].unique()[:3]  # First 3 wells
fig = plotter.plot_time_series(wells=wells_to_plot)
plt.show()
```

### Single Well Analysis

```python
# Get detailed plot for one well
site = data['site_no'].unique()[0]
fig = plotter.plot_single_well(site, show_trend=True, show_stats=True)
plt.show()
```

### Monthly Distribution

```python
# Box plot of monthly values
fig = plotter.plot_monthly_boxplot()
plt.show()
```

## Complete Example Workflow

Here's a complete workflow from data retrieval to visualization:

```python
from pyGWRetrieval import (
    GroundwaterRetrieval,
    TemporalAggregator,
    GroundwaterPlotter,
    setup_logging
)
import matplotlib.pyplot as plt
import logging

# Set up logging to see progress
setup_logging(level=logging.INFO)

# 1. Retrieve Data from ALL USGS sources
print("Step 1: Retrieving groundwater data...")
gw = GroundwaterRetrieval(
    start_date='2010-01-01',
    end_date='2023-12-31',
    data_sources='all'  # gwlevels + dv + iv
)
data = gw.get_data_by_zipcode('89701', buffer_miles=20)

# 2. View Summary
print("\nStep 2: Data Summary")
summary = gw.get_data_summary()
print(f"Total records: {summary['total_records']}")
print(f"Number of wells: {summary['num_wells']}")

# Check data by source
if 'data_source' in data.columns:
    print("\\nRecords by data source:")
    print(data.groupby('data_source').size())

# 3. Temporal Aggregation
print("\nStep 3: Temporal Aggregation")
aggregator = TemporalAggregator(data)
monthly = aggregator.to_monthly()
annual = aggregator.to_annual()

# 4. Trend Analysis
print("\nStep 4: Trend Analysis")
trends = aggregator.get_trends(period='annual')
print(trends[['site_no', 'slope', 'r_squared', 'trend_direction']].head())

# 5. Visualization
print("\nStep 5: Creating Visualizations")
plotter = GroundwaterPlotter(data)

# Time series
fig = plotter.plot_time_series()
plt.savefig('time_series.png', dpi=300)
plt.close()

# Annual summary
fig = plotter.plot_annual_summary()
plt.savefig('annual_summary.png', dpi=300)
plt.close()

# 6. Export Data
print("\nStep 6: Exporting Data")
gw.to_csv('groundwater_raw.csv')
monthly.to_csv('groundwater_monthly.csv', index=False)
annual.to_csv('groundwater_annual.csv', index=False)
gw.save_wells_to_file('wells.geojson')

print("\nComplete! Files saved.")
```

## Full Workflow: CSV Zip Codes Example

For a complete end-to-end workflow processing multiple zip codes from a CSV file, see `examples/full_workflow_csv_zipcodes.py`. This example demonstrates:

```python
from pyGWRetrieval import GroundwaterRetrieval, TemporalAggregator, GroundwaterPlotter
import matplotlib.pyplot as plt

# Configuration
START_DATE = "1970-01-01"
BUFFER_MILES = 100

# 1. Download data for all zip codes in CSV
gw = GroundwaterRetrieval(start_date=START_DATE)
data = gw.get_data_by_zipcodes_csv(
    'AirbnbMSACity_with_ZipCode.csv',
    zipcode_column='ZipCode',
    buffer_miles=BUFFER_MILES
)

# 2. Save combined and per-zipcode data
gw.to_csv('output/all_groundwater_data.csv')
saved_files = gw.save_data_per_zipcode(
    output_dir='output/data_by_zipcode/',
    file_format='csv',
    prefix='gw_data'
)

# 3. Save wells as GeoJSON
gw.save_wells_to_file('output/groundwater_wells.geojson')

# 4. Temporal aggregation
aggregator = TemporalAggregator(data)
monthly = aggregator.to_monthly(agg_func='mean')
annual = aggregator.to_annual(agg_func='mean')
stats = aggregator.calculate_statistics(groupby='site_no')

# 5. Visualization per zip code
for zipcode in data['source_zipcode'].unique()[:3]:  # Top 3
    zipcode_data = data[data['source_zipcode'] == zipcode]
    plotter = GroundwaterPlotter(zipcode_data)
    
    # Time series for best well
    top_well = zipcode_data.groupby('site_no')['lev_dt'].count().idxmax()
    fig = plotter.plot_single_well(top_well)
    fig.savefig(f'output/plots/timeseries_{zipcode}.png', dpi=300)
    plt.close(fig)

# 6. Cross-zipcode comparison
fig, ax = plt.subplots(figsize=(10, 6))
data.groupby('source_zipcode')['site_no'].nunique().plot(kind='bar', ax=ax)
ax.set_title('Wells per Zip Code')
fig.savefig('output/plots/wells_per_zipcode.png', dpi=300)
```

The full example script includes:
- Automatic output directory creation
- Detailed progress logging
- Error handling for each zip code
- Multiple visualization types (time series, boxplots, comparisons)
- Summary report with statistics

## Next Steps

- Read the [API Reference](api_reference.md) for detailed documentation
- Check out the [Case Study](case_study.md) for a comprehensive regional analysis example
- Learn about [Advanced Usage](advanced_usage.md) for complex workflows

## Citation

If you use this package in your research, please cite:

```bibtex
@software{pyGWRetrieval,
  author = {Sayantan Majumdar},
  title = {pyGWRetrieval: Scalable Retrieval and Analysis of USGS Groundwater Level Data},
  year = {2026},
  url = {https://github.com/montimaj/pyGWRetrieval}
}
```
