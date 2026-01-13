# API Reference

Complete API documentation for pyGWRetrieval.

## Core Classes

### GroundwaterRetrieval

```python
class GroundwaterRetrieval(start_date='1900-01-01', end_date=None, data_sources='gwlevels')
```

Main class for retrieving groundwater level data from USGS NWIS.

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `start_date` | str | '1900-01-01' | Start date in 'YYYY-MM-DD' format |
| `end_date` | str | None | End date in 'YYYY-MM-DD' format. Defaults to today |
| `data_sources` | str \| List[str] | 'gwlevels' | USGS data sources to retrieve. See [Data Sources](#data-sources) |

#### Data Sources

pyGWRetrieval supports three USGS NWIS data sources:

| Source | API Function | Description |
|--------|-------------|-------------|
| `gwlevels` | `get_gwlevels()` | Field groundwater-level measurements - discrete manual readings taken during field visits. Most accurate but infrequent. |
| `dv` | `get_dv()` | Daily values - daily statistical summaries (mean, min, max) computed from continuous sensors. |
| `iv` | `get_iv()` | Instantaneous values - current/historical observations at 15-60 minute intervals from continuous sensors. |

**Usage:**
```python
# Single source (default)
gw = GroundwaterRetrieval(data_sources='gwlevels')

# Multiple sources
gw = GroundwaterRetrieval(data_sources=['gwlevels', 'dv'])

# All sources
gw = GroundwaterRetrieval(data_sources='all')
```

The output data includes a `data_source` column indicating the origin of each record.

> **Note on Data Aggregation**: All three data types are stored as-is after download without any aggregation. Daily values (`dv`) are pre-computed by USGS, and instantaneous values (`iv`) retain their original high-frequency resolution. Use `TemporalAggregator` from the `temporal` module if aggregation is needed.

#### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `wells` | gpd.GeoDataFrame | GeoDataFrame of discovered wells |
| `data` | pd.DataFrame | Retrieved groundwater level data |
| `start_date` | str | Query start date |
| `end_date` | str | Query end date |
| `data_sources` | List[str] | List of active data sources |

#### Methods

##### `get_data_by_zipcode(zipcode, buffer_miles=10.0, country='US')`

Retrieve data for wells within a buffer around a zip code.

**Parameters:**
- `zipcode` (str): US zip code
- `buffer_miles` (float): Buffer distance in miles
- `country` (str): Country code for zip lookup

**Returns:** pd.DataFrame

**Example:**
```python
gw = GroundwaterRetrieval(start_date='2020-01-01')
data = gw.get_data_by_zipcode('89701', buffer_miles=15)
```

---

##### `get_data_by_geojson(filepath, buffer_miles=None, layer=None)`

Retrieve data based on GeoJSON geometries.

**Parameters:**
- `filepath` (str | Path): Path to GeoJSON file
- `buffer_miles` (float | None): Buffer for point geometries (required for points)
- `layer` (str | None): Layer name for multi-layer files

**Returns:** pd.DataFrame

---

##### `get_data_by_shapefile(filepath, buffer_miles=None)`

Retrieve data based on shapefile geometries.

**Parameters:**
- `filepath` (str | Path): Path to shapefile
- `buffer_miles` (float | None): Buffer for point geometries

**Returns:** pd.DataFrame

---

##### `get_data_by_zipcodes_csv(filepath, zipcode_column, buffer_miles=10.0, country='US', merge_results=True, parallel=True, n_workers=None, scheduler='threads')`

Retrieve data for multiple zip codes from a CSV file with parallel processing support.

**Parameters:**
- `filepath` (str | Path): Path to CSV file containing zip codes
- `zipcode_column` (str): Name of the column containing zip codes
- `buffer_miles` (float): Buffer distance in miles (default: 10)
- `country` (str): Country code for zip lookup (default: 'US')
- `merge_results` (bool): If True, return single DataFrame; if False, return dict
- `parallel` (bool): Enable parallel processing (default: True)
- `n_workers` (int | None): Number of parallel workers (default: auto-detect)
- `scheduler` (str): Dask scheduler - 'threads', 'processes', or 'synchronous'

**Returns:** pd.DataFrame (with `source_zipcode` column) or Dict[str, pd.DataFrame]

**Example:**
```python
# CSV file with 'zip' column - parallel by default
data = gw.get_data_by_zipcodes_csv(
    'locations.csv',
    zipcode_column='zip',
    buffer_miles=15,
    parallel=True,
    n_workers=4
)
print(data['source_zipcode'].value_counts())

# Get separate DataFrames per zip code
data_dict = gw.get_data_by_zipcodes_csv(
    'locations.csv',
    zipcode_column='zip',
    merge_results=False
)
```

---

##### `get_data_by_state(state_code)`

Retrieve data for an entire state.

**Parameters:**
- `state_code` (str): Two-letter state code (e.g., 'NV', 'CA')

**Returns:** pd.DataFrame

---

##### `get_data_by_sites(site_numbers)`

Retrieve data for specific USGS sites.

**Parameters:**
- `site_numbers` (List[str]): List of USGS site numbers

**Returns:** pd.DataFrame

**Example:**
```python
sites = ['390000119000001', '390000119000002']
data = gw.get_data_by_sites(sites)
```

---

##### `to_csv(filepath, data=None)`

Save data to CSV file.

**Parameters:**
- `filepath` (str | Path): Output file path
- `data` (pd.DataFrame | None): Data to save (uses self.data if None)

---

##### `to_parquet(filepath, data=None)`

Save data to Parquet file.

**Parameters:**
- `filepath` (str | Path): Output file path
- `data` (pd.DataFrame | None): Data to save

---

##### `save_data_per_zipcode(output_dir, file_format='csv', prefix='gw_data', data=None)`

Save groundwater data to separate files for each zip code.

**Parameters:**
- `output_dir` (str | Path): Directory for output files (created if doesn't exist)
- `file_format` (str): Output format - 'csv' or 'parquet' (default: 'csv')
- `prefix` (str): Filename prefix (default: 'gw_data')
- `data` (pd.DataFrame | None): Data to save (uses self.data if None)

**Returns:** Dict[str, Path] - Mapping of zip codes to file paths

**Raises:**
- `ValueError`: If data lacks 'source_zipcode' column or invalid format specified

**Example:**
```python
gw = GroundwaterRetrieval()
data = gw.get_data_by_zipcodes_csv('locations.csv', zipcode_column='zip')

# Save each zip code to separate CSV file
saved = gw.save_data_per_zipcode('output/', file_format='csv')
# Creates: output/gw_data_89701.csv, output/gw_data_89703.csv, etc.

# Save as parquet with custom prefix
saved = gw.save_data_per_zipcode('output/', file_format='parquet', prefix='groundwater')
# Creates: output/groundwater_89701.parquet, etc.
```

---

##### `get_wells_geodataframe()`

Get discovered wells as GeoDataFrame.

**Returns:** gpd.GeoDataFrame

---

##### `save_wells_to_file(filepath, driver='GeoJSON')`

Save wells to geospatial file.

**Parameters:**
- `filepath` (str | Path): Output file path
- `driver` (str): Output format ('GeoJSON', 'ESRI Shapefile', 'GPKG')

---

##### `get_data_summary()`

Get summary statistics of retrieved data.

**Returns:** dict

---

### TemporalAggregator

```python
class TemporalAggregator(data, date_column='lev_dt', value_column='lev_va', site_column='site_no')
```

Class for temporal aggregation of groundwater data.

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `data` | pd.DataFrame | - | Input groundwater data |
| `date_column` | str | 'lev_dt' | Name of date column |
| `value_column` | str | 'lev_va' | Name of value column |
| `site_column` | str | 'site_no' | Name of site column |

#### Methods

##### `to_monthly(agg_func='mean', include_count=True)`

Aggregate to monthly resolution.

**Parameters:**
- `agg_func` (str | Callable): Aggregation function ('mean', 'median', 'min', 'max')
- `include_count` (bool): Include observation count

**Returns:** pd.DataFrame with columns: site_no, year, month, value, count, date

---

##### `to_annual(agg_func='mean', include_count=True, water_year=False, water_year_start_month=10)`

Aggregate to annual resolution.

**Parameters:**
- `agg_func` (str | Callable): Aggregation function
- `include_count` (bool): Include observation count
- `water_year` (bool): Use water year instead of calendar year
- `water_year_start_month` (int): Starting month for water year (default: October)

**Returns:** pd.DataFrame

---

##### `to_growing_season(start_month=4, end_month=9, agg_func='mean', include_count=True, region=None)`

Aggregate to growing season periods.

**Parameters:**
- `start_month` (int): Start month (1-12)
- `end_month` (int): End month (1-12)
- `agg_func` (str | Callable): Aggregation function
- `include_count` (bool): Include observation count
- `region` (str | None): Pre-defined region name

**Pre-defined Regions:**
- `'northern_hemisphere'`: April-October
- `'southern_hemisphere'`: October-April
- `'western_us'`: April-September
- `'midwest_us'`: May-September
- `'southern_us'`: March-November

**Returns:** pd.DataFrame

---

##### `to_custom_period(months, agg_func='mean', include_count=True, period_name='custom')`

Aggregate to custom month periods.

**Parameters:**
- `months` (List[int]): List of months to include (1-12)
- `agg_func` (str | Callable): Aggregation function
- `include_count` (bool): Include observation count
- `period_name` (str): Name for the custom period

**Returns:** pd.DataFrame

**Example:**
```python
# Summer months
summer = aggregator.to_custom_period([6, 7, 8], period_name='summer')

# Winter months (spans year boundary)
winter = aggregator.to_custom_period([12, 1, 2], period_name='winter')
```

---

##### `to_weekly(agg_func='mean', include_count=True)`

Aggregate to weekly resolution.

**Returns:** pd.DataFrame

---

##### `resample(freq, agg_func='mean')`

Resample using pandas resample functionality.

**Parameters:**
- `freq` (str): Pandas frequency string ('D', 'W', 'M', 'Q', 'Y')
- `agg_func` (str | Callable): Aggregation function

**Returns:** pd.DataFrame

---

##### `calculate_statistics(groupby=None)`

Calculate comprehensive statistics.

**Parameters:**
- `groupby` (List[str] | None): Columns to group by

**Returns:** pd.DataFrame with count, mean, std, min, q25, median, q75, max

---

##### `get_trends(period='annual')`

Calculate linear trends for each well.

**Parameters:**
- `period` (str): Period for analysis ('annual' or 'monthly')

**Returns:** pd.DataFrame with slope, intercept, r_squared, p_value, trend_direction

---

### GroundwaterPlotter

```python
class GroundwaterPlotter(data, date_column='lev_dt', value_column='lev_va', site_column='site_no', style='default')
```

Class for visualizing groundwater data.

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `data` | pd.DataFrame | - | Groundwater data |
| `date_column` | str | 'lev_dt' | Date column name |
| `value_column` | str | 'lev_va' | Value column name |
| `site_column` | str | 'site_no' | Site column name |
| `style` | str | 'default' | Plot style ('default', 'seaborn', 'ggplot') |

#### Methods

##### `plot_time_series(wells=None, ax=None, figsize=(12, 6), title=None, xlabel='Date', ylabel='Depth to Water (ft)', legend=True, invert_yaxis=True, **kwargs)`

Plot time series for one or more wells.

**Parameters:**
- `wells` (List[str] | None): Wells to plot (max 10 if None)
- `ax` (plt.Axes | None): Axes to plot on
- `figsize` (Tuple[int, int]): Figure size
- `title` (str | None): Plot title
- `invert_yaxis` (bool): Invert y-axis for depth

**Returns:** plt.Figure

---

##### `plot_single_well(site_no, ax=None, figsize=(12, 6), show_trend=True, show_stats=True, **kwargs)`

Detailed plot for a single well.

**Parameters:**
- `site_no` (str): USGS site number
- `show_trend` (bool): Show linear trend line
- `show_stats` (bool): Show statistics annotation

**Returns:** plt.Figure

---

##### `plot_comparison(wells, normalize=False, figsize=(12, 8), **kwargs)`

Create comparison plots for multiple wells.

**Parameters:**
- `wells` (List[str]): Wells to compare
- `normalize` (bool): Normalize data for comparison

**Returns:** plt.Figure

---

##### `plot_monthly_boxplot(wells=None, figsize=(12, 6), **kwargs)`

Box plots of water levels by month.

**Returns:** plt.Figure

---

##### `plot_annual_summary(wells=None, agg_func='mean', figsize=(12, 6), **kwargs)`

Plot annual summary with error bars.

**Returns:** plt.Figure

---

##### `plot_heatmap(well, figsize=(14, 8), cmap='RdYlBu', **kwargs)`

Year-month heatmap for a single well.

**Parameters:**
- `well` (str): Site number
- `cmap` (str): Colormap name

**Returns:** plt.Figure

---

##### `plot_spatial_distribution(wells_gdf, value_col=None, figsize=(10, 10), cmap='viridis', basemap=False, **kwargs)`

Plot spatial distribution of wells.

**Parameters:**
- `wells_gdf` (gpd.GeoDataFrame): Well locations
- `value_col` (str | None): Column for coloring
- `basemap` (bool): Add basemap (requires contextily)

**Returns:** plt.Figure

---

##### `plot_wells_map(wells_gdf=None, agg_func='mean', title=None, cmap='RdYlBu_r', marker_size=None, add_basemap=True, group_by_column=None, **kwargs)`

Create a spatial map of groundwater wells with automatic zoom level adjustment.

**Parameters:**
- `wells_gdf` (gpd.GeoDataFrame | None): Pre-existing GeoDataFrame with well locations. If None, creates one from the data
- `agg_func` (str): Aggregation function for water levels ('mean', 'median', 'min', 'max')
- `title` (str | None): Map title (auto-generated if None)
- `cmap` (str): Colormap name. Default 'RdYlBu_r' (red=deep, blue=shallow)
- `marker_size` (float | None): Marker size (auto-calculated based on extent if None)
- `add_basemap` (bool): Add contextily basemap (default True)
- `group_by_column` (str | None): Column to group wells by (e.g., 'source_zipcode')

**Auto-Zoom Configuration:**
The zoom level automatically adjusts based on spatial extent:
- **Local** (<20 miles): zoom 12, detailed view
- **Regional** (<100 miles): zoom 10
- **State** (<500 miles): zoom 8
- **Multi-state** (<1500 miles): zoom 6
- **National** (>1500 miles): zoom 4-5

**Returns:** plt.Figure

**Example:**
```python
plotter = GroundwaterPlotter(data)
fig = plotter.plot_wells_map(
    agg_func='mean',
    cmap='RdYlBu_r',
    add_basemap=True,
    group_by_column='source_zipcode'
)
plt.savefig('wells_map.png', dpi=300)
```

---

### Standalone Visualization Functions

#### `plot_wells_map(data, wells_gdf=None, agg_func='mean', title=None, cmap='RdYlBu_r', add_basemap=True, group_by_column=None, **kwargs)`

Quick function to create a spatial map of groundwater wells without instantiating a plotter.

**Parameters:**
- `data` (pd.DataFrame): Groundwater level data with columns for site_no, lat, lon, and values
- `wells_gdf` (gpd.GeoDataFrame | None): Pre-existing GeoDataFrame with well locations
- `agg_func` (str): Aggregation function ('mean', 'median', 'min', 'max')
- `title` (str | None): Map title (auto-generated if None)
- `cmap` (str): Colormap name (default 'RdYlBu_r')
- `add_basemap` (bool): Add contextily basemap (default True)
- `group_by_column` (str | None): Column to group wells by

**Returns:** plt.Figure

**Example:**
```python
from pyGWRetrieval import GroundwaterRetrieval, plot_wells_map

gw = GroundwaterRetrieval()
data = gw.get_data_by_zipcodes_csv('locations.csv', zipcode_column='zip')

# Create map with auto-zoom
fig = plot_wells_map(data, group_by_column='source_zipcode')
plt.savefig('wells_map.png')
```

---

#### `create_comparison_map(data, wells_gdf=None, figsize=(18, 12), add_basemap=True)`

Create a multi-panel comparison map showing different statistics.

Creates a 2x2 panel with:
- Mean water level
- Data availability (count of records)
- Min water level (shallowest)
- Max water level (deepest)

**Parameters:**
- `data` (pd.DataFrame): Groundwater level data
- `wells_gdf` (gpd.GeoDataFrame | None): Pre-existing GeoDataFrame with well locations
- `figsize` (Tuple[int, int]): Figure size (default (18, 12))
- `add_basemap` (bool): Add contextily basemap (default True)

**Returns:** plt.Figure

**Example:**
```python
from pyGWRetrieval import GroundwaterRetrieval, create_comparison_map

gw = GroundwaterRetrieval()
data = gw.get_data_by_zipcodes_csv('locations.csv', zipcode_column='zip')

# Create 4-panel comparison map
fig = create_comparison_map(data, figsize=(18, 12))
plt.savefig('comparison_map.png', dpi=300)
```

---

##### `save_figure(fig, filepath, dpi=300, **kwargs)`

Save figure to file.

---

## Spatial Functions

### `get_zipcode_geometry(zipcode, country='US')`

Get centroid geometry for a zip code.

**Returns:** Tuple[Point, dict]

---

### `get_geometry_from_geojson(filepath, layer=None)`

Read geometry from GeoJSON file.

**Returns:** gpd.GeoDataFrame

---

### `get_geometry_from_shapefile(filepath)`

Read geometry from shapefile.

**Returns:** gpd.GeoDataFrame

---

### `buffer_geometry(geometry, buffer_miles, cap_style='round')`

Create buffer around geometry.

**Parameters:**
- `geometry`: Input geometry (Point, Polygon, or GeoDataFrame)
- `buffer_miles` (float): Buffer distance in miles
- `cap_style` (str): Buffer cap style ('round', 'flat', 'square')

**Returns:** Buffered geometry

---

### `get_bounding_box(geometry)`

Get bounding box of geometry.

**Returns:** Tuple[float, float, float, float] (min_lon, min_lat, max_lon, max_lat)

---

## Utility Functions

### `save_to_csv(data, filepath, index=False, **kwargs)`

Save DataFrame to CSV.

---

### `save_to_parquet(data, filepath, compression='snappy', **kwargs)`

Save DataFrame to Parquet.

---

### `validate_date_range(start_date, end_date, date_format='%Y-%m-%d')`

Validate date range.

**Raises:** ValueError if invalid

---

### `setup_logging(level=logging.INFO, log_file=None, format_string=None)`

Configure package logging.

**Parameters:**
- `level` (int): Logging level
- `log_file` (str | Path | None): Path to log file
- `format_string` (str | None): Custom format string

---

### `clean_data(data, value_column='lev_va', drop_na=True, remove_negative=False, min_value=None, max_value=None)`

Clean groundwater data.

**Returns:** pd.DataFrame

---

### `get_data_coverage(data, site_column='site_no', date_column='lev_dt')`

Calculate data coverage statistics per well.

**Returns:** pd.DataFrame with first_date, last_date, n_records, coverage_pct

---

### `filter_by_data_availability(data, min_records=10, min_years=1, site_column='site_no', date_column='lev_dt')`

Filter wells by data availability.

**Returns:** pd.DataFrame

---

## Convenience Functions

### `aggregate_by_period(data, period='monthly', ...)`

Quick temporal aggregation function.

**Parameters:**
- `period` (str): 'daily', 'weekly', 'monthly', 'annual', 'water_year', 'growing_season'

**Returns:** pd.DataFrame

---

### `quick_plot(data, wells=None, ...)`

---

## Data Columns Reference

### Groundwater Level Data Columns

Data retrieved from USGS NWIS contains the following columns:

| Column | Description | Units | Source |
|--------|-------------|-------|--------|
| `site_no` | USGS site identification number | - | All |
| `datetime` | Date/time of measurement (standardized) | Datetime | All |
| `value` | Water level value (standardized) | Feet | All |
| `data_source` | Origin data source | - | All (when multi-source) |
| `lev_dt` | Date of water level measurement | Date | gwlevels |
| `lev_tm` | Time of measurement | Time | gwlevels |
| `lev_va` | Water level value | Feet below land surface | gwlevels, dv, iv |
| `lev_acy_cd` | Water level accuracy code | - | gwlevels |
| `lev_src_cd` | Source of water level data | - | gwlevels |
| `lev_meth_cd` | Method of measurement code | - | gwlevels |
| `lev_status_cd` | Status of the site at time of measurement | - | gwlevels |

### Data Source Comparison

| Feature | gwlevels | dv | iv |
|---------|----------|----|----|
| **Type** | Discrete manual | Daily summaries | High-frequency |
| **Frequency** | Sporadic (field visits) | Daily | 15-60 minutes |
| **Accuracy** | Highest | Computed | Sensor-based |
| **Time Column** | lev_dt + lev_tm | datetime | datetime |
| **Value Column** | lev_va | parameter-specific | parameter-specific |
| **Best For** | Long-term trends | Daily monitoring | Real-time analysis |

### Site Information Columns (merged)

| Column | Description | Units |
|--------|-------------|-------|
| `station_nm` | Station name | - |
| `dec_lat_va` | Decimal latitude | Degrees |
| `dec_long_va` | Decimal longitude | Degrees |

### Additional Columns (added by pyGWRetrieval)

| Column | Description | When Added |
|--------|-------------|------------|
| `source_zipcode` | Origin zip code for the data | CSV zip code queries |
| `data_source` | USGS data source (gwlevels, dv, iv) | Multi-source retrieval |
| `year` | Year extracted from date | Temporal aggregation |
| `month` | Month extracted from date | Temporal aggregation |

### USGS Parameter Codes

| Code | Description | Units |
|------|-------------|-------|
| `72019` | Depth to water level below land surface (primary) | Feet |
| `72020` | Elevation above NGVD 1929 | Feet |
| `62610` | Groundwater level above NGVD 1929 | Feet |
| `62611` | Groundwater level above NAVD 1988 | Feet |

### Understanding Water Level Values

- **`lev_va`** (primary measurement): Depth to water in **feet below land surface**
  - Lower values = shallower water table (closer to surface)
  - Higher values = deeper water table (further from surface)
  - Example: `lev_va = 15.5` means water is 15.5 feet below the ground surface

### Measurement Method Codes (`lev_meth_cd`)

| Code | Description |
|------|-------------|
| `S` | Steel tape |
| `E` | Electric tape |
| `T` | Pressure transducer |
| `V` | Calibrated airline |
| `R` | Reported (not measured) |

---

## Parallel Processing Functions

### `check_dask_available()`

Check if Dask is available for parallel processing.

**Returns:** bool

---

### `get_dask_client(n_workers=None, threads_per_worker=2, memory_limit='auto')`

Create a Dask distributed client for multi-node parallelism.

**Parameters:**
- `n_workers` (int | None): Number of worker processes
- `threads_per_worker` (int): Threads per worker (default: 2)
- `memory_limit` (str): Memory limit per worker (default: 'auto')

**Returns:** Dask Client or None

**Example:**
```python
from pyGWRetrieval import get_dask_client

client = get_dask_client(n_workers=4)
print(f"Dashboard: {client.dashboard_link}")
```

---

### `parallel_map(func, items, n_workers=None, show_progress=True, scheduler='threads')`

Apply a function to items in parallel.

**Parameters:**
- `func` (Callable): Function to apply
- `items` (List): Items to process
- `n_workers` (int | None): Number of workers
- `show_progress` (bool): Show progress bar
- `scheduler` (str): 'threads', 'processes', or 'synchronous'

**Returns:** List of results

---

### `get_parallel_config()`

Get current parallel processing configuration.

**Returns:** Dict with dask_available, scheduler, num_workers, dask_version

---

### `to_dask_dataframe(df, npartitions=None)`

Convert pandas DataFrame to Dask DataFrame for large-scale operations.

**Parameters:**
- `df` (pd.DataFrame): Input DataFrame
- `npartitions` (int | None): Number of partitions

**Returns:** dd.DataFrame

---

### `from_dask_dataframe(ddf)`

Convert Dask DataFrame back to pandas.

**Returns:** pd.DataFrame

Quick plotting function.

**Returns:** plt.Figure
