# Command Line Interface

pyGWRetrieval provides a comprehensive command-line interface for retrieving, aggregating, analyzing, and visualizing USGS groundwater data.

## Installation

After installing pyGWRetrieval, the `pygwretrieval` command is automatically available:

```bash
pip install pyGWRetrieval
pygwretrieval --help
```

## Commands Overview

| Command | Description |
|---------|-------------|
| `retrieve` | Download groundwater data from USGS NWIS |
| `aggregate` | Temporally aggregate data (monthly, annual, etc.) |
| `stats` | Calculate statistics and trend analysis |
| `plot` | Create time series and statistical plots |
| `map` | Create spatial maps with basemaps |
| `info` | Display data file information |

## Global Options

```bash
pygwretrieval [-h] [-V] [-v] [-q] COMMAND ...
```

| Option | Description |
|--------|-------------|
| `-h, --help` | Show help message |
| `-V, --version` | Show version number |
| `-v, --verbose` | Enable verbose (debug) output |
| `-q, --quiet` | Suppress all output except errors |

---

## retrieve - Data Retrieval

Download groundwater level data from USGS NWIS based on various spatial inputs.

### Synopsis

```bash
pygwretrieval retrieve [options]
```

### Input Sources (choose one)

| Option | Description |
|--------|-------------|
| `--zipcode ZIP, -z ZIP` | Single zip code to query |
| `--csv FILE, -c FILE` | CSV file containing zip codes |
| `--shapefile FILE, -shp FILE` | Shapefile path (.shp) |
| `--geojson FILE, -gj FILE` | GeoJSON file path |
| `--state CODE, -st CODE` | Two-letter state code (e.g., NV, CA) |
| `--sites SITE [SITE ...], -s` | USGS site numbers |

### Spatial Options

| Option | Description |
|--------|-------------|
| `--buffer MILES, -b MILES` | Buffer distance in miles (default: 10) |

### Temporal Options

| Option | Description |
|--------|-------------|
| `--start-date DATE` | Start date YYYY-MM-DD (default: 1970-01-01) |
| `--end-date DATE` | End date YYYY-MM-DD (default: today) |

### Data Source Options

| Option | Description |
|--------|-------------|
| `--data-sources SOURCE [SOURCE ...]` | USGS data sources to retrieve (default: gwlevels) |

**Available Sources:**
- `gwlevels` - Field groundwater-level measurements (discrete, most accurate)
- `dv` - Daily values (daily summaries from continuous sensors)
- `iv` - Instantaneous values (15-60 minute intervals from sensors)
- `all` - All available sources (gwlevels + dv + iv)

### CSV Options

| Option | Description |
|--------|-------------|
| `--zipcode-column COL` | Column name with zip codes (default: zipcode) |
| `--save-per-zipcode` | Save separate files for each zip code |
| `--per-zipcode-dir DIR` | Directory for per-zipcode files |

### Processing Options

| Option | Description |
|--------|-------------|
| `--parallel, -p` | Enable parallel processing |
| `--workers N, -w N` | Number of parallel workers (default: auto) |
| `--scheduler TYPE` | Dask scheduler: threads, processes, synchronous |

### Output Options

| Option | Description |
|--------|-------------|
| `--output FILE, -o FILE` | Output file path |
| `--format FORMAT, -f FORMAT` | Output format: csv or parquet |
| `--wells-output FILE` | Output file for well locations |

### Examples

```bash
# Single zip code with 10-mile buffer (default: gwlevels only)
pygwretrieval retrieve --zipcode 89701 --buffer 10 --output data.csv

# Retrieve from ALL data sources
pygwretrieval retrieve --zipcode 89701 --buffer 10 --data-sources all --output data.csv

# Retrieve from specific sources (field measurements + daily values)
pygwretrieval retrieve --zipcode 89701 --data-sources gwlevels dv --output data.csv

# Multiple zip codes from CSV with parallel processing and all sources
pygwretrieval retrieve --csv locations.csv --zipcode-column zip \
    --data-sources all --parallel --output data.csv

# Save per-zipcode files
pygwretrieval retrieve --csv locations.csv --zipcode-column zip \
    --save-per-zipcode --per-zipcode-dir output_by_zipcode/ --output all_data.csv

# From shapefile with date range
pygwretrieval retrieve --shapefile basin.shp --buffer 5 \
    --start-date 2010-01-01 --end-date 2023-12-31 --output basin_data.csv

# State-wide query
pygwretrieval retrieve --state NV --output nevada_data.parquet --format parquet

# Specific USGS sites
pygwretrieval retrieve --sites 390000119000001 390000119000002 --output sites.csv

# Save wells as GeoJSON
pygwretrieval retrieve --zipcode 89701 --buffer 15 \
    --output data.csv --wells-output wells.geojson
```

---

## aggregate - Temporal Aggregation

Aggregate groundwater level data to different temporal periods.

### Synopsis

```bash
pygwretrieval aggregate --input FILE --output FILE [options]
```

### Required Arguments

| Option | Description |
|--------|-------------|
| `--input FILE, -i FILE` | Input CSV or Parquet file |
| `--output FILE, -o FILE` | Output file path |

### Options

| Option | Description |
|--------|-------------|
| `--period PERIOD, -p` | Aggregation period (see below) |
| `--agg-func FUNC` | Aggregation function: mean, median, min, max, std, count |
| `--start-month M` | Start month for custom/growing-season (1-12) |
| `--end-month M` | End month for custom/growing-season (1-12) |

### Aggregation Periods

| Period | Description |
|--------|-------------|
| `monthly` | Calendar month aggregation |
| `annual` | Calendar year aggregation |
| `water-year` | Water year (October-September) aggregation |
| `growing-season` | Growing season (requires start/end months) |
| `custom` | Custom month range (requires start/end months) |

### Column Options

| Option | Description |
|--------|-------------|
| `--date-column COL` | Date column name (default: lev_dt) |
| `--value-column COL` | Value column name (default: lev_va) |
| `--site-column COL` | Site column name (default: site_no) |

### Examples

```bash
# Monthly mean aggregation
pygwretrieval aggregate --input data.csv --period monthly --output monthly.csv

# Annual median
pygwretrieval aggregate --input data.csv --period annual --agg-func median --output annual.csv

# Water year aggregation
pygwretrieval aggregate --input data.csv --period water-year --output water_year.csv

# Growing season (April-September)
pygwretrieval aggregate --input data.csv --period growing-season \
    --start-month 4 --end-month 9 --output growing.csv

# Summer months (June-August) with max values
pygwretrieval aggregate --input data.csv --period custom \
    --start-month 6 --end-month 8 --agg-func max --output summer_max.csv
```

---

## stats - Statistics and Trends

Calculate summary statistics and perform trend analysis.

### Synopsis

```bash
pygwretrieval stats --input FILE --output FILE [options]
```

### Required Arguments

| Option | Description |
|--------|-------------|
| `--input FILE, -i FILE` | Input CSV or Parquet file |
| `--output FILE, -o FILE` | Output file path (creates _statistics.csv and _trends.csv) |

### Options

| Option | Description |
|--------|-------------|
| `--type TYPE, -t TYPE` | Analysis type: statistics, trends, or both (default: both) |
| `--trend-period PERIOD` | Period for trend analysis: monthly or annual |
| `--parallel, -p` | Enable parallel processing for trend analysis |
| `--groupby COL` | Column to group by (default: site_no) |

### Examples

```bash
# Calculate both statistics and trends
pygwretrieval stats --input data.csv --output analysis

# Statistics only
pygwretrieval stats --input data.csv --output well_stats --type statistics

# Trends with parallel processing
pygwretrieval stats --input data.csv --output trends --type trends --parallel

# Annual trend analysis
pygwretrieval stats --input data.csv --output annual_trends --trend-period annual
```

---

## plot - Visualization

Create various plots from groundwater data.

### Synopsis

```bash
pygwretrieval plot --input FILE --output FILE [options]
```

### Required Arguments

| Option | Description |
|--------|-------------|
| `--input FILE, -i FILE` | Input CSV or Parquet file |
| `--output FILE, -o FILE` | Output image file (PNG, PDF, SVG) |

### Plot Types

| Type | Description |
|------|-------------|
| `timeseries` | Time series plot for multiple wells |
| `single-well` | Detailed plot for a single well |
| `boxplot` | Monthly boxplot distribution |
| `annual` | Annual summary plot |
| `heatmap` | Data availability heatmap |

### Options

| Option | Description |
|--------|-------------|
| `--type TYPE, -t TYPE` | Plot type (default: timeseries) |
| `--wells SITE [SITE ...]` | Well site numbers to plot |
| `--title TEXT` | Custom plot title |
| `--figsize W H` | Figure size in inches (default: 12 8) |
| `--dpi DPI` | Output resolution (default: 300) |
| `--show-trend` | Show trend line (single-well plot) |
| `--show-stats` | Show statistics box (single-well plot) |

### Examples

```bash
# Time series for all wells (top 5 by record count)
pygwretrieval plot --input data.csv --type timeseries --output timeseries.png

# Single well with trend and statistics
pygwretrieval plot --input data.csv --type single-well \
    --wells 390000119000001 --show-trend --show-stats --output well.png

# Monthly distribution boxplot
pygwretrieval plot --input data.csv --type boxplot --output boxplot.png

# Annual summary
pygwretrieval plot --input data.csv --type annual --output annual.png

# Data availability heatmap
pygwretrieval plot --input data.csv --type heatmap --output heatmap.png

# High resolution output
pygwretrieval plot --input data.csv --type timeseries \
    --figsize 16 10 --dpi 300 --output timeseries_hires.png
```

---

## map - Spatial Maps

Create spatial maps of groundwater wells with automatic zoom adjustment.

### Synopsis

```bash
pygwretrieval map --input FILE --output FILE [options]
```

### Required Arguments

| Option | Description |
|--------|-------------|
| `--input FILE, -i FILE` | Input CSV or Parquet file |
| `--output FILE, -o FILE` | Output image file (PNG, PDF, SVG) |

### Options

| Option | Description |
|--------|-------------|
| `--agg-func FUNC` | Aggregation function: mean, median, min, max |
| `--cmap NAME` | Colormap name (default: RdYlBu_r) |
| `--basemap, -m` | Add basemap |
| `--basemap-source SOURCE` | Basemap provider (see below) |
| `--group-by COL` | Column to group wells by |
| `--marker-size SIZE` | Marker size (default: auto-scaled) |
| `--title TEXT` | Custom map title |
| `--figsize W H` | Figure size in inches |
| `--dpi DPI` | Output resolution (default: 300) |
| `--comparison` | Create 4-panel comparison map |

### Basemap Sources

| Source | Description |
|--------|-------------|
| `CartoDB.Positron` | Light gray basemap (default) |
| `CartoDB.DarkMatter` | Dark basemap |
| `OpenStreetMap.Mapnik` | OpenStreetMap standard |
| `Esri.WorldImagery` | Satellite imagery |
| `Esri.WorldStreetMap` | Esri street map |
| `Esri.WorldTopoMap` | Esri topographic map |

### Auto-Zoom

The map automatically adjusts zoom level based on data extent:

| Extent | Scale | Zoom Level |
|--------|-------|------------|
| < 20 miles | Local | 12 |
| < 100 miles | Regional | 10 |
| < 500 miles | State | 7-8 |
| < 1500 miles | Multi-state | 5-6 |
| > 1500 miles | National | 4-5 |

### Examples

```bash
# Basic map with basemap
pygwretrieval map --input data.csv --output wells_map.png --basemap

# Map grouped by zip code
pygwretrieval map --input data.csv --output map.png --basemap --group-by source_zipcode

# Satellite imagery basemap
pygwretrieval map --input data.csv --output map.png --basemap \
    --basemap-source Esri.WorldImagery

# Custom colormap and title
pygwretrieval map --input data.csv --output map.png --basemap \
    --cmap viridis --title "Study Area Wells"

# 4-panel comparison map
pygwretrieval map --input data.csv --output comparison.png --comparison --basemap

# High resolution output
pygwretrieval map --input data.csv --output map.png --basemap \
    --figsize 16 12 --dpi 300
```

---

## info - Data Information

Display summary information about a groundwater data file.

### Synopsis

```bash
pygwretrieval info --input FILE [options]
```

### Required Arguments

| Option | Description |
|--------|-------------|
| `--input FILE, -i FILE` | Input CSV or Parquet file |

### Options

| Option | Description |
|--------|-------------|
| `--detailed` | Show detailed statistics |

### Examples

```bash
# Basic information
pygwretrieval info --input data.csv

# Detailed statistics
pygwretrieval info --input data.csv --detailed
```

### Output Example

```
============================================================
GROUNDWATER DATA SUMMARY
============================================================

File: data.csv
Records: 125,432
Columns: 12
Unique Wells: 156
Date Range: 1970-01-15 to 2023-12-28
Zip Codes: 5

Columns: ['site_no', 'lev_dt', 'lev_va', 'lev_status_cd', ...]

------------------------------------------------------------
DETAILED STATISTICS (with --detailed)
------------------------------------------------------------

Water Level (lev_va) - feet below surface:
  Count:  124,891
  Mean:   45.23
  Std:    28.15
  Min:    2.10
  25%:    22.45
  50%:    38.90
  75%:    62.30
  Max:    198.50

Records per well:
  Min:    5
  Mean:   803.5
  Max:    4,521

Records per zip code:
  89701: 45,230 records, 52 wells
  89703: 32,100 records, 41 wells
  ...

============================================================
```

---

## Workflow Examples

### Complete Analysis Pipeline

```bash
#!/bin/bash
# Complete groundwater analysis workflow

# 1. Retrieve data from multiple zip codes
pygwretrieval retrieve --csv locations.csv --zipcode-column zip \
    --buffer 15 --parallel \
    --start-date 1970-01-01 \
    --output data/raw_data.csv \
    --wells-output data/wells.geojson \
    --save-per-zipcode --per-zipcode-dir data/by_zipcode/

# 2. View data summary
pygwretrieval info --input data/raw_data.csv --detailed

# 3. Aggregate to different periods
pygwretrieval aggregate --input data/raw_data.csv --period monthly --output data/monthly.csv
pygwretrieval aggregate --input data/raw_data.csv --period annual --output data/annual.csv
pygwretrieval aggregate --input data/raw_data.csv --period water-year --output data/water_year.csv

# 4. Calculate statistics and trends
pygwretrieval stats --input data/raw_data.csv --output data/analysis --parallel

# 5. Create visualizations
pygwretrieval plot --input data/raw_data.csv --type timeseries --output plots/timeseries.png
pygwretrieval plot --input data/raw_data.csv --type annual --output plots/annual.png
pygwretrieval plot --input data/raw_data.csv --type heatmap --output plots/heatmap.png

# 6. Create spatial maps
pygwretrieval map --input data/raw_data.csv --output plots/wells_map.png --basemap --group-by source_zipcode
pygwretrieval map --input data/raw_data.csv --output plots/comparison.png --comparison --basemap

echo "Analysis complete!"
```

### Single Location Quick Analysis

```bash
#!/bin/bash
# Quick analysis for a single zip code

ZIPCODE="89701"
BUFFER=15

# Retrieve, analyze, and visualize in one go
pygwretrieval retrieve --zipcode $ZIPCODE --buffer $BUFFER --output data.csv
pygwretrieval info --input data.csv --detailed
pygwretrieval aggregate --input data.csv --period monthly --output monthly.csv
pygwretrieval plot --input data.csv --type timeseries --output timeseries.png
pygwretrieval map --input data.csv --output map.png --basemap

echo "Done! Check data.csv, monthly.csv, timeseries.png, and map.png"
```
