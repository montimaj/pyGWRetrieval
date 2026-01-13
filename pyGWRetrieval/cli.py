#!/usr/bin/env python
"""
Command Line Interface for pyGWRetrieval.

This module provides a comprehensive CLI for retrieving and analyzing
USGS groundwater level data from the command line.

Usage:
    pygwretrieval --help
    pygwretrieval retrieve --zipcode 89701 --buffer 10 --output data.csv
    pygwretrieval retrieve --csv locations.csv --zipcode-column zip --parallel
    pygwretrieval aggregate --input data.csv --period monthly --output monthly.csv
    pygwretrieval plot --input data.csv --type timeseries --output plot.png
    pygwretrieval map --input data.csv --output wells_map.png
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, List
from datetime import datetime

import pandas as pd


def setup_logging(verbose: bool = False, quiet: bool = False) -> None:
    """Configure logging based on verbosity flags."""
    if quiet:
        level = logging.ERROR
    elif verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser with all subcommands."""
    
    # Main parser
    parser = argparse.ArgumentParser(
        prog='pygwretrieval',
        description='pyGWRetrieval - USGS Groundwater Data Retrieval and Analysis Tool',
        epilog='''
Examples:
  # Retrieve data by zip code (field measurements only - default)
  pygwretrieval retrieve --zipcode 89701 --buffer 10 --output data.csv

  # Retrieve data from all sources (gwlevels, dv, iv)
  pygwretrieval retrieve --zipcode 89701 --data-sources all --output data.csv

  # Retrieve field measurements and daily values
  pygwretrieval retrieve --zipcode 89701 --data-sources gwlevels dv --output data.csv

  # Retrieve data from CSV with multiple zip codes (parallel processing)
  pygwretrieval retrieve --csv locations.csv --zipcode-column zip --parallel --output data.csv

  # Retrieve data by shapefile
  pygwretrieval retrieve --shapefile basin.shp --output basin_data.csv

  # Aggregate data to monthly
  pygwretrieval aggregate --input data.csv --period monthly --output monthly.csv

  # Create time series plot
  pygwretrieval plot --input data.csv --type timeseries --output timeseries.png

  # Create spatial map
  pygwretrieval map --input data.csv --output wells_map.png --basemap

Data Sources:
  gwlevels - Field groundwater-level measurements (discrete, most accurate)
  dv       - Daily values (daily summaries from continuous sensors)
  iv       - Instantaneous values (15-60 min intervals from sensors)
  all      - All available sources

For more information, visit: https://github.com/example/pyGWRetrieval
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--version', '-V',
        action='version',
        version='%(prog)s 0.1.0'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress all output except errors'
    )
    
    # Subparsers for commands
    subparsers = parser.add_subparsers(
        dest='command',
        title='Commands',
        description='Available commands',
        metavar='COMMAND'
    )
    
    # =========================================================================
    # RETRIEVE command
    # =========================================================================
    retrieve_parser = subparsers.add_parser(
        'retrieve',
        help='Retrieve groundwater data from USGS NWIS',
        description='''
Retrieve groundwater level data from USGS NWIS based on spatial inputs.

Supports multiple input methods:
  - Single zip code with buffer
  - Multiple zip codes from CSV file
  - Shapefile (polygon or point with buffer)
  - GeoJSON file
  - State code
  - Specific site numbers
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Input source group (mutually exclusive)
    input_group = retrieve_parser.add_argument_group('Input Source (choose one)')
    source_group = input_group.add_mutually_exclusive_group(required=True)
    
    source_group.add_argument(
        '--zipcode', '-z',
        type=str,
        metavar='ZIP',
        help='Single zip code to query'
    )
    
    source_group.add_argument(
        '--csv', '-c',
        type=str,
        metavar='FILE',
        help='CSV file with zip codes'
    )
    
    source_group.add_argument(
        '--shapefile', '-shp',
        type=str,
        metavar='FILE',
        help='Shapefile path (.shp)'
    )
    
    source_group.add_argument(
        '--geojson', '-gj',
        type=str,
        metavar='FILE',
        help='GeoJSON file path'
    )
    
    source_group.add_argument(
        '--state', '-st',
        type=str,
        metavar='CODE',
        help='Two-letter state code (e.g., NV, CA)'
    )
    
    source_group.add_argument(
        '--sites', '-s',
        type=str,
        nargs='+',
        metavar='SITE',
        help='USGS site numbers (space-separated)'
    )
    
    # CSV options
    csv_group = retrieve_parser.add_argument_group('CSV Options')
    csv_group.add_argument(
        '--zipcode-column',
        type=str,
        default='zipcode',
        metavar='COL',
        help='Column name containing zip codes (default: zipcode)'
    )
    
    csv_group.add_argument(
        '--save-per-zipcode',
        action='store_true',
        help='Save separate files for each zip code'
    )
    
    csv_group.add_argument(
        '--per-zipcode-dir',
        type=str,
        metavar='DIR',
        help='Directory for per-zipcode files (default: output_by_zipcode/)'
    )
    
    # Spatial options
    spatial_group = retrieve_parser.add_argument_group('Spatial Options')
    spatial_group.add_argument(
        '--buffer', '-b',
        type=float,
        default=10.0,
        metavar='MILES',
        help='Buffer distance in miles (default: 10)'
    )
    
    # Temporal options
    temporal_group = retrieve_parser.add_argument_group('Temporal Options')
    temporal_group.add_argument(
        '--start-date',
        type=str,
        default='1970-01-01',
        metavar='DATE',
        help='Start date (YYYY-MM-DD, default: 1970-01-01)'
    )
    
    temporal_group.add_argument(
        '--end-date',
        type=str,
        metavar='DATE',
        help='End date (YYYY-MM-DD, default: today)'
    )
    
    # Data source options
    source_options_group = retrieve_parser.add_argument_group('Data Source Options')
    source_options_group.add_argument(
        '--data-sources',
        type=str,
        nargs='+',
        default=['gwlevels'],
        metavar='SOURCE',
        help='''USGS data sources to retrieve (default: gwlevels).
Options: gwlevels (field measurements), dv (daily values), iv (instantaneous values), all.
Examples: --data-sources gwlevels dv, --data-sources all'''
    )
    
    # Processing options
    proc_group = retrieve_parser.add_argument_group('Processing Options')
    proc_group.add_argument(
        '--parallel', '-p',
        action='store_true',
        help='Enable parallel processing (for CSV input)'
    )
    
    proc_group.add_argument(
        '--workers', '-w',
        type=int,
        metavar='N',
        help='Number of parallel workers (default: auto)'
    )
    
    proc_group.add_argument(
        '--scheduler',
        choices=['threads', 'processes', 'synchronous'],
        default='threads',
        help='Dask scheduler type (default: threads)'
    )
    
    # Output options
    output_group = retrieve_parser.add_argument_group('Output Options')
    output_group.add_argument(
        '--output', '-o',
        type=str,
        metavar='FILE',
        help='Output file path (CSV or Parquet based on extension)'
    )
    
    output_group.add_argument(
        '--format', '-f',
        choices=['csv', 'parquet'],
        default='csv',
        help='Output format (default: csv)'
    )
    
    output_group.add_argument(
        '--wells-output',
        type=str,
        metavar='FILE',
        help='Output file for well locations (GeoJSON or Shapefile)'
    )
    
    # =========================================================================
    # AGGREGATE command
    # =========================================================================
    aggregate_parser = subparsers.add_parser(
        'aggregate',
        help='Aggregate groundwater data temporally',
        description='''
Temporally aggregate groundwater level data to different periods.

Supported periods:
  - monthly: Monthly aggregation
  - annual: Calendar year aggregation
  - water-year: Water year (Oct-Sep) aggregation
  - growing-season: Growing season aggregation
  - custom: Custom month range
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    aggregate_parser.add_argument(
        '--input', '-i',
        type=str,
        required=True,
        metavar='FILE',
        help='Input CSV or Parquet file'
    )
    
    aggregate_parser.add_argument(
        '--output', '-o',
        type=str,
        required=True,
        metavar='FILE',
        help='Output file path'
    )
    
    aggregate_parser.add_argument(
        '--period', '-p',
        choices=['monthly', 'annual', 'water-year', 'growing-season', 'custom'],
        default='monthly',
        help='Aggregation period (default: monthly)'
    )
    
    aggregate_parser.add_argument(
        '--agg-func',
        choices=['mean', 'median', 'min', 'max', 'std', 'count'],
        default='mean',
        help='Aggregation function (default: mean)'
    )
    
    aggregate_parser.add_argument(
        '--start-month',
        type=int,
        choices=range(1, 13),
        metavar='M',
        help='Start month for custom/growing-season (1-12)'
    )
    
    aggregate_parser.add_argument(
        '--end-month',
        type=int,
        choices=range(1, 13),
        metavar='M',
        help='End month for custom/growing-season (1-12)'
    )
    
    # Column name options
    col_group = aggregate_parser.add_argument_group('Column Options')
    col_group.add_argument(
        '--date-column',
        type=str,
        default='lev_dt',
        help='Date column name (default: lev_dt)'
    )
    
    col_group.add_argument(
        '--value-column',
        type=str,
        default='lev_va',
        help='Value column name (default: lev_va)'
    )
    
    col_group.add_argument(
        '--site-column',
        type=str,
        default='site_no',
        help='Site column name (default: site_no)'
    )
    
    # =========================================================================
    # STATS command
    # =========================================================================
    stats_parser = subparsers.add_parser(
        'stats',
        help='Calculate statistics and trends',
        description='Calculate statistics and trend analysis for groundwater data.'
    )
    
    stats_parser.add_argument(
        '--input', '-i',
        type=str,
        required=True,
        metavar='FILE',
        help='Input CSV or Parquet file'
    )
    
    stats_parser.add_argument(
        '--output', '-o',
        type=str,
        required=True,
        metavar='FILE',
        help='Output file path'
    )
    
    stats_parser.add_argument(
        '--type', '-t',
        choices=['statistics', 'trends', 'both'],
        default='both',
        help='Type of analysis (default: both)'
    )
    
    stats_parser.add_argument(
        '--trend-period',
        choices=['monthly', 'annual'],
        default='annual',
        help='Period for trend analysis (default: annual)'
    )
    
    stats_parser.add_argument(
        '--parallel', '-p',
        action='store_true',
        help='Enable parallel processing for trend analysis'
    )
    
    stats_parser.add_argument(
        '--groupby',
        type=str,
        default='site_no',
        help='Column to group by (default: site_no)'
    )
    
    # =========================================================================
    # PLOT command
    # =========================================================================
    plot_parser = subparsers.add_parser(
        'plot',
        help='Create visualizations',
        description='''
Create various visualizations from groundwater data.

Plot types:
  - timeseries: Time series plot for wells
  - single-well: Detailed plot for a single well
  - boxplot: Monthly boxplot distribution
  - annual: Annual summary plot
  - heatmap: Data availability heatmap
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    plot_parser.add_argument(
        '--input', '-i',
        type=str,
        required=True,
        metavar='FILE',
        help='Input CSV or Parquet file'
    )
    
    plot_parser.add_argument(
        '--output', '-o',
        type=str,
        required=True,
        metavar='FILE',
        help='Output image file (PNG, PDF, SVG)'
    )
    
    plot_parser.add_argument(
        '--type', '-t',
        choices=['timeseries', 'single-well', 'boxplot', 'annual', 'heatmap'],
        default='timeseries',
        help='Plot type (default: timeseries)'
    )
    
    plot_parser.add_argument(
        '--wells', '-w',
        type=str,
        nargs='+',
        metavar='SITE',
        help='Well site numbers to plot (default: top 5 by record count)'
    )
    
    plot_parser.add_argument(
        '--title',
        type=str,
        help='Custom plot title'
    )
    
    plot_parser.add_argument(
        '--figsize',
        type=int,
        nargs=2,
        metavar=('W', 'H'),
        default=[12, 8],
        help='Figure size in inches (default: 12 8)'
    )
    
    plot_parser.add_argument(
        '--dpi',
        type=int,
        default=300,
        help='Output resolution (default: 300)'
    )
    
    plot_parser.add_argument(
        '--show-trend',
        action='store_true',
        help='Show trend line (for single-well plot)'
    )
    
    plot_parser.add_argument(
        '--show-stats',
        action='store_true',
        help='Show statistics (for single-well plot)'
    )
    
    # =========================================================================
    # MAP command
    # =========================================================================
    map_parser = subparsers.add_parser(
        'map',
        help='Create spatial map visualization',
        description='''
Create a spatial map showing groundwater wells colored by water level.

Features:
  - Automatic zoom based on data extent
  - Optional basemap from various providers
  - Group annotations (e.g., by zip code)
  - Colormap customization
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    map_parser.add_argument(
        '--input', '-i',
        type=str,
        required=True,
        metavar='FILE',
        help='Input CSV or Parquet file'
    )
    
    map_parser.add_argument(
        '--output', '-o',
        type=str,
        required=True,
        metavar='FILE',
        help='Output image file (PNG, PDF, SVG)'
    )
    
    map_parser.add_argument(
        '--agg-func',
        choices=['mean', 'median', 'min', 'max'],
        default='mean',
        help='Aggregation function for water levels (default: mean)'
    )
    
    map_parser.add_argument(
        '--cmap',
        type=str,
        default='RdYlBu_r',
        help='Colormap name (default: RdYlBu_r)'
    )
    
    map_parser.add_argument(
        '--basemap', '-m',
        action='store_true',
        help='Add basemap'
    )
    
    map_parser.add_argument(
        '--basemap-source',
        choices=[
            'CartoDB.Positron', 'CartoDB.DarkMatter',
            'OpenStreetMap.Mapnik', 'Esri.WorldImagery',
            'Esri.WorldStreetMap', 'Esri.WorldTopoMap'
        ],
        default='CartoDB.Positron',
        help='Basemap provider (default: CartoDB.Positron)'
    )
    
    map_parser.add_argument(
        '--group-by',
        type=str,
        metavar='COL',
        help='Column to group wells by (e.g., source_zipcode)'
    )
    
    map_parser.add_argument(
        '--marker-size',
        type=int,
        metavar='SIZE',
        help='Marker size (default: auto-scaled)'
    )
    
    map_parser.add_argument(
        '--title',
        type=str,
        help='Custom map title'
    )
    
    map_parser.add_argument(
        '--figsize',
        type=int,
        nargs=2,
        metavar=('W', 'H'),
        help='Figure size in inches (default: auto-scaled)'
    )
    
    map_parser.add_argument(
        '--dpi',
        type=int,
        default=300,
        help='Output resolution (default: 300)'
    )
    
    map_parser.add_argument(
        '--comparison',
        action='store_true',
        help='Create 4-panel comparison map (mean, count, min, max)'
    )
    
    # =========================================================================
    # INFO command
    # =========================================================================
    info_parser = subparsers.add_parser(
        'info',
        help='Display information about data file',
        description='Display summary information about a groundwater data file.'
    )
    
    info_parser.add_argument(
        '--input', '-i',
        type=str,
        required=True,
        metavar='FILE',
        help='Input CSV or Parquet file'
    )
    
    info_parser.add_argument(
        '--detailed',
        action='store_true',
        help='Show detailed statistics'
    )
    
    return parser


# =============================================================================
# Command handlers
# =============================================================================

def cmd_retrieve(args: argparse.Namespace) -> int:
    """Handle the retrieve command."""
    from pyGWRetrieval import GroundwaterRetrieval
    
    logger = logging.getLogger(__name__)
    
    # Parse data sources
    data_sources = args.data_sources
    if isinstance(data_sources, list) and len(data_sources) == 1:
        data_sources = data_sources[0]  # Single source as string
    if data_sources == ['all'] or data_sources == 'all':
        data_sources = 'all'
    
    # Initialize retrieval
    gw = GroundwaterRetrieval(
        start_date=args.start_date,
        end_date=args.end_date,
        data_sources=data_sources
    )
    
    logger.info(f"Date range: {args.start_date} to {args.end_date or 'present'}")
    logger.info(f"Data sources: {data_sources}")
    
    # Retrieve data based on input source
    data = None
    
    if args.zipcode:
        logger.info(f"Retrieving data for zip code {args.zipcode} with {args.buffer} mile buffer")
        data = gw.get_data_by_zipcode(args.zipcode, buffer_miles=args.buffer)
        
    elif args.csv:
        csv_path = Path(args.csv)
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_path}")
            return 1
        
        logger.info(f"Retrieving data from CSV: {csv_path}")
        logger.info(f"Zip code column: {args.zipcode_column}")
        logger.info(f"Parallel processing: {args.parallel}")
        
        data = gw.get_data_by_zipcodes_csv(
            filepath=csv_path,
            zipcode_column=args.zipcode_column,
            buffer_miles=args.buffer,
            merge_results=True,
            parallel=args.parallel,
            n_workers=args.workers,
            scheduler=args.scheduler
        )
        
        # Save per zipcode if requested
        if args.save_per_zipcode:
            per_zip_dir = Path(args.per_zipcode_dir or 'output_by_zipcode')
            logger.info(f"Saving data per zip code to: {per_zip_dir}")
            saved_files = gw.save_data_per_zipcode(
                output_dir=per_zip_dir,
                file_format=args.format
            )
            for zipcode, filepath in saved_files.items():
                logger.info(f"  Saved {zipcode}: {filepath}")
                
    elif args.shapefile:
        shp_path = Path(args.shapefile)
        if not shp_path.exists():
            logger.error(f"Shapefile not found: {shp_path}")
            return 1
        
        logger.info(f"Retrieving data from shapefile: {shp_path}")
        data = gw.get_data_by_shapefile(shp_path, buffer_miles=args.buffer)
        
    elif args.geojson:
        gj_path = Path(args.geojson)
        if not gj_path.exists():
            logger.error(f"GeoJSON file not found: {gj_path}")
            return 1
        
        logger.info(f"Retrieving data from GeoJSON: {gj_path}")
        data = gw.get_data_by_geojson(gj_path, buffer_miles=args.buffer)
        
    elif args.state:
        logger.info(f"Retrieving data for state: {args.state}")
        data = gw.get_data_by_state(args.state)
        
    elif args.sites:
        logger.info(f"Retrieving data for {len(args.sites)} sites")
        data = gw.get_data_by_sites(args.sites)
    
    # Check if data was retrieved
    if data is None or data.empty:
        logger.warning("No data retrieved")
        return 1
    
    # Display summary
    n_records = len(data)
    n_wells = data['site_no'].nunique()
    logger.info(f"Retrieved {n_records:,} records from {n_wells:,} wells")
    
    # Save output
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if args.format == 'parquet' or output_path.suffix == '.parquet':
            gw.to_parquet(output_path)
        else:
            gw.to_csv(output_path)
        
        logger.info(f"Saved data to: {output_path}")
    
    # Save wells if requested
    if args.wells_output:
        wells_path = Path(args.wells_output)
        wells_path.parent.mkdir(parents=True, exist_ok=True)
        
        driver = 'GeoJSON' if wells_path.suffix.lower() == '.geojson' else 'ESRI Shapefile'
        gw.save_wells_to_file(wells_path, driver=driver)
        logger.info(f"Saved well locations to: {wells_path}")
    
    return 0


def cmd_aggregate(args: argparse.Namespace) -> int:
    """Handle the aggregate command."""
    from pyGWRetrieval import TemporalAggregator
    
    logger = logging.getLogger(__name__)
    
    # Load data
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return 1
    
    logger.info(f"Loading data from: {input_path}")
    
    if input_path.suffix == '.parquet':
        data = pd.read_parquet(input_path)
    else:
        data = pd.read_csv(input_path)
    
    logger.info(f"Loaded {len(data):,} records")
    
    # Create aggregator
    aggregator = TemporalAggregator(
        data,
        date_column=args.date_column,
        value_column=args.value_column,
        site_column=args.site_column
    )
    
    # Perform aggregation based on period
    if args.period == 'monthly':
        logger.info(f"Aggregating to monthly ({args.agg_func})")
        result = aggregator.to_monthly(agg_func=args.agg_func)
        
    elif args.period == 'annual':
        logger.info(f"Aggregating to annual ({args.agg_func})")
        result = aggregator.to_annual(agg_func=args.agg_func, water_year=False)
        
    elif args.period == 'water-year':
        logger.info(f"Aggregating to water year ({args.agg_func})")
        result = aggregator.to_annual(agg_func=args.agg_func, water_year=True)
        
    elif args.period == 'growing-season':
        start_month = args.start_month or 4
        end_month = args.end_month or 9
        logger.info(f"Aggregating to growing season (months {start_month}-{end_month}, {args.agg_func})")
        result = aggregator.to_growing_season(
            start_month=start_month,
            end_month=end_month,
            agg_func=args.agg_func
        )
        
    elif args.period == 'custom':
        if not args.start_month or not args.end_month:
            logger.error("Custom period requires --start-month and --end-month")
            return 1
        logger.info(f"Aggregating to custom period (months {args.start_month}-{args.end_month}, {args.agg_func})")
        result = aggregator.to_custom_period(
            months=list(range(args.start_month, args.end_month + 1)),
            agg_func=args.agg_func
        )
    
    # Save result
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if output_path.suffix == '.parquet':
        result.to_parquet(output_path, index=False)
    else:
        result.to_csv(output_path, index=False)
    
    logger.info(f"Saved {len(result):,} aggregated records to: {output_path}")
    
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    """Handle the stats command."""
    from pyGWRetrieval import TemporalAggregator
    
    logger = logging.getLogger(__name__)
    
    # Load data
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return 1
    
    logger.info(f"Loading data from: {input_path}")
    
    if input_path.suffix == '.parquet':
        data = pd.read_parquet(input_path)
    else:
        data = pd.read_csv(input_path)
    
    logger.info(f"Loaded {len(data):,} records")
    
    # Create aggregator
    aggregator = TemporalAggregator(data)
    
    output_path = Path(args.output)
    output_base = output_path.stem
    output_dir = output_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    results = []
    
    # Calculate statistics
    if args.type in ['statistics', 'both']:
        logger.info(f"Calculating statistics grouped by {args.groupby}")
        stats = aggregator.calculate_statistics(groupby=args.groupby)
        
        stats_file = output_dir / f"{output_base}_statistics.csv"
        stats.to_csv(stats_file, index=False)
        logger.info(f"Saved statistics to: {stats_file}")
        results.append(('statistics', stats))
    
    # Calculate trends
    if args.type in ['trends', 'both']:
        logger.info(f"Calculating trends (period: {args.trend_period}, parallel: {args.parallel})")
        trends = aggregator.get_trends(
            period=args.trend_period,
            parallel=args.parallel
        )
        
        trends_file = output_dir / f"{output_base}_trends.csv"
        trends.to_csv(trends_file, index=False)
        logger.info(f"Saved trends to: {trends_file}")
        results.append(('trends', trends))
        
        # Show significant trends
        sig_trends = trends[trends['p_value'] < 0.05]
        if not sig_trends.empty:
            logger.info(f"Found {len(sig_trends)} wells with significant trends (p < 0.05)")
    
    return 0


def cmd_plot(args: argparse.Namespace) -> int:
    """Handle the plot command."""
    from pyGWRetrieval import GroundwaterPlotter
    import matplotlib.pyplot as plt
    
    logger = logging.getLogger(__name__)
    
    # Load data
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return 1
    
    logger.info(f"Loading data from: {input_path}")
    
    if input_path.suffix == '.parquet':
        data = pd.read_parquet(input_path)
    else:
        data = pd.read_csv(input_path)
    
    logger.info(f"Loaded {len(data):,} records from {data['site_no'].nunique()} wells")
    
    # Create plotter
    plotter = GroundwaterPlotter(data)
    
    # Create plot based on type
    fig = None
    
    if args.type == 'timeseries':
        logger.info("Creating time series plot")
        fig = plotter.plot_time_series(
            site_numbers=args.wells,
            title=args.title,
            figsize=tuple(args.figsize) if args.figsize else None
        )
        
    elif args.type == 'single-well':
        if not args.wells:
            # Get well with most records
            wells = data.groupby('site_no')['lev_dt'].count().idxmax()
            well = wells if isinstance(wells, str) else wells
        else:
            well = args.wells[0]
        
        logger.info(f"Creating single well plot for: {well}")
        fig = plotter.plot_single_well(
            well,
            title=args.title,
            show_trend=args.show_trend,
            show_stats=args.show_stats,
            figsize=tuple(args.figsize) if args.figsize else None
        )
        
    elif args.type == 'boxplot':
        logger.info("Creating monthly boxplot")
        well = args.wells[0] if args.wells else None
        fig = plotter.plot_monthly_boxplot(
            site_no=well,
            title=args.title,
            figsize=tuple(args.figsize) if args.figsize else None
        )
        
    elif args.type == 'annual':
        logger.info("Creating annual summary plot")
        fig = plotter.plot_annual_summary(
            title=args.title,
            figsize=tuple(args.figsize) if args.figsize else None
        )
        
    elif args.type == 'heatmap':
        logger.info("Creating data availability heatmap")
        fig = plotter.plot_data_availability_heatmap(
            title=args.title,
            figsize=tuple(args.figsize) if args.figsize else None
        )
    
    # Save figure
    if fig is not None:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=args.dpi, bbox_inches='tight')
        plt.close(fig)
        logger.info(f"Saved plot to: {output_path}")
    else:
        logger.error("Failed to create plot")
        return 1
    
    return 0


def cmd_map(args: argparse.Namespace) -> int:
    """Handle the map command."""
    from pyGWRetrieval import GroundwaterPlotter, plot_wells_map, create_comparison_map
    import matplotlib.pyplot as plt
    
    logger = logging.getLogger(__name__)
    
    # Load data
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return 1
    
    logger.info(f"Loading data from: {input_path}")
    
    if input_path.suffix == '.parquet':
        data = pd.read_parquet(input_path)
    else:
        data = pd.read_csv(input_path)
    
    n_wells = data['site_no'].nunique()
    logger.info(f"Loaded {len(data):,} records from {n_wells} wells")
    
    # Create map
    if args.comparison:
        logger.info("Creating comparison map (4 panels)")
        figsize = tuple(args.figsize) if args.figsize else (18, 12)
        fig = create_comparison_map(
            data,
            figsize=figsize,
            add_basemap=args.basemap
        )
    else:
        logger.info(f"Creating spatial map (agg_func: {args.agg_func})")
        
        fig = plot_wells_map(
            data,
            agg_func=args.agg_func,
            title=args.title,
            cmap=args.cmap,
            add_basemap=args.basemap,
            group_by_column=args.group_by
        )
    
    # Save figure
    if fig is not None:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=args.dpi, bbox_inches='tight')
        plt.close(fig)
        logger.info(f"Saved map to: {output_path}")
    else:
        logger.error("Failed to create map")
        return 1
    
    return 0


def cmd_info(args: argparse.Namespace) -> int:
    """Handle the info command."""
    logger = logging.getLogger(__name__)
    
    # Load data
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return 1
    
    if input_path.suffix == '.parquet':
        data = pd.read_parquet(input_path)
    else:
        data = pd.read_csv(input_path)
    
    # Convert date column
    if 'lev_dt' in data.columns:
        data['lev_dt'] = pd.to_datetime(data['lev_dt'], errors='coerce')
    
    # Basic info
    print("\n" + "=" * 60)
    print("GROUNDWATER DATA SUMMARY")
    print("=" * 60)
    print(f"\nFile: {input_path}")
    print(f"Records: {len(data):,}")
    print(f"Columns: {len(data.columns)}")
    
    if 'site_no' in data.columns:
        print(f"Unique Wells: {data['site_no'].nunique():,}")
    
    if 'lev_dt' in data.columns and not data['lev_dt'].isna().all():
        print(f"Date Range: {data['lev_dt'].min().date()} to {data['lev_dt'].max().date()}")
    
    if 'source_zipcode' in data.columns:
        print(f"Zip Codes: {data['source_zipcode'].nunique()}")
    
    # Column info
    print(f"\nColumns: {list(data.columns)}")
    
    if args.detailed:
        print("\n" + "-" * 60)
        print("DETAILED STATISTICS")
        print("-" * 60)
        
        # Water level statistics
        if 'lev_va' in data.columns:
            wl = data['lev_va'].dropna()
            print(f"\nWater Level (lev_va) - feet below surface:")
            print(f"  Count:  {len(wl):,}")
            print(f"  Mean:   {wl.mean():.2f}")
            print(f"  Std:    {wl.std():.2f}")
            print(f"  Min:    {wl.min():.2f}")
            print(f"  25%:    {wl.quantile(0.25):.2f}")
            print(f"  50%:    {wl.median():.2f}")
            print(f"  75%:    {wl.quantile(0.75):.2f}")
            print(f"  Max:    {wl.max():.2f}")
        
        # Records per well
        if 'site_no' in data.columns:
            records_per_well = data.groupby('site_no').size()
            print(f"\nRecords per well:")
            print(f"  Min:    {records_per_well.min()}")
            print(f"  Mean:   {records_per_well.mean():.1f}")
            print(f"  Max:    {records_per_well.max()}")
        
        # Records per zip code
        if 'source_zipcode' in data.columns:
            records_per_zip = data.groupby('source_zipcode').size()
            print(f"\nRecords per zip code:")
            for zipcode, count in records_per_zip.items():
                n_wells = data[data['source_zipcode'] == zipcode]['site_no'].nunique()
                print(f"  {zipcode}: {count:,} records, {n_wells} wells")
        
        # Data coverage by year
        if 'lev_dt' in data.columns and not data['lev_dt'].isna().all():
            print(f"\nRecords by year (first/last 5):")
            yearly = data.groupby(data['lev_dt'].dt.year).size()
            years = yearly.index.tolist()
            if len(years) > 10:
                for year in years[:5]:
                    print(f"  {year}: {yearly[year]:,}")
                print("  ...")
                for year in years[-5:]:
                    print(f"  {year}: {yearly[year]:,}")
            else:
                for year, count in yearly.items():
                    print(f"  {year}: {count:,}")
    
    print("\n" + "=" * 60)
    
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    """Main entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args(argv)
    
    # Set up logging
    setup_logging(verbose=args.verbose, quiet=args.quiet)
    
    # Handle no command
    if args.command is None:
        parser.print_help()
        return 0
    
    # Dispatch to command handler
    handlers = {
        'retrieve': cmd_retrieve,
        'aggregate': cmd_aggregate,
        'stats': cmd_stats,
        'plot': cmd_plot,
        'map': cmd_map,
        'info': cmd_info,
    }
    
    handler = handlers.get(args.command)
    if handler is None:
        logging.error(f"Unknown command: {args.command}")
        return 1
    
    try:
        return handler(args)
    except KeyboardInterrupt:
        logging.info("Operation cancelled by user")
        return 130
    except Exception as e:
        logging.error(f"Error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
