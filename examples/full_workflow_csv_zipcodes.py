#!/usr/bin/env python
"""
Example: Full Workflow - CSV Zip Codes to Groundwater Analysis

This script demonstrates a complete workflow for:
1. Reading zip codes from a CSV file
2. Downloading groundwater data within a buffer of each zip code
3. Retrieving data from multiple USGS sources (gwlevels, dv, iv)
4. Saving data per zip code
5. Temporal aggregation and analysis
6. Visualization of results

Dataset: AirbnbMSACity_with_ZipCode.csv (Chicago metro area locations)

USGS Data Sources:
------------------
- gwlevels: Field groundwater-level measurements (discrete manual readings)
- dv: Daily values (daily statistical summaries from sensors)
- iv: Instantaneous values (current/historical observations, 15-60 min intervals)

USGS Data Columns Retrieved:
----------------------------
- site_no: USGS site identification number
- lev_dt/datetime: Date of water level measurement (YYYY-MM-DD)
- lev_tm: Time of measurement (HH:MM)
- lev_va/value: Water level value in FEET BELOW LAND SURFACE
          (lower values = shallower water, higher values = deeper water)
- data_source: Source of the data (gwlevels, dv, or iv)
- lev_acy_cd: Water level accuracy code
- lev_src_cd: Source of water level data
- lev_meth_cd: Method of measurement (S=steel tape, E=electric tape, T=transducer)
- lev_status_cd: Status of the site at time of measurement
- station_nm: Station name
- dec_lat_va: Decimal latitude (degrees)
- dec_long_va: Decimal longitude (degrees)
- source_zipcode: Origin zip code (added by pyGWRetrieval for CSV queries)

Units:
------
- lev_va: Feet below land surface (e.g., 25.5 means water is 25.5 ft underground)
- Coordinates: Decimal degrees (WGS84)
"""

import os
import sys
from pathlib import Path

# Add parent directory to path for development
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyGWRetrieval import (
    GroundwaterRetrieval,
    TemporalAggregator,
    GroundwaterPlotter,
    setup_logging,
)
import matplotlib.pyplot as plt
import pandas as pd
import logging


def main():
    """Main function demonstrating full CSV zip code workflow."""
    
    # Set up logging to see progress
    setup_logging(level=logging.INFO)
    
    print("=" * 70)
    print("pyGWRetrieval - Full Workflow: CSV Zip Codes Example")
    print("=" * 70)
    
    # -------------------------------------------------------------------------
    # Configuration
    # -------------------------------------------------------------------------
    
    # Path to CSV file with zip codes
    csv_file = Path(__file__).parent / "AirbnbMSACity_with_ZipCode.csv"
    
    # Output directories
    output_dir = Path(__file__).parent / "output"
    data_per_zipcode_dir = output_dir / "data_by_zipcode"
    plots_dir = output_dir / "plots"
    
    # Create output directories
    output_dir.mkdir(exist_ok=True)
    data_per_zipcode_dir.mkdir(exist_ok=True)
    plots_dir.mkdir(exist_ok=True)
    
    # Parameters
    START_DATE = "1970-01-01"
    END_DATE = None  # Uses current date (present)
    BUFFER_MILES = 100
    ZIPCODE_COLUMN = "ZipCode"
    
    # Data sources: 'gwlevels', 'dv', 'iv', or 'all'
    # - gwlevels: Field measurements (discrete, most accurate)
    # - dv: Daily values (daily summaries from sensors)
    # - iv: Instantaneous values (high-frequency sensor data)
    DATA_SOURCES = ['gwlevels']  # Retrieve from gwlevels
    
    print(f"\nConfiguration:")
    print(f"  CSV File: {csv_file}")
    print(f"  Date Range: {START_DATE} to present")
    print(f"  Buffer: {BUFFER_MILES} miles")
    print(f"  Zip Code Column: {ZIPCODE_COLUMN}")
    print(f"  Data Sources: {DATA_SOURCES}")
    
    # -------------------------------------------------------------------------
    # Step 1: Preview the CSV file
    # -------------------------------------------------------------------------
    print("\n" + "-" * 70)
    print("Step 1: Preview CSV File")
    print("-" * 70)
    
    csv_data = pd.read_csv(csv_file)
    print(f"\nCSV contains {len(csv_data)} rows")
    print(f"Columns: {list(csv_data.columns)}")
    print(f"\nFirst 5 rows:")
    print(csv_data.head())
    
    unique_zipcodes = csv_data[ZIPCODE_COLUMN].dropna().unique()
    print(f"\nUnique zip codes to process: {len(unique_zipcodes)}")
    print(f"Zip codes: {list(unique_zipcodes)}")
    
    # -------------------------------------------------------------------------
    # Step 2: Initialize Retrieval and Download Data (with Parallel Processing)
    # -------------------------------------------------------------------------
    print("\n" + "-" * 70)
    print("Step 2: Download Groundwater Data for All Zip Codes (Parallel)")
    print("-" * 70)
    
    # Check parallel processing availability
    from pyGWRetrieval import check_dask_available, get_parallel_config
    
    parallel_available = check_dask_available()
    print(f"\nParallel processing available: {parallel_available}")
    if parallel_available:
        config = get_parallel_config()
        print(f"Dask version: {config.get('dask_version', 'N/A')}")
        print(f"Available workers: {config.get('num_workers', 'auto')}")
    
    # Initialize the retrieval object with data sources
    gw = GroundwaterRetrieval(
        start_date=START_DATE,
        end_date=END_DATE,
        data_sources=DATA_SOURCES  # Specify which USGS data sources to query
    )
    
    # Retrieve data for all zip codes in the CSV
    # Parallel processing is enabled by default when Dask is available
    print(f"\nRetrieving data from USGS NWIS...")
    print(f"Data sources: {DATA_SOURCES}")
    print(f"Using {'parallel' if parallel_available else 'sequential'} processing...")
    print(f"This may take several minutes depending on the number of zip codes...")
    
    data = gw.get_data_by_zipcodes_csv(
        filepath=csv_file,
        zipcode_column=ZIPCODE_COLUMN,
        buffer_miles=BUFFER_MILES,
        merge_results=True,
        parallel=True,           # Enable parallel processing
        n_workers=None,          # Auto-detect number of workers
        scheduler='threads'      # Use thread-based parallelism
    )
    
    if data.empty:
        print("\nNo data retrieved. This could be due to:")
        print("  - No groundwater wells within the buffer distance")
        print("  - Network issues with USGS NWIS")
        print("  - Invalid zip codes")
        return
    
    # Display summary
    print(f"\n✓ Successfully retrieved data!")
    print(f"  Total records: {len(data):,}")
    print(f"  Unique wells: {data['site_no'].nunique():,}")
    print(f"  Zip codes with data: {data['source_zipcode'].nunique()}")
    
    # Summary by data source (if multiple sources were queried)
    if 'data_source' in data.columns:
        print("\nRecords by data source:")
        source_summary = data.groupby('data_source').agg({
            'site_no': 'nunique',
            'lev_va': 'count'
        }).rename(columns={'site_no': 'wells', 'lev_va': 'records'})
        print(source_summary)
    
    # Summary by zip code
    print("\nRecords per zip code:")
    zipcode_summary = data.groupby('source_zipcode').agg({
        'site_no': 'nunique',
        'lev_dt': 'count'
    }).rename(columns={'site_no': 'wells', 'lev_dt': 'records'})
    print(zipcode_summary)
    
    # -------------------------------------------------------------------------
    # Step 3: Save Data
    # -------------------------------------------------------------------------
    print("\n" + "-" * 70)
    print("Step 3: Save Data")
    print("-" * 70)
    
    # Save combined data
    combined_file = output_dir / "all_groundwater_data.parquet" # csv is too large and slow
    gw.save_to_parquet(combined_file, data)
    gw.to_csv(output_dir / "all_groundwater_data.csv", data)
    print(f"\n✓ Saved combined data to: {combined_file}")
    
    # Save data per zip code
    print(f"\nSaving data per zip code to: {data_per_zipcode_dir}")
    saved_files = gw.save_data_per_zipcode(
        output_dir=data_per_zipcode_dir,
        file_format='parquet',
        prefix='gw_data'
    )
    
    print(f"✓ Saved {len(saved_files)} files:")
    for zipcode, filepath in saved_files.items():
        file_size = filepath.stat().st_size / 1024  # KB
        print(f"  {zipcode}: {filepath.name} ({file_size:.1f} KB)")
    
    # Save wells shapefile/geojson
    if gw.wells is not None and not gw.wells.empty:
        wells_file = output_dir / "groundwater_wells.geojson"
        gw.save_wells_to_file(wells_file, driver='GeoJSON')
        print(f"\n✓ Saved wells locations to: {wells_file}")
    
    # -------------------------------------------------------------------------
    # Step 4: Temporal Aggregation (with Parallel Trend Analysis)
    # -------------------------------------------------------------------------
    print("\n" + "-" * 70)
    print("Step 4: Temporal Aggregation (with Parallel Trend Analysis)")
    print("-" * 70)
    
    # Create temporal aggregator
    aggregator = TemporalAggregator(data)
    
    # Monthly aggregation
    monthly = aggregator.to_monthly(agg_func='mean')
    print(f"\nMonthly aggregated records: {len(monthly):,}")
    
    # Annual aggregation
    annual = aggregator.to_annual(agg_func='mean')
    print(f"Annual aggregated records: {len(annual):,}")
    
    # Water year aggregation
    water_year = aggregator.to_annual(water_year=True, agg_func='mean')
    print(f"Water year aggregated records: {len(water_year):,}")
    
    # Calculate statistics
    stats = aggregator.calculate_statistics(groupby='site_no')
    print(f"\nStatistics calculated for {len(stats)} wells")
    print("\nTop 5 wells by record count:")
    print(stats.nlargest(5, 'count')[['count', 'mean', 'std', 'min', 'max']])
    
    # Calculate trends with parallel processing
    print("\nCalculating trends (parallel processing)...")
    try:
        trends = aggregator.get_trends(period='annual', parallel=True)
        print(f"Trends calculated for {len(trends)} wells")
        if not trends.empty:
            print("\nWells with significant trends (p < 0.05):")
            sig_trends = trends[trends['p_value'] < 0.05]
            if not sig_trends.empty:
                print(sig_trends[['site_no', 'slope', 'r_squared', 'trend_direction']].head(10))
            trends.to_csv(output_dir / "trends_analysis.csv", index=False)
    except Exception as e:
        print(f"Could not calculate trends: {e}")
    
    # Save aggregated data
    monthly.to_csv(output_dir / "monthly_aggregated.csv", index=False)
    annual.to_csv(output_dir / "annual_aggregated.csv", index=False)
    print(f"\n✓ Saved aggregated data to {output_dir}")
    
    # -------------------------------------------------------------------------
    # Step 5: Visualization
    # -------------------------------------------------------------------------
    print("\n" + "-" * 70)
    print("Step 5: Visualization")
    print("-" * 70)
    
    # Get zip codes with most data for visualization
    zipcode_counts = data.groupby('source_zipcode')['lev_dt'].count()
    top_zipcodes = zipcode_counts.nlargest(3).index.tolist()
    
    print(f"\nCreating visualizations for top zip codes: {top_zipcodes}")
    
    for zipcode in top_zipcodes:
        print(f"\n  Processing zip code: {zipcode}")
        
        # Filter data for this zip code
        zipcode_data = data[data['source_zipcode'] == zipcode].copy()
        
        if zipcode_data.empty or zipcode_data['site_no'].nunique() < 1:
            print(f"    Skipping - insufficient data")
            continue
        
        # Create plotter
        plotter = GroundwaterPlotter(zipcode_data)
        
        # Get the well with most records for detailed plots
        well_counts = zipcode_data.groupby('site_no')['lev_dt'].count()
        top_well = well_counts.idxmax()
        
        # --- Plot 1: Time Series for Top Well ---
        try:
            fig = plotter.plot_single_well(
                top_well,
                title=f"Groundwater Levels - Well {top_well}\n(Zip Code: {zipcode})"
            )
            if fig is not None:
                plot_file = plots_dir / f"timeseries_{zipcode}_{top_well}.png"
                fig.savefig(plot_file, dpi=300, bbox_inches='tight')
                plt.close(fig)
                print(f"    ✓ Saved: {plot_file.name}")
        except Exception as e:
            print(f"    Warning: Could not create time series plot: {e}")
        
        # --- Plot 2: Multi-well Time Series ---
        try:
            # Get top 5 wells for this zip code
            top_wells = well_counts.nlargest(5).index.tolist()
            fig = plotter.plot_time_series(
                site_numbers=top_wells,
                title=f"Groundwater Levels - Multiple Wells\n(Zip Code: {zipcode})"
            )
            if fig is not None:
                plot_file = plots_dir / f"multi_well_{zipcode}.png"
                fig.savefig(plot_file, dpi=300, bbox_inches='tight')
                plt.close(fig)
                print(f"    ✓ Saved: {plot_file.name}")
        except Exception as e:
            print(f"    Warning: Could not create multi-well plot: {e}")
        
        # --- Plot 3: Monthly Boxplot ---
        try:
            fig = plotter.plot_monthly_boxplot(
                site_no=top_well,
                title=f"Monthly Distribution - Well {top_well}\n(Zip Code: {zipcode})"
            )
            if fig is not None:
                plot_file = plots_dir / f"monthly_boxplot_{zipcode}_{top_well}.png"
                fig.savefig(plot_file, dpi=300, bbox_inches='tight')
                plt.close(fig)
                print(f"    ✓ Saved: {plot_file.name}")
        except Exception as e:
            print(f"    Warning: Could not create boxplot: {e}")
    
    # --- Plot 4: Overall Comparison Across Zip Codes ---
    print("\n  Creating cross-zip code comparison...")
    try:
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        
        # Plot 4a: Record count by zip code
        ax = axes[0, 0]
        zipcode_counts.plot(kind='bar', ax=ax, color='steelblue', edgecolor='black')
        ax.set_title('Records per Zip Code', fontsize=12, fontweight='bold')
        ax.set_xlabel('Zip Code')
        ax.set_ylabel('Number of Records')
        ax.tick_params(axis='x', rotation=45)
        
        # Plot 4b: Wells per zip code
        ax = axes[0, 1]
        wells_per_zip = data.groupby('source_zipcode')['site_no'].nunique()
        wells_per_zip.plot(kind='bar', ax=ax, color='forestgreen', edgecolor='black')
        ax.set_title('Wells per Zip Code', fontsize=12, fontweight='bold')
        ax.set_xlabel('Zip Code')
        ax.set_ylabel('Number of Wells')
        ax.tick_params(axis='x', rotation=45)
        
        # Plot 4c: Data timeline
        ax = axes[1, 0]
        if 'lev_dt' in data.columns:
            data['lev_dt'] = pd.to_datetime(data['lev_dt'], errors='coerce')
            yearly_counts = data.groupby(data['lev_dt'].dt.year)['lev_dt'].count()
            yearly_counts.plot(kind='line', ax=ax, color='coral', linewidth=2, marker='o', markersize=3)
            ax.set_title('Records Over Time (All Zip Codes)', fontsize=12, fontweight='bold')
            ax.set_xlabel('Year')
            ax.set_ylabel('Number of Records')
            ax.grid(True, alpha=0.3)
        
        # Plot 4d: Water level distribution by zip code
        ax = axes[1, 1]
        if 'lev_va' in data.columns:
            # Box plot of water levels by zip code
            data_for_box = data[data['lev_va'].notna()]
            if not data_for_box.empty:
                data_for_box.boxplot(column='lev_va', by='source_zipcode', ax=ax)
                ax.set_title('Water Level Distribution by Zip Code', fontsize=12, fontweight='bold')
                ax.set_xlabel('Zip Code')
                ax.set_ylabel('Water Level (ft below surface)')
                plt.suptitle('')  # Remove automatic title
                ax.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plot_file = plots_dir / "comparison_all_zipcodes.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"  ✓ Saved: {plot_file.name}")
        
    except Exception as e:
        print(f"  Warning: Could not create comparison plot: {e}")
    
    # --- Plot 5: Spatial Map of All Wells (Auto-Zoom) ---
    print("\n  Creating spatial map of all wells...")
    try:
        from pyGWRetrieval import plot_wells_map, create_comparison_map
        
        # Main spatial map with wells colored by mean water level
        # Auto-zoom adjusts based on the spatial extent:
        # - Local (<20 mi): zoom 12, detailed basemap
        # - Regional (<100 mi): zoom 10
        # - State (<500 mi): zoom 8
        # - Multi-state (<1500 mi): zoom 6
        # - National (>1500 mi): zoom 4-5
        fig = plot_wells_map(
            data,
            agg_func='mean',
            title='Groundwater Wells - Mean Water Level (ft below surface)',
            cmap='RdYlBu_r',  # Red=deep, Blue=shallow
            add_basemap=True,
            group_by_column='source_zipcode'  # Add annotations for each zip code
        )
        if fig is not None:
            plot_file = plots_dir / "spatial_map_all_wells.png"
            fig.savefig(plot_file, dpi=300, bbox_inches='tight')
            plt.close(fig)
            print(f"    ✓ Saved: {plot_file.name}")
        
        # Multi-panel comparison map (mean, count, min, max)
        fig = create_comparison_map(
            data,
            figsize=(18, 12),
            add_basemap=True
        )
        if fig is not None:
            plot_file = plots_dir / "spatial_comparison_map.png"
            fig.savefig(plot_file, dpi=300, bbox_inches='tight')
            plt.close(fig)
            print(f"    ✓ Saved: {plot_file.name}")
        
    except ImportError as e:
        print(f"  Note: Spatial mapping requires contextily. Install with: pip install contextily")
    except Exception as e:
        print(f"  Warning: Could not create spatial map: {e}")
    
    # -------------------------------------------------------------------------
    # Step 6: Summary Report
    # -------------------------------------------------------------------------
    print("\n" + "-" * 70)
    print("Step 6: Summary Report")
    print("-" * 70)
    
    print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║                        WORKFLOW COMPLETE                              ║
╠══════════════════════════════════════════════════════════════════════╣
║  Input:                                                               ║
║    - CSV File: {csv_file.name:<52} ║
║    - Zip Codes Processed: {len(unique_zipcodes):<45} ║
║    - Buffer Radius: {BUFFER_MILES} miles{' ' * 46}║
║    - Date Range: {START_DATE} to present{' ' * 33}║
║                                                                       ║
║  Results:                                                             ║
║    - Total Records: {len(data):,}{' ' * (47 - len(f'{len(data):,}'))}║
║    - Unique Wells: {data['site_no'].nunique():,}{' ' * (48 - len(f"{data['site_no'].nunique():,}"))}║
║    - Zip Codes with Data: {data['source_zipcode'].nunique():<41} ║
║                                                                       ║
║  Output Files:                                                        ║
║    - Combined Data: {str(combined_file.name):<47} ║
║    - Per-Zipcode Data: {str(data_per_zipcode_dir.name)}/gw_data_*.csv{' ' * 23}║
║    - Wells GeoJSON: groundwater_wells.geojson{' ' * 22}║
║    - Plots: {str(plots_dir.name)}/*.png{' ' * 45}║
╚══════════════════════════════════════════════════════════════════════╝
""")
    
    print(f"\nAll output saved to: {output_dir}")
    print("\nDone!")


if __name__ == "__main__":
    main()
