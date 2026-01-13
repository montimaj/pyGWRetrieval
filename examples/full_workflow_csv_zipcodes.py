#!/usr/bin/env python
"""
Example: Full Workflow - CSV Zip Codes to Groundwater Analysis

This script demonstrates a complete workflow for:
1. Reading zip codes from a CSV file (9 major US metro areas)
2. Downloading groundwater data within a 25-mile buffer of each zip code
3. Retrieving field measurement data from USGS (gwlevels)
4. Caching data to avoid redundant downloads
5. Temporal aggregation and trend analysis by region
6. Comprehensive visualization of regional groundwater patterns

Dataset: AirbnbMSACity_with_ZipCode.csv
Major Metro Areas (MSA): Chicago, San Francisco, Philadelphia, Dallas,
                         Washington DC, Houston, Boston, New York, Miami

USGS Data Sources:
------------------
- gwlevels: Field groundwater-level measurements (discrete manual readings)

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

Output Visualizations:
----------------------
1. regional_trends_by_msa.png - Regional trend analysis (4 panels):
   - Long-term water level trends per MSA
   - Records and wells count by region
   - Average water level depth by MSA
   - Trend slopes comparison

2. data_quality_analysis.png - Data quality metrics (4 panels):
   - Data density (records per well by region)
   - Distribution of annual measurements per well
   - Water level depth categories by region (%)
   - Historical vs recent water level change

3. regional_distributions.png - Regional distributions (4 panels):
   - Mean water levels with std dev by MSA
   - Violin plots of water level distribution
   - Annual record counts by MSA
   - Data availability timeline heatmap

4. regional_temporal_patterns.png - Temporal patterns (4 panels):
   - Mean annual water levels by MSA
   - Rolling 5-year mean trends
   - Inter-annual variability by MSA
   - Decadal change rates

5. monthly_boxplots_by_region.png - Monthly patterns for each MSA
6. annual_boxplots_by_region.png - Annual patterns for each MSA

7. regional_correlation_clustering.png - Correlation & clustering (4 panels):
   - Regional correlation heatmap
   - Hierarchical clustering dendrogram
   - Coefficient of variation (stability)
   - Seasonal amplitude comparison

8. extreme_events_analysis.png - Extreme events (4 panels):
   - Percentile exceedance (drought severity)
   - Record highs/lows timeline
   - Departure from long-term mean (anomaly)
   - Inter-annual variability

9. rolling_trend_analysis.png - Rate of change (4 panels):
   - Rolling 10-year trend slopes
   - Trend acceleration analysis
   - Year-over-year change distribution
   - Cumulative change since baseline

10. geographic_grouping_analysis.png - Geographic patterns (4 panels):
    - Coastal vs Inland comparison
    - East vs West Coast trends
    - Aquifer characteristics scatter
    - Normalized regional metrics

11. regional_summary_dashboard.png - Summary scorecard:
    - Sparklines for all regions
    - Mean depth, trend direction, data quality

Caching:
--------
Set FORCE_DOWNLOAD = False to use cached parquet files and skip re-downloading.
Set FORCE_DOWNLOAD = True to force fresh data retrieval from USGS.
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
    BUFFER_MILES = 25
    ZIPCODE_COLUMN = "ZipCode"
    FORCE_DOWNLOAD = False  # Set to True to re-download even if data exists
    
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
    print(f"  Force Download: {FORCE_DOWNLOAD}")
    
    # Check for cached data
    combined_file = output_dir / "all_groundwater_data.parquet"
    
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
    
    # Initialize the retrieval object with data sources
    gw = GroundwaterRetrieval(
        start_date=START_DATE,
        end_date=END_DATE,
        data_sources=DATA_SOURCES  # Specify which USGS data sources to query
    )
    
    # Check if cached data exists
    if combined_file.exists() and not FORCE_DOWNLOAD:
        print(f"\n✓ Loading cached data from: {combined_file}")
        data = pd.read_parquet(combined_file)
        print(f"  Loaded {len(data):,} records from cache")
        print(f"  Unique wells: {data['site_no'].nunique():,}")
        print(f"  Zip codes with data: {data['source_zipcode'].nunique()}")
        print("\n  (Set FORCE_DOWNLOAD = True to re-download from USGS)")
    else:
        # Check parallel processing availability
        from pyGWRetrieval import check_dask_available, get_parallel_config
        
        parallel_available = check_dask_available()
        print(f"\nParallel processing available: {parallel_available}")
        if parallel_available:
            config = get_parallel_config()
            print(f"Dask version: {config.get('dask_version', 'N/A')}")
            print(f"Available workers: {config.get('num_workers', 'auto')}")
        
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
    
    # Save combined data (only if newly downloaded)
    if not combined_file.exists() or FORCE_DOWNLOAD:
        gw.to_parquet(combined_file, data)
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
    else:
        print("\n✓ Using cached data (skipping save)")
    
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
    
    # -------------------------------------------------------------------------
    # Regional Trend Analysis by MSA (Metropolitan Statistical Area)
    # -------------------------------------------------------------------------
    print("\n" + "." * 50)
    print("Regional Trend Analysis by Metro Area (MSA)")
    print("." * 50)
    
    # Merge MSA information from CSV into data
    zipcode_to_msa = csv_data.set_index(ZIPCODE_COLUMN)['MSA'].to_dict()
    # Correct misspellings in MSA names from CSV
    msa_corrections = {'Philadhepia': 'Philadelphia'}
    data['MSA'] = data['source_zipcode'].map(
        lambda z: msa_corrections.get(
            zipcode_to_msa.get(int(z) if str(z).isdigit() else z, 'Unknown'),
            zipcode_to_msa.get(int(z) if str(z).isdigit() else z, 'Unknown')
        )
    )
    
    # Get unique MSA regions in data
    msa_list = data['MSA'].dropna().unique()
    print(f"\nAnalyzing trends for {len(msa_list)} metro regions: {list(msa_list)}")
    
    regional_trends = []
    for msa in msa_list:
        msa_data = data[data['MSA'] == msa].copy()
        if len(msa_data) < 10:
            continue
        
        try:
            msa_aggregator = TemporalAggregator(msa_data)
            msa_annual = msa_aggregator.to_annual(agg_func='mean')
            
            # Calculate regional statistics
            n_wells = msa_data['site_no'].nunique()
            n_records = len(msa_data)
            mean_level = msa_data['lev_va'].mean()
            
            # Get trend for the region (aggregate all wells)
            if len(msa_annual) >= 3:
                msa_trends = msa_aggregator.get_trends(period='annual', parallel=False)
                if not msa_trends.empty:
                    # Summarize regional trend
                    # Note: trend_direction uses 'increasing'/'decreasing' 
                    # 'increasing' slope = water level deepening (falling water table)
                    # 'decreasing' slope = water level rising (rising water table)
                    avg_slope = msa_trends['slope'].mean()
                    
                    # Count by trend direction (increasing = falling, decreasing = rising)
                    if 'trend_direction' in msa_trends.columns:
                        falling = (msa_trends['trend_direction'] == 'increasing').sum()
                        rising = (msa_trends['trend_direction'] == 'decreasing').sum()
                    else:
                        # Fallback: use slope to determine direction
                        falling = (msa_trends['slope'] > 0.01).sum()
                        rising = (msa_trends['slope'] < -0.01).sum()
                    
                    # Stable = not clearly rising or falling
                    stable = len(msa_trends) - rising - falling
                    sig_count = (msa_trends['p_value'] < 0.05).sum() if 'p_value' in msa_trends.columns else 0
                    
                    regional_trends.append({
                        'MSA': msa,
                        'n_wells': n_wells,
                        'n_records': n_records,
                        'mean_water_level_ft': round(mean_level, 2) if pd.notna(mean_level) else None,
                        'avg_slope_ft_per_year': round(avg_slope, 4) if pd.notna(avg_slope) else None,
                        'wells_rising': int(rising),
                        'wells_falling': int(falling),
                        'wells_stable': int(stable),
                        'wells_significant_trend': int(sig_count),
                        'dominant_trend': 'rising' if rising > falling else ('falling' if falling > rising else 'stable')
                    })
        except Exception as e:
            print(f"  Warning: Could not analyze {msa}: {e}")
    
    if regional_trends:
        regional_df = pd.DataFrame(regional_trends)
        regional_df = regional_df.sort_values('n_records', ascending=False)
        
        print("\n" + "=" * 80)
        print("REGIONAL GROUNDWATER TREND SUMMARY")
        print("=" * 80)
        print(f"\n{'MSA':<20} {'Wells':>8} {'Records':>10} {'Avg Level':>12} {'Slope':>12} {'Trend':>12}")
        print("-" * 80)
        for _, row in regional_df.iterrows():
            level_str = f"{row['mean_water_level_ft']:.1f} ft" if pd.notna(row['mean_water_level_ft']) else "N/A"
            slope_str = f"{row['avg_slope_ft_per_year']:.4f}" if pd.notna(row['avg_slope_ft_per_year']) else "N/A"
            print(f"{row['MSA']:<20} {row['n_wells']:>8} {row['n_records']:>10} {level_str:>12} {slope_str:>12} {row['dominant_trend']:>12}")
        print("-" * 80)
        
        # Interpretation
        print("\nInterpretation:")
        print("  - Slope: ft/year change in water level (positive = deepening, negative = rising)")
        print("  - Rising: Water table getting shallower (recharge > withdrawal)")
        print("  - Falling: Water table getting deeper (withdrawal > recharge)")
        
        regional_df.to_csv(output_dir / "regional_trends_by_msa.csv", index=False)
        print(f"\n✓ Saved regional trends to: {output_dir / 'regional_trends_by_msa.csv'}")
        
        # --- Regional Trends Visualization ---
        print("\n  Creating regional trends visualization...")
        try:
            fig, axes = plt.subplots(2, 2, figsize=(16, 12))
            
            # Sort for consistent plotting
            plot_df = regional_df.sort_values('MSA')
            
            # Plot 1: Average slope by region (bar chart)
            ax = axes[0, 0]
            colors = ['#d73027' if s > 0 else '#4575b4' if s < 0 else '#999999' 
                      for s in plot_df['avg_slope_ft_per_year'].fillna(0)]
            x_pos = range(len(plot_df))
            bars = ax.bar(x_pos, plot_df['avg_slope_ft_per_year'].fillna(0), 
                         color=colors, edgecolor='black')
            ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
            ax.set_title('Average Groundwater Trend by Metro Region', fontsize=12, fontweight='bold')
            ax.set_xlabel('Metro Region (MSA)')
            ax.set_ylabel('Slope (ft/year)')
            ax.set_xticks(x_pos)
            ax.set_xticklabels(plot_df['MSA'], rotation=45, ha='right')
            # Add legend
            from matplotlib.patches import Patch
            legend_elements = [Patch(facecolor='#d73027', label='Deepening (falling water table)'),
                              Patch(facecolor='#4575b4', label='Rising (rising water table)')]
            ax.legend(handles=legend_elements, loc='best', fontsize=9)
            
            # Plot 2: Mean water level by region
            ax = axes[0, 1]
            ax.barh(plot_df['MSA'], plot_df['mean_water_level_ft'].fillna(0), 
                   color='steelblue', edgecolor='black')
            ax.set_title('Mean Water Level by Metro Region', fontsize=12, fontweight='bold')
            ax.set_xlabel('Mean Water Level (ft below surface)')
            ax.set_ylabel('Metro Region (MSA)')
            ax.invert_xaxis()  # Deeper on right
            
            # Plot 3: Stacked bar - trend direction counts
            ax = axes[1, 0]
            bar_width = 0.6
            x = range(len(plot_df))
            ax.bar(x, plot_df['wells_rising'], bar_width, label='Rising', color='#4575b4')
            ax.bar(x, plot_df['wells_stable'], bar_width, bottom=plot_df['wells_rising'], 
                   label='Stable', color='#999999')
            ax.bar(x, plot_df['wells_falling'], bar_width, 
                   bottom=plot_df['wells_rising'] + plot_df['wells_stable'], 
                   label='Falling', color='#d73027')
            ax.set_title('Wells by Trend Direction per Region', fontsize=12, fontweight='bold')
            ax.set_xlabel('Metro Region (MSA)')
            ax.set_ylabel('Number of Wells')
            ax.set_xticks(x)
            ax.set_xticklabels(plot_df['MSA'], rotation=45, ha='right')
            ax.legend(loc='upper right')
            
            # Plot 4: Number of records by region (bubble/scatter size = wells)
            ax = axes[1, 1]
            scatter = ax.scatter(plot_df['n_records'], plot_df['avg_slope_ft_per_year'].fillna(0),
                                s=plot_df['n_wells'] * 5,  # Size based on wells
                                c=plot_df['avg_slope_ft_per_year'].fillna(0),
                                cmap='RdYlBu_r', edgecolors='black', alpha=0.7)
            ax.axhline(y=0, color='black', linestyle='--', linewidth=0.5, alpha=0.5)
            for i, row in plot_df.iterrows():
                ax.annotate(row['MSA'], (row['n_records'], row['avg_slope_ft_per_year'] or 0),
                           fontsize=8, ha='center', va='bottom')
            ax.set_title('Records vs Trend (bubble size = wells)', fontsize=12, fontweight='bold')
            ax.set_xlabel('Number of Records')
            ax.set_ylabel('Slope (ft/year)')
            plt.colorbar(scatter, ax=ax, label='Slope')
            
            plt.tight_layout()
            plot_file = plots_dir / "regional_trends_by_msa.png"
            fig.savefig(plot_file, dpi=300, bbox_inches='tight')
            plt.close(fig)
            print(f"  ✓ Saved: {plot_file.name}")
            
        except Exception as e:
            print(f"  Warning: Could not create regional trends plot: {e}")
    
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
    
    # --- Plot 4: Data Quality and Coverage Analysis ---
    print("\n  Creating data quality and coverage analysis...")
    try:
        # Ensure datetime and compute metrics
        data['lev_dt'] = pd.to_datetime(data['lev_dt'], format='mixed', errors='coerce')
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # Plot 4a: Data completeness by MSA (% of years with data)
        ax = axes[0, 0]
        if 'MSA' not in data.columns:
            zipcode_to_msa = csv_data.set_index(ZIPCODE_COLUMN)['MSA'].to_dict()
            data['MSA'] = data['source_zipcode'].map(lambda z: zipcode_to_msa.get(int(z) if str(z).isdigit() else z, 'Unknown'))
        
        msa_coverage = data.groupby('MSA').agg({
            'lev_dt': ['min', 'max', 'count'],
            'site_no': 'nunique'
        })
        msa_coverage.columns = ['start', 'end', 'records', 'wells']
        msa_coverage['years_span'] = (msa_coverage['end'] - msa_coverage['start']).dt.days / 365
        msa_coverage['records_per_well'] = msa_coverage['records'] / msa_coverage['wells']
        msa_coverage = msa_coverage.sort_values('records', ascending=True)
        
        colors = plt.cm.viridis(msa_coverage['records_per_well'] / msa_coverage['records_per_well'].max())
        ax.barh(msa_coverage.index, msa_coverage['records_per_well'], color=colors, edgecolor='black')
        ax.set_title('Data Density: Records per Well by Region', fontsize=12, fontweight='bold')
        ax.set_xlabel('Average Records per Well')
        ax.set_ylabel('Metro Region (MSA)')
        ax.grid(True, alpha=0.3, axis='x')
        
        # Plot 4b: Measurement frequency distribution
        ax = axes[0, 1]
        # Calculate measurements per well per year
        data['year'] = data['lev_dt'].dt.year
        well_yearly = data.groupby(['site_no', 'year']).size().reset_index(name='measurements')
        
        ax.hist(well_yearly['measurements'], bins=50, color='steelblue', edgecolor='black', alpha=0.7)
        ax.axvline(well_yearly['measurements'].median(), color='red', linestyle='--', linewidth=2, 
                   label=f'Median: {well_yearly["measurements"].median():.0f}')
        ax.axvline(well_yearly['measurements'].mean(), color='orange', linestyle='--', linewidth=2,
                   label=f'Mean: {well_yearly["measurements"].mean():.1f}')
        ax.set_title('Distribution of Annual Measurements per Well', fontsize=12, fontweight='bold')
        ax.set_xlabel('Measurements per Well per Year')
        ax.set_ylabel('Frequency')
        ax.legend()
        ax.set_xlim(0, min(100, well_yearly['measurements'].quantile(0.99)))
        
        # Plot 4c: Water level depth categories by region
        ax = axes[1, 0]
        # Categorize water levels
        def categorize_depth(val):
            if pd.isna(val):
                return 'Unknown'
            elif val < 10:
                return 'Shallow (<10 ft)'
            elif val < 50:
                return 'Moderate (10-50 ft)'
            elif val < 100:
                return 'Deep (50-100 ft)'
            else:
                return 'Very Deep (>100 ft)'
        
        data['depth_category'] = data['lev_va'].apply(categorize_depth)
        depth_by_msa = data[data['MSA'] != 'Unknown'].groupby(['MSA', 'depth_category']).size().unstack(fill_value=0)
        
        # Normalize to percentages
        depth_pct = depth_by_msa.div(depth_by_msa.sum(axis=1), axis=0) * 100
        categories_order = ['Shallow (<10 ft)', 'Moderate (10-50 ft)', 'Deep (50-100 ft)', 'Very Deep (>100 ft)']
        categories_present = [c for c in categories_order if c in depth_pct.columns]
        depth_pct = depth_pct[categories_present]
        
        depth_pct.plot(kind='barh', stacked=True, ax=ax, 
                      color=['#2166ac', '#67a9cf', '#ef8a62', '#b2182b'][:len(categories_present)],
                      edgecolor='black', linewidth=0.5)
        ax.set_title('Water Level Depth Categories by Region (%)', fontsize=12, fontweight='bold')
        ax.set_xlabel('Percentage of Measurements')
        ax.set_ylabel('Metro Region (MSA)')
        ax.legend(title='Depth Category', bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=9)
        ax.set_xlim(0, 100)
        
        # Plot 4d: Long-term vs recent data comparison
        ax = axes[1, 1]
        # Compare data from before 2000 vs after 2010
        historical = data[data['year'] < 2000].groupby('MSA')['lev_va'].mean()
        recent = data[data['year'] >= 2010].groupby('MSA')['lev_va'].mean()
        
        comparison = pd.DataFrame({'Historical (<2000)': historical, 'Recent (≥2010)': recent})
        comparison = comparison.dropna()
        comparison['change'] = comparison['Recent (≥2010)'] - comparison['Historical (<2000)']
        comparison = comparison.sort_values('change')
        
        colors = ['#d73027' if c > 0 else '#4575b4' for c in comparison['change']]
        ax.barh(comparison.index, comparison['change'], color=colors, edgecolor='black')
        ax.axvline(0, color='black', linewidth=1)
        ax.set_title('Change in Mean Water Level: Historical vs Recent', fontsize=12, fontweight='bold')
        ax.set_xlabel('Change in Water Level (ft)\n(+ = Deepening, - = Rising)')
        ax.set_ylabel('Metro Region (MSA)')
        ax.grid(True, alpha=0.3, axis='x')
        
        # Add legend
        from matplotlib.patches import Patch
        legend_elements = [Patch(facecolor='#d73027', label='Falling (water table deepening)'),
                         Patch(facecolor='#4575b4', label='Rising (water table rising)')]
        ax.legend(handles=legend_elements, loc='lower right', fontsize=9)
        
        plt.tight_layout()
        plot_file = plots_dir / "data_quality_analysis.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"  ✓ Saved: {plot_file.name}")
        
    except Exception as e:
        print(f"  Warning: Could not create data quality plot: {e}")
        import traceback
        traceback.print_exc()
    
    # --- Plot 5: Regional Distributions Across 9 Major Metro Areas ---
    print("\n  Creating regional distribution visualizations...")
    try:
        # Ensure MSA column exists
        if 'MSA' not in data.columns:
            zipcode_to_msa = csv_data.set_index(ZIPCODE_COLUMN)['MSA'].to_dict()
            data['MSA'] = data['source_zipcode'].map(lambda z: zipcode_to_msa.get(int(z) if str(z).isdigit() else z, 'Unknown'))
        
        # Filter to known MSAs
        msa_data = data[data['MSA'] != 'Unknown'].copy()
        msa_order = msa_data.groupby('MSA')['lev_va'].count().sort_values(ascending=False).index.tolist()
        
        # Figure 5a: Water Level Distributions by Region (Violin/Box plots)
        fig, axes = plt.subplots(2, 2, figsize=(16, 14))
        
        # Plot 5a: Water level distribution by MSA (violin plot)
        ax = axes[0, 0]
        msa_levels = [msa_data[msa_data['MSA'] == msa]['lev_va'].dropna().values for msa in msa_order]
        parts = ax.violinplot(msa_levels, positions=range(len(msa_order)), showmeans=True, showmedians=True)
        ax.set_xticks(range(len(msa_order)))
        ax.set_xticklabels(msa_order, rotation=45, ha='right')
        ax.set_title('Water Level Distribution by Metro Region', fontsize=12, fontweight='bold')
        ax.set_xlabel('Metro Region (MSA)')
        ax.set_ylabel('Water Level (ft below surface)')
        ax.grid(True, alpha=0.3, axis='y')
        
        # Plot 5b: Number of wells and records by region
        ax = axes[0, 1]
        msa_stats = msa_data.groupby('MSA').agg({
            'site_no': 'nunique',
            'lev_va': 'count'
        }).rename(columns={'site_no': 'wells', 'lev_va': 'records'})
        msa_stats = msa_stats.loc[msa_order]
        
        x = range(len(msa_stats))
        width = 0.35
        bars1 = ax.bar([i - width/2 for i in x], msa_stats['wells'], width, label='Wells', color='steelblue')
        ax2 = ax.twinx()
        bars2 = ax2.bar([i + width/2 for i in x], msa_stats['records'], width, label='Records', color='coral', alpha=0.7)
        
        ax.set_xticks(x)
        ax.set_xticklabels(msa_order, rotation=45, ha='right')
        ax.set_title('Wells and Records by Metro Region', fontsize=12, fontweight='bold')
        ax.set_xlabel('Metro Region (MSA)')
        ax.set_ylabel('Number of Wells', color='steelblue')
        ax2.set_ylabel('Number of Records', color='coral')
        ax.legend(loc='upper left')
        ax2.legend(loc='upper right')
        
        # Plot 5c: Temporal coverage by region (data span)
        ax = axes[1, 0]
        msa_data['lev_dt'] = pd.to_datetime(msa_data['lev_dt'], format='mixed', errors='coerce')
        temporal_stats = msa_data.groupby('MSA').agg({
            'lev_dt': ['min', 'max']
        })
        temporal_stats.columns = ['start_year', 'end_year']
        temporal_stats = temporal_stats.loc[msa_order]
        
        for i, msa in enumerate(msa_order):
            start = temporal_stats.loc[msa, 'start_year']
            end = temporal_stats.loc[msa, 'end_year']
            if pd.notna(start) and pd.notna(end):
                bar_width = (end - start).days / 365
                ax.barh(i, bar_width, left=start.year, color='forestgreen', alpha=0.7, edgecolor='black')
                # Place year labels inside the bar at each end
                ax.text(start.year + 1, i, f"{start.year}", va='center', ha='left', fontsize=8, color='white', fontweight='bold')
                ax.text(end.year - 1, i, f"{end.year}", va='center', ha='right', fontsize=8, color='white', fontweight='bold')
        
        ax.set_yticks(range(len(msa_order)))
        ax.set_yticklabels(msa_order)
        ax.set_title('Data Temporal Coverage by Metro Region', fontsize=12, fontweight='bold')
        ax.set_xlabel('Year')
        ax.set_ylabel('Metro Region (MSA)')
        ax.grid(True, alpha=0.3, axis='x')
        
        # Plot 5d: Mean and variability by region
        ax = axes[1, 1]
        msa_summary = msa_data.groupby('MSA')['lev_va'].agg(['mean', 'std', 'median']).loc[msa_order]
        
        x = range(len(msa_summary))
        ax.bar(x, msa_summary['mean'], yerr=msa_summary['std'], 
               capsize=5, color='steelblue', edgecolor='black', alpha=0.7, label='Mean ± Std')
        ax.scatter(x, msa_summary['median'], color='red', s=50, zorder=5, label='Median', marker='D')
        
        ax.set_xticks(x)
        ax.set_xticklabels(msa_order, rotation=45, ha='right')
        ax.set_title('Mean Water Level with Variability by Region', fontsize=12, fontweight='bold')
        ax.set_xlabel('Metro Region (MSA)')
        ax.set_ylabel('Water Level (ft below surface)')
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plot_file = plots_dir / "regional_distributions.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"  ✓ Saved: {plot_file.name}")
        
        # Figure 5b: Decadal trends by region
        fig, axes = plt.subplots(2, 2, figsize=(16, 14))
        
        # Add decade column
        msa_data['decade'] = (msa_data['lev_dt'].dt.year // 10) * 10
        decades = sorted(msa_data['decade'].dropna().unique())
        decades = [d for d in decades if d >= 1950]  # Filter old data
        
        # Plot: Mean water level by decade for each region
        ax = axes[0, 0]
        decade_means = msa_data.groupby(['MSA', 'decade'])['lev_va'].mean().unstack(level=0)
        decade_means = decade_means[msa_order]
        decade_means.plot(ax=ax, marker='o', linewidth=2)
        ax.set_title('Mean Water Level by Decade and Region', fontsize=12, fontweight='bold')
        ax.set_xlabel('Decade')
        ax.set_ylabel('Mean Water Level (ft below surface)')
        ax.legend(title='MSA', bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3)
        
        # Plot: Record count by decade
        ax = axes[0, 1]
        decade_counts = msa_data.groupby(['MSA', 'decade']).size().unstack(level=0)
        decade_counts = decade_counts[msa_order]
        decade_counts.plot(kind='bar', ax=ax, stacked=True, width=0.8)
        ax.set_title('Records by Decade and Region', fontsize=12, fontweight='bold')
        ax.set_xlabel('Decade')
        ax.set_ylabel('Number of Records')
        ax.legend(title='MSA', bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=8)
        ax.tick_params(axis='x', rotation=45)
        
        # Plot: Seasonal patterns by region
        ax = axes[1, 0]
        msa_data['month'] = msa_data['lev_dt'].dt.month
        monthly_means = msa_data.groupby(['MSA', 'month'])['lev_va'].mean().unstack(level=0)
        monthly_means = monthly_means[msa_order]
        month_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        for msa in msa_order[:5]:  # Top 5 regions for clarity
            if msa in monthly_means.columns:
                ax.plot(range(1, 13), monthly_means[msa], marker='o', label=msa, linewidth=2)
        ax.set_xticks(range(1, 13))
        ax.set_xticklabels(month_labels)
        ax.set_title('Seasonal Water Level Patterns (Top 5 Regions)', fontsize=12, fontweight='bold')
        ax.set_xlabel('Month')
        ax.set_ylabel('Mean Water Level (ft below surface)')
        ax.legend(title='MSA', fontsize=9)
        ax.grid(True, alpha=0.3)
        
        # Plot: Data density heatmap (years vs regions)
        ax = axes[1, 1]
        msa_data['year'] = msa_data['lev_dt'].dt.year
        year_counts = msa_data.groupby(['MSA', 'year']).size().unstack(level=0, fill_value=0)
        year_counts = year_counts[msa_order]
        # Sample to recent decades for clarity
        recent_years = year_counts[year_counts.index >= 1990]
        
        im = ax.imshow(recent_years.T.values, aspect='auto', cmap='YlOrRd', interpolation='nearest')
        ax.set_yticks(range(len(msa_order)))
        ax.set_yticklabels(msa_order)
        ax.set_xlabel('Year')
        ax.set_ylabel('Metro Region (MSA)')
        ax.set_title('Data Density Heatmap (1990-Present)', fontsize=12, fontweight='bold')
        
        # Set x-axis labels for years
        year_indices = list(range(0, len(recent_years), max(1, len(recent_years)//10)))
        ax.set_xticks(year_indices)
        ax.set_xticklabels([recent_years.index[i] for i in year_indices], rotation=45)
        
        plt.colorbar(im, ax=ax, label='Record Count')
        
        plt.tight_layout()
        plot_file = plots_dir / "regional_temporal_patterns.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"  ✓ Saved: {plot_file.name}")
        
        # Figure 5c: Monthly boxplots for each major region
        print("  Creating monthly boxplots by region...")
        n_regions = len(msa_order)
        n_cols = 3
        n_rows = (n_regions + n_cols - 1) // n_cols
        fig, axes = plt.subplots(n_rows, n_cols, figsize=(18, 4 * n_rows))
        axes = axes.flatten()
        
        month_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        for i, msa in enumerate(msa_order):
            ax = axes[i]
            region_data = msa_data[msa_data['MSA'] == msa].copy()
            
            if region_data.empty:
                ax.set_visible(False)
                continue
            
            # Create monthly boxplot
            monthly_data = [region_data[region_data['month'] == m]['lev_va'].dropna().values 
                           for m in range(1, 13)]
            
            # Filter empty months
            valid_months = [(m, d) for m, d in zip(month_labels, monthly_data) if len(d) > 0]
            if valid_months:
                bp = ax.boxplot([d for _, d in valid_months], 
                               labels=[m for m, _ in valid_months],
                               patch_artist=True)
                for patch in bp['boxes']:
                    patch.set_facecolor('steelblue')
                    patch.set_alpha(0.7)
            
            ax.set_title(f'{msa}', fontsize=11, fontweight='bold')
            ax.set_xlabel('Month')
            ax.set_ylabel('Water Level (ft)')
            ax.tick_params(axis='x', rotation=45)
            ax.grid(True, alpha=0.3, axis='y')
        
        # Hide unused subplots
        for i in range(len(msa_order), len(axes)):
            axes[i].set_visible(False)
        
        fig.suptitle('Monthly Water Level Distribution by Metro Region', fontsize=14, fontweight='bold', y=1.02)
        plt.tight_layout()
        plot_file = plots_dir / "monthly_boxplots_by_region.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"  ✓ Saved: {plot_file.name}")
        
        # Figure 5d: Annual boxplots for each major region
        print("  Creating annual boxplots by region...")
        fig, axes = plt.subplots(n_rows, n_cols, figsize=(18, 4 * n_rows))
        axes = axes.flatten()
        
        for i, msa in enumerate(msa_order):
            ax = axes[i]
            region_data = msa_data[msa_data['MSA'] == msa].copy()
            
            if region_data.empty:
                ax.set_visible(False)
                continue
            
            # Get years with data (focus on recent decades)
            region_data = region_data[region_data['year'] >= 1980]
            years = sorted(region_data['year'].dropna().unique())
            
            if len(years) == 0:
                ax.set_visible(False)
                continue
            
            # Sample years for readability (every 5 years or so)
            if len(years) > 15:
                step = max(1, len(years) // 12)
                sample_years = years[::step]
            else:
                sample_years = years
            
            # Create annual boxplot
            annual_data = [region_data[region_data['year'] == y]['lev_va'].dropna().values 
                          for y in sample_years]
            
            # Filter empty years
            valid_years = [(y, d) for y, d in zip(sample_years, annual_data) if len(d) > 0]
            if valid_years:
                bp = ax.boxplot([d for _, d in valid_years], 
                               labels=[int(y) for y, _ in valid_years],
                               patch_artist=True)
                for patch in bp['boxes']:
                    patch.set_facecolor('coral')
                    patch.set_alpha(0.7)
            
            ax.set_title(f'{msa}', fontsize=11, fontweight='bold')
            ax.set_xlabel('Year')
            ax.set_ylabel('Water Level (ft)')
            ax.tick_params(axis='x', rotation=45)
            ax.grid(True, alpha=0.3, axis='y')
        
        # Hide unused subplots
        for i in range(len(msa_order), len(axes)):
            axes[i].set_visible(False)
        
        fig.suptitle('Annual Water Level Distribution by Metro Region (1980-Present)', fontsize=14, fontweight='bold', y=1.02)
        plt.tight_layout()
        plot_file = plots_dir / "annual_boxplots_by_region.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"  ✓ Saved: {plot_file.name}")
        
    except Exception as e:
        print(f"  Warning: Could not create regional distribution plots: {e}")
        import traceback
        traceback.print_exc()
    
    # --- Plot 7: Regional Correlation & Clustering Analysis ---
    print("\n  Creating regional correlation and clustering analysis...")
    try:
        import numpy as np
        from scipy import stats as scipy_stats
        from scipy.cluster import hierarchy
        from scipy.spatial.distance import pdist
        
        # Prepare annual mean data by region for correlation
        msa_data['year'] = msa_data['lev_dt'].dt.year
        annual_by_msa = msa_data.groupby(['year', 'MSA'])['lev_va'].mean().unstack()
        
        # Filter to years with data for most regions
        min_regions = max(3, len(msa_order) // 2)
        annual_by_msa = annual_by_msa.dropna(thresh=min_regions)
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 14))
        
        # Plot 7a: Correlation heatmap between regions
        ax = axes[0, 0]
        corr_matrix = annual_by_msa.corr()
        
        im = ax.imshow(corr_matrix.values, cmap='RdBu_r', vmin=-1, vmax=1, aspect='auto')
        ax.set_xticks(range(len(corr_matrix.columns)))
        ax.set_yticks(range(len(corr_matrix.index)))
        ax.set_xticklabels(corr_matrix.columns, rotation=45, ha='right', fontsize=9)
        ax.set_yticklabels(corr_matrix.index, fontsize=9)
        ax.set_title('Regional Correlation Matrix\n(Annual Mean Water Levels)', fontsize=12, fontweight='bold')
        
        # Add correlation values as text
        for i in range(len(corr_matrix)):
            for j in range(len(corr_matrix)):
                val = corr_matrix.iloc[i, j]
                color = 'white' if abs(val) > 0.5 else 'black'
                ax.text(j, i, f'{val:.2f}', ha='center', va='center', color=color, fontsize=8)
        
        plt.colorbar(im, ax=ax, label='Correlation Coefficient')
        
        # Plot 7b: Dendrogram - hierarchical clustering of regions
        ax = axes[0, 1]
        # Use correlation distance for clustering
        corr_dist = 1 - corr_matrix.values
        np.fill_diagonal(corr_dist, 0)
        
        # Ensure symmetric and valid distance matrix
        corr_dist = (corr_dist + corr_dist.T) / 2
        corr_dist = np.clip(corr_dist, 0, 2)
        
        # Convert to condensed form
        condensed = pdist(corr_matrix.values, metric='correlation')
        condensed = np.nan_to_num(condensed, nan=1.0)
        
        linkage = hierarchy.linkage(condensed, method='ward')
        hierarchy.dendrogram(linkage, labels=list(corr_matrix.columns), ax=ax, 
                            leaf_rotation=45, leaf_font_size=9)
        ax.set_title('Regional Clustering Dendrogram\n(by Water Level Pattern Similarity)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Distance (1 - Correlation)')
        
        # Plot 7c: Coefficient of Variation by region (stability measure)
        ax = axes[1, 0]
        cv_by_msa = msa_data.groupby('MSA')['lev_va'].agg(['mean', 'std'])
        cv_by_msa['cv'] = (cv_by_msa['std'] / cv_by_msa['mean']) * 100
        cv_by_msa = cv_by_msa.sort_values('cv')
        
        colors = plt.cm.RdYlGn_r(cv_by_msa['cv'] / cv_by_msa['cv'].max())
        ax.barh(cv_by_msa.index, cv_by_msa['cv'], color=colors, edgecolor='black')
        ax.set_title('Water Level Variability by Region\n(Coefficient of Variation)', fontsize=12, fontweight='bold')
        ax.set_xlabel('Coefficient of Variation (%)\n(Lower = More Stable)')
        ax.set_ylabel('Metro Region (MSA)')
        ax.grid(True, alpha=0.3, axis='x')
        
        # Add stability labels
        ax.axvline(cv_by_msa['cv'].median(), color='red', linestyle='--', linewidth=2, 
                   label=f'Median: {cv_by_msa["cv"].median():.1f}%')
        ax.legend(loc='lower right')
        
        # Plot 7d: Seasonal amplitude comparison
        ax = axes[1, 1]
        monthly_stats = msa_data.groupby(['MSA', 'month'])['lev_va'].mean().unstack()
        seasonal_amplitude = monthly_stats.max(axis=1) - monthly_stats.min(axis=1)
        seasonal_amplitude = seasonal_amplitude.sort_values()
        
        colors = plt.cm.coolwarm(seasonal_amplitude / seasonal_amplitude.max())
        ax.barh(seasonal_amplitude.index, seasonal_amplitude.values, color=colors, edgecolor='black')
        ax.set_title('Seasonal Amplitude by Region\n(Max - Min Monthly Mean)', fontsize=12, fontweight='bold')
        ax.set_xlabel('Seasonal Amplitude (ft)')
        ax.set_ylabel('Metro Region (MSA)')
        ax.grid(True, alpha=0.3, axis='x')
        
        plt.tight_layout()
        plot_file = plots_dir / "regional_correlation_clustering.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"  ✓ Saved: {plot_file.name}")
        
    except Exception as e:
        print(f"  Warning: Could not create correlation/clustering plot: {e}")
        import traceback
        traceback.print_exc()
    
    # --- Plot 8: Extreme Events & Anomaly Analysis ---
    print("\n  Creating extreme events and anomaly analysis...")
    try:
        fig, axes = plt.subplots(2, 2, figsize=(16, 14))
        
        # Plot 8a: Percentile exceedance (drought severity proxy)
        ax = axes[0, 0]
        percentile_stats = []
        for msa in msa_order:
            region_data = msa_data[msa_data['MSA'] == msa]['lev_va'].dropna()
            if len(region_data) > 10:
                p90 = np.percentile(region_data, 90)
                p95 = np.percentile(region_data, 95)
                pct_above_90 = (region_data > p90).sum() / len(region_data) * 100
                pct_above_95 = (region_data > p95).sum() / len(region_data) * 100
                percentile_stats.append({
                    'MSA': msa,
                    'pct_above_90th': pct_above_90,
                    'pct_above_95th': pct_above_95,
                    'p90_value': p90,
                    'p95_value': p95
                })
        
        pct_df = pd.DataFrame(percentile_stats).set_index('MSA')
        x = range(len(pct_df))
        width = 0.35
        ax.bar([i - width/2 for i in x], pct_df['pct_above_90th'], width, label='Above 90th %ile', color='#fdae61')
        ax.bar([i + width/2 for i in x], pct_df['pct_above_95th'], width, label='Above 95th %ile', color='#d73027')
        ax.set_xticks(x)
        ax.set_xticklabels(pct_df.index, rotation=45, ha='right')
        ax.set_title('Extreme Deep Water Levels by Region\n(% of Readings Above Percentiles)', fontsize=12, fontweight='bold')
        ax.set_xlabel('Metro Region (MSA)')
        ax.set_ylabel('Percentage of Readings (%)')
        ax.legend()
        ax.axhline(10, color='gray', linestyle='--', alpha=0.5, label='Expected 10%')
        ax.grid(True, alpha=0.3, axis='y')
        
        # Plot 8b: Record highs/lows timeline
        ax = axes[0, 1]
        for i, msa in enumerate(msa_order[:6]):  # Top 6 regions
            region_data = msa_data[msa_data['MSA'] == msa].copy()
            if region_data.empty:
                continue
            
            yearly_max = region_data.groupby('year')['lev_va'].max()
            yearly_min = region_data.groupby('year')['lev_va'].min()
            
            # Plot range as filled area
            years = yearly_max.index
            ax.fill_between(years, yearly_min.values, yearly_max.values, alpha=0.3, label=msa)
        
        ax.set_title('Annual Water Level Range by Region\n(Min to Max Each Year)', fontsize=12, fontweight='bold')
        ax.set_xlabel('Year')
        ax.set_ylabel('Water Level (ft below surface)')
        ax.legend(title='MSA', fontsize=8, loc='upper left')
        ax.grid(True, alpha=0.3)
        
        # Plot 8c: Departure from long-term mean (anomaly)
        ax = axes[1, 0]
        anomaly_data = []
        for msa in msa_order:
            region_data = msa_data[msa_data['MSA'] == msa].copy()
            if len(region_data) < 100:
                continue
            
            long_term_mean = region_data['lev_va'].mean()
            yearly_mean = region_data.groupby('year')['lev_va'].mean()
            anomaly = yearly_mean - long_term_mean
            
            for year, anom in anomaly.items():
                anomaly_data.append({'MSA': msa, 'year': year, 'anomaly': anom})
        
        anomaly_df = pd.DataFrame(anomaly_data)
        if not anomaly_df.empty:
            # Plot anomaly time series for top regions
            for msa in msa_order[:5]:
                msa_anomaly = anomaly_df[anomaly_df['MSA'] == msa]
                if not msa_anomaly.empty:
                    ax.plot(msa_anomaly['year'], msa_anomaly['anomaly'], marker='o', 
                           label=msa, linewidth=1.5, markersize=3)
            
            ax.axhline(0, color='black', linestyle='-', linewidth=1)
            ax.fill_between(ax.get_xlim(), -5, 5, alpha=0.1, color='green', label='Normal range')
            ax.set_title('Water Level Anomaly from Long-term Mean\n(Top 5 Regions)', fontsize=12, fontweight='bold')
            ax.set_xlabel('Year')
            ax.set_ylabel('Anomaly (ft from mean)\n(+ = Deeper, - = Shallower)')
            ax.legend(title='MSA', fontsize=8)
            ax.grid(True, alpha=0.3)
        
        # Plot 8d: Inter-annual variability (standard deviation of annual means)
        ax = axes[1, 1]
        interannual_var = msa_data.groupby(['MSA', 'year'])['lev_va'].mean().groupby('MSA').std()
        interannual_var = interannual_var.sort_values()
        
        colors = plt.cm.YlOrRd(interannual_var / interannual_var.max())
        ax.barh(interannual_var.index, interannual_var.values, color=colors, edgecolor='black')
        ax.set_title('Inter-annual Variability by Region\n(Std Dev of Annual Means)', fontsize=12, fontweight='bold')
        ax.set_xlabel('Standard Deviation (ft)')
        ax.set_ylabel('Metro Region (MSA)')
        ax.grid(True, alpha=0.3, axis='x')
        
        plt.tight_layout()
        plot_file = plots_dir / "extreme_events_analysis.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"  ✓ Saved: {plot_file.name}")
        
    except Exception as e:
        print(f"  Warning: Could not create extreme events plot: {e}")
        import traceback
        traceback.print_exc()
    
    # --- Plot 9: Rolling Trend & Rate of Change Analysis ---
    print("\n  Creating rolling trend and rate of change analysis...")
    try:
        fig, axes = plt.subplots(2, 2, figsize=(16, 14))
        
        # Prepare annual data
        annual_means = msa_data.groupby(['MSA', 'year'])['lev_va'].mean().unstack(level=0)
        
        # Plot 9a: Rolling 10-year trend slopes
        ax = axes[0, 0]
        window = 10
        
        for msa in msa_order[:5]:  # Top 5 regions
            if msa not in annual_means.columns:
                continue
            series = annual_means[msa].dropna()
            if len(series) < window + 5:
                continue
            
            rolling_slopes = []
            years = []
            for i in range(len(series) - window + 1):
                window_data = series.iloc[i:i+window]
                x = np.arange(len(window_data))
                slope, _, _, _, _ = scipy_stats.linregress(x, window_data.values)
                rolling_slopes.append(slope)
                years.append(series.index[i + window - 1])
            
            ax.plot(years, rolling_slopes, marker='o', label=msa, linewidth=2, markersize=4)
        
        ax.axhline(0, color='black', linestyle='-', linewidth=1)
        ax.fill_between(ax.get_xlim(), -0.5, 0.5, alpha=0.1, color='gray')
        ax.set_title(f'Rolling {window}-Year Trend Slope by Region', fontsize=12, fontweight='bold')
        ax.set_xlabel('Year (End of Window)')
        ax.set_ylabel('Slope (ft/year)')
        ax.legend(title='MSA', fontsize=9)
        ax.grid(True, alpha=0.3)
        
        # Plot 9b: Acceleration (change in slope over time)
        ax = axes[0, 1]
        accel_data = []
        for msa in msa_order:
            if msa not in annual_means.columns:
                continue
            series = annual_means[msa].dropna()
            if len(series) < 20:
                continue
            
            # Split into early and late periods
            mid_point = len(series) // 2
            early = series.iloc[:mid_point]
            late = series.iloc[mid_point:]
            
            if len(early) >= 5 and len(late) >= 5:
                early_slope, _, _, _, _ = scipy_stats.linregress(np.arange(len(early)), early.values)
                late_slope, _, _, _, _ = scipy_stats.linregress(np.arange(len(late)), late.values)
                accel_data.append({
                    'MSA': msa,
                    'early_slope': early_slope,
                    'late_slope': late_slope,
                    'acceleration': late_slope - early_slope
                })
        
        if accel_data:
            accel_df = pd.DataFrame(accel_data).set_index('MSA')
            accel_df = accel_df.sort_values('acceleration')
            
            colors = ['#d73027' if a > 0 else '#4575b4' for a in accel_df['acceleration']]
            ax.barh(accel_df.index, accel_df['acceleration'], color=colors, edgecolor='black')
            ax.axvline(0, color='black', linewidth=1)
            ax.set_title('Trend Acceleration by Region\n(Late Period Slope - Early Period Slope)', fontsize=12, fontweight='bold')
            ax.set_xlabel('Acceleration (ft/year²)\n(+ = Accelerating decline, - = Slowing/Reversing)')
            ax.set_ylabel('Metro Region (MSA)')
            ax.grid(True, alpha=0.3, axis='x')
            
            # Add legend
            from matplotlib.patches import Patch
            legend_elements = [Patch(facecolor='#d73027', label='Accelerating decline'),
                              Patch(facecolor='#4575b4', label='Slowing/Improving')]
            ax.legend(handles=legend_elements, loc='lower right', fontsize=9)
        
        # Plot 9c: Year-over-year change distribution
        ax = axes[1, 0]
        yoy_changes = []
        for msa in msa_order:
            if msa not in annual_means.columns:
                continue
            series = annual_means[msa].dropna()
            changes = series.diff().dropna()
            for change in changes:
                yoy_changes.append({'MSA': msa, 'change': change})
        
        yoy_df = pd.DataFrame(yoy_changes)
        if not yoy_df.empty:
            yoy_by_msa = [yoy_df[yoy_df['MSA'] == msa]['change'].values for msa in msa_order if msa in yoy_df['MSA'].values]
            valid_msas = [msa for msa in msa_order if msa in yoy_df['MSA'].values]
            
            bp = ax.boxplot(yoy_by_msa, labels=valid_msas, patch_artist=True, vert=True)
            for patch in bp['boxes']:
                patch.set_facecolor('lightblue')
                patch.set_alpha(0.7)
            
            ax.axhline(0, color='red', linestyle='--', linewidth=1.5)
            ax.set_title('Year-over-Year Change Distribution by Region', fontsize=12, fontweight='bold')
            ax.set_xlabel('Metro Region (MSA)')
            ax.set_ylabel('Annual Change (ft)\n(+ = Deepening, - = Rising)')
            ax.tick_params(axis='x', rotation=45)
            ax.grid(True, alpha=0.3, axis='y')
        
        # Plot 9d: Cumulative change since baseline
        ax = axes[1, 1]
        baseline_year = 1990
        
        for msa in msa_order[:6]:  # Top 6 regions
            if msa not in annual_means.columns:
                continue
            series = annual_means[msa].dropna()
            series = series[series.index >= baseline_year]
            if len(series) < 5:
                continue
            
            baseline = series.iloc[0]
            cumulative = series - baseline
            ax.plot(cumulative.index, cumulative.values, marker='o', label=msa, linewidth=2, markersize=3)
        
        ax.axhline(0, color='black', linestyle='-', linewidth=1)
        ax.set_title(f'Cumulative Change Since {baseline_year}', fontsize=12, fontweight='bold')
        ax.set_xlabel('Year')
        ax.set_ylabel('Cumulative Change (ft from baseline)\n(+ = Deeper, - = Shallower)')
        ax.legend(title='MSA', fontsize=9)
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plot_file = plots_dir / "rolling_trend_analysis.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"  ✓ Saved: {plot_file.name}")
        
    except Exception as e:
        print(f"  Warning: Could not create rolling trend plot: {e}")
        import traceback
        traceback.print_exc()
    
    # --- Plot 10: Geographic Grouping Analysis ---
    print("\n  Creating geographic grouping analysis...")
    try:
        # Define geographic groupings
        coastal_regions = ['Miami', 'San Francisco', 'Boston', 'New York']
        inland_regions = ['Dallas', 'Chicago', 'Philadelphia', 'Washington']
        gulf_regions = ['Miami', 'Houston']
        east_coast = ['Miami', 'Boston', 'New York', 'Philadelphia', 'Washington']
        west_coast = ['San Francisco']
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 14))
        
        # Plot 10a: Coastal vs Inland comparison
        ax = axes[0, 0]
        msa_data['geo_type'] = msa_data['MSA'].apply(
            lambda x: 'Coastal' if x in coastal_regions else ('Inland' if x in inland_regions else 'Other')
        )
        
        geo_groups = msa_data[msa_data['geo_type'] != 'Other'].groupby('geo_type')['lev_va']
        geo_data = [geo_groups.get_group(g).dropna().values for g in ['Coastal', 'Inland'] if g in geo_groups.groups]
        geo_labels = [g for g in ['Coastal', 'Inland'] if g in geo_groups.groups]
        
        if geo_data:
            parts = ax.violinplot(geo_data, positions=range(len(geo_labels)), showmeans=True, showmedians=True)
            ax.set_xticks(range(len(geo_labels)))
            ax.set_xticklabels(geo_labels)
            ax.set_title('Water Level Distribution: Coastal vs Inland', fontsize=12, fontweight='bold')
            ax.set_xlabel('Geographic Type')
            ax.set_ylabel('Water Level (ft below surface)')
            ax.grid(True, alpha=0.3, axis='y')
            
            # Add statistics
            for i, (geo_arr, label) in enumerate(zip(geo_data, geo_labels)):
                ax.text(i, ax.get_ylim()[1] * 0.95, f'n={len(geo_arr):,}\nmean={np.mean(geo_arr):.1f}', 
                       ha='center', va='top', fontsize=9)
        
        # Plot 10b: East vs West Coast trends
        ax = axes[0, 1]
        msa_data['coast'] = msa_data['MSA'].apply(
            lambda x: 'East Coast' if x in east_coast else ('West Coast' if x in west_coast else 'Gulf/Central')
        )
        
        coast_annual = msa_data.groupby(['coast', 'year'])['lev_va'].mean().unstack(level=0)
        for coast in ['East Coast', 'West Coast', 'Gulf/Central']:
            if coast in coast_annual.columns:
                series = coast_annual[coast].dropna()
                ax.plot(series.index, series.values, marker='o', label=coast, linewidth=2, markersize=4)
        
        ax.set_title('Annual Mean Water Level by Coast/Region', fontsize=12, fontweight='bold')
        ax.set_xlabel('Year')
        ax.set_ylabel('Mean Water Level (ft below surface)')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Plot 10c: Climate zone proxy (using depth as proxy for aquifer type)
        ax = axes[1, 0]
        depth_stats = msa_data.groupby('MSA')['lev_va'].agg(['mean', 'std', 'median', 'count'])
        depth_stats['iqr'] = msa_data.groupby('MSA')['lev_va'].apply(
            lambda x: np.percentile(x.dropna(), 75) - np.percentile(x.dropna(), 25)
        )
        
        # Scatter: mean depth vs variability (IQR)
        scatter = ax.scatter(depth_stats['mean'], depth_stats['iqr'], 
                            s=depth_stats['count'] / 100, 
                            c=depth_stats['std'], cmap='YlOrRd',
                            edgecolors='black', alpha=0.7)
        
        for msa in depth_stats.index:
            ax.annotate(msa, (depth_stats.loc[msa, 'mean'], depth_stats.loc[msa, 'iqr']),
                       fontsize=8, ha='center', va='bottom')
        
        ax.set_title('Aquifer Characteristics by Region\n(bubble size = record count)', fontsize=12, fontweight='bold')
        ax.set_xlabel('Mean Water Level (ft below surface)')
        ax.set_ylabel('Interquartile Range (ft)')
        plt.colorbar(scatter, ax=ax, label='Std Deviation')
        ax.grid(True, alpha=0.3)
        
        # Plot 10d: Regional comparison radar/polar chart
        ax = axes[1, 1]
        # Compute normalized metrics for each region
        metrics = {}
        for msa in msa_order:
            region_data = msa_data[msa_data['MSA'] == msa]['lev_va'].dropna()
            if len(region_data) < 100:
                continue
            metrics[msa] = {
                'Mean Depth': region_data.mean(),
                'Variability (CV)': (region_data.std() / region_data.mean()) * 100 if region_data.mean() != 0 else 0,
                'Data Density': len(region_data) / msa_data[msa_data['MSA'] == msa]['site_no'].nunique(),
                'Seasonal Range': region_data.max() - region_data.min()
            }
        
        if metrics:
            metrics_df = pd.DataFrame(metrics).T
            # Normalize to 0-1 scale
            metrics_norm = (metrics_df - metrics_df.min()) / (metrics_df.max() - metrics_df.min())
            
            # Create grouped bar chart instead of radar (easier to read)
            x = np.arange(len(metrics_norm.columns))
            width = 0.8 / len(metrics_norm)
            
            for i, (msa, row) in enumerate(metrics_norm.iterrows()):
                ax.bar(x + i * width, row.values, width, label=msa, alpha=0.8)
            
            ax.set_xticks(x + width * len(metrics_norm) / 2)
            ax.set_xticklabels(metrics_norm.columns, rotation=45, ha='right')
            ax.set_title('Normalized Regional Metrics Comparison', fontsize=12, fontweight='bold')
            ax.set_xlabel('Metric')
            ax.set_ylabel('Normalized Value (0-1)')
            ax.legend(title='MSA', bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=8)
            ax.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plot_file = plots_dir / "geographic_grouping_analysis.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"  ✓ Saved: {plot_file.name}")
        
    except Exception as e:
        print(f"  Warning: Could not create geographic grouping plot: {e}")
        import traceback
        traceback.print_exc()
    
    # --- Plot 11: Regional Summary Dashboard ---
    print("\n  Creating regional summary dashboard...")
    try:
        n_regions = len(msa_order)
        fig = plt.figure(figsize=(20, 16))
        
        # Create grid for sparklines and metrics
        # Need n_regions + 2 rows: 1 header + 1 column headers + n_regions data rows
        gs = fig.add_gridspec(n_regions + 2, 5, hspace=0.4, wspace=0.3)
        
        # Header row
        header_ax = fig.add_subplot(gs[0, :])
        header_ax.axis('off')
        header_ax.text(0.5, 0.5, 'REGIONAL GROUNDWATER SUMMARY DASHBOARD', 
                      fontsize=18, fontweight='bold', ha='center', va='center',
                      transform=header_ax.transAxes)
        
        # Column headers
        col_headers = ['Region', 'Trend Sparkline', 'Mean (ft)', 'Trend', 'Data Quality']
        for j, header in enumerate(col_headers):
            ax = fig.add_subplot(gs[1, j])
            ax.axis('off')
            ax.text(0.5, 0.5, header, fontsize=11, fontweight='bold', ha='center', va='center')
        
        # Data rows
        for i, msa in enumerate(msa_order):
            region_data = msa_data[msa_data['MSA'] == msa]
            if region_data.empty:
                continue
            
            # Column 0: Region name
            ax = fig.add_subplot(gs[i + 2, 0])
            ax.axis('off')
            ax.text(0.5, 0.5, msa, fontsize=10, fontweight='bold', ha='center', va='center')
            
            # Column 1: Sparkline
            ax = fig.add_subplot(gs[i + 2, 1])
            annual = region_data.groupby('year')['lev_va'].mean()
            if len(annual) > 3:
                ax.plot(annual.index, annual.values, color='steelblue', linewidth=1.5)
                ax.fill_between(annual.index, annual.values, alpha=0.3, color='steelblue')
                # Add trend line
                z = np.polyfit(range(len(annual)), annual.values, 1)
                p = np.poly1d(z)
                ax.plot(annual.index, p(range(len(annual))), 'r--', linewidth=1)
            ax.set_xticks([])
            ax.set_yticks([])
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            
            # Column 2: Mean depth
            ax = fig.add_subplot(gs[i + 2, 2])
            ax.axis('off')
            mean_val = region_data['lev_va'].mean()
            ax.text(0.5, 0.5, f'{mean_val:.1f}', fontsize=12, ha='center', va='center')
            
            # Column 3: Trend indicator
            ax = fig.add_subplot(gs[i + 2, 3])
            ax.axis('off')
            if len(annual) > 3:
                slope = z[0]  # from polyfit above
                if slope > 0.1:
                    indicator = '↓ Falling'
                    color = '#d73027'
                elif slope < -0.1:
                    indicator = '↑ Rising'
                    color = '#4575b4'
                else:
                    indicator = '→ Stable'
                    color = '#999999'
                ax.text(0.5, 0.5, indicator, fontsize=11, ha='center', va='center', 
                       color=color, fontweight='bold')
            
            # Column 4: Data quality indicator
            ax = fig.add_subplot(gs[i + 2, 4])
            ax.axis('off')
            region_records = len(region_data)
            region_wells = region_data['site_no'].nunique()
            years_span = region_data['year'].max() - region_data['year'].min()
            
            # Simple quality score
            if region_records > 10000 and years_span > 30:
                quality = '★★★ Excellent'
                color = '#2166ac'
            elif region_records > 1000 and years_span > 20:
                quality = '★★☆ Good'
                color = '#67a9cf'
            elif region_records > 100:
                quality = '★☆☆ Fair'
                color = '#fddbc7'
            else:
                quality = '☆☆☆ Limited'
                color = '#d73027'
            
            ax.text(0.5, 0.5, quality, fontsize=10, ha='center', va='center', color=color)
        
        plt.tight_layout()
        plot_file = plots_dir / "regional_summary_dashboard.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"  ✓ Saved: {plot_file.name}")
        
    except Exception as e:
        print(f"  Warning: Could not create summary dashboard: {e}")
        import traceback
        traceback.print_exc()
    
    # --- Plot 12: Change Point Detection & Regime Shifts ---
    print("\n  Creating change point detection analysis...")
    try:
        fig, axes = plt.subplots(2, 2, figsize=(16, 14))
        
        # Prepare annual data by region
        annual_means = msa_data.groupby(['MSA', 'year'])['lev_va'].mean().unstack(level=0)
        
        # Simple change point detection using cumulative sum (CUSUM)
        def detect_change_points(series, threshold=2.0):
            """Detect change points using CUSUM method."""
            if len(series) < 10:
                return []
            series = series.dropna()
            mean_val = series.mean()
            std_val = series.std()
            if std_val == 0:
                return []
            
            # Normalized cumulative sum
            normalized = (series - mean_val) / std_val
            cusum = normalized.cumsum()
            
            # Find significant deviations
            change_points = []
            for i in range(5, len(cusum) - 5):
                # Check if this is a local extremum
                local_window = cusum.iloc[max(0, i-3):min(len(cusum), i+4)]
                if cusum.iloc[i] == local_window.max() or cusum.iloc[i] == local_window.min():
                    if abs(cusum.iloc[i]) > threshold:
                        change_points.append(series.index[i])
            return change_points
        
        # Plot 12a: Change point visualization for top regions
        ax = axes[0, 0]
        change_point_results = {}
        colors = plt.cm.tab10(np.linspace(0, 1, len(msa_order)))
        
        for idx, msa in enumerate(msa_order[:6]):
            if msa not in annual_means.columns:
                continue
            series = annual_means[msa].dropna()
            if len(series) < 10:
                continue
            
            # Normalize for comparison
            normalized = (series - series.mean()) / series.std()
            ax.plot(series.index, normalized.values, label=msa, color=colors[idx], linewidth=1.5, alpha=0.7)
            
            # Detect and mark change points
            cps = detect_change_points(series)
            change_point_results[msa] = cps
            for cp in cps:
                ax.axvline(cp, color=colors[idx], linestyle='--', alpha=0.5, linewidth=1)
        
        ax.axhline(0, color='black', linestyle='-', linewidth=0.5)
        ax.set_title('Normalized Water Level Trends with Change Points', fontsize=12, fontweight='bold')
        ax.set_xlabel('Year')
        ax.set_ylabel('Normalized Water Level (z-score)')
        ax.legend(title='MSA', fontsize=8, loc='upper left')
        ax.grid(True, alpha=0.3)
        
        # Plot 12b: Change point summary by region
        ax = axes[0, 1]
        cp_summary = []
        for msa in msa_order:
            if msa in change_point_results:
                cps = change_point_results[msa]
                cp_summary.append({
                    'MSA': msa,
                    'n_change_points': len(cps),
                    'years': ', '.join(map(str, cps)) if cps else 'None'
                })
        
        if cp_summary:
            cp_df = pd.DataFrame(cp_summary)
            bars = ax.barh(cp_df['MSA'], cp_df['n_change_points'], color='steelblue', edgecolor='black')
            ax.set_title('Number of Detected Regime Shifts by Region', fontsize=12, fontweight='bold')
            ax.set_xlabel('Number of Change Points')
            ax.set_ylabel('Metro Region (MSA)')
            ax.grid(True, alpha=0.3, axis='x')
            
            # Add year labels on bars
            for i, (bar, row) in enumerate(zip(bars, cp_df.itertuples())):
                if row.n_change_points > 0:
                    ax.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2, 
                           row.years, va='center', fontsize=8)
        
        # Plot 12c: Decade-over-decade change comparison
        ax = axes[1, 0]
        decade_changes = []
        for msa in msa_order:
            if msa not in annual_means.columns:
                continue
            series = annual_means[msa].dropna()
            
            # Calculate mean by decade
            decades = [1980, 1990, 2000, 2010, 2020]
            for i in range(len(decades) - 1):
                d1, d2 = decades[i], decades[i+1]
                mean1 = series[(series.index >= d1) & (series.index < d2)].mean()
                mean2 = series[(series.index >= d2) & (series.index < d2+10)].mean()
                if pd.notna(mean1) and pd.notna(mean2):
                    decade_changes.append({
                        'MSA': msa,
                        'period': f'{d1}s→{d2}s',
                        'change': mean2 - mean1
                    })
        
        if decade_changes:
            dc_df = pd.DataFrame(decade_changes)
            dc_pivot = dc_df.pivot(index='MSA', columns='period', values='change')
            
            dc_pivot.plot(kind='bar', ax=ax, width=0.8, edgecolor='black')
            ax.axhline(0, color='black', linewidth=1)
            ax.set_title('Decade-over-Decade Water Level Change', fontsize=12, fontweight='bold')
            ax.set_xlabel('Metro Region (MSA)')
            ax.set_ylabel('Change in Mean Water Level (ft)\n(+ = Deepening)')
            ax.legend(title='Period', fontsize=8)
            ax.tick_params(axis='x', rotation=45)
            ax.grid(True, alpha=0.3, axis='y')
        
        # Plot 12d: Stability periods identification
        ax = axes[1, 1]
        stability_data = []
        for msa in msa_order:
            if msa not in annual_means.columns:
                continue
            series = annual_means[msa].dropna()
            if len(series) < 15:
                continue
            
            # Calculate rolling standard deviation (5-year window)
            rolling_std = series.rolling(window=5, center=True).std()
            
            # Find most stable and most volatile periods
            if not rolling_std.dropna().empty:
                most_stable_year = rolling_std.idxmin()
                most_volatile_year = rolling_std.idxmax()
                stability_data.append({
                    'MSA': msa,
                    'most_stable': most_stable_year,
                    'most_volatile': most_volatile_year,
                    'avg_volatility': rolling_std.mean()
                })
        
        if stability_data:
            stab_df = pd.DataFrame(stability_data).set_index('MSA')
            stab_df = stab_df.sort_values('avg_volatility')
            
            colors = plt.cm.RdYlGn_r(stab_df['avg_volatility'] / stab_df['avg_volatility'].max())
            ax.barh(stab_df.index, stab_df['avg_volatility'], color=colors, edgecolor='black')
            ax.set_title('Average Water Level Volatility by Region\n(5-year rolling std)', fontsize=12, fontweight='bold')
            ax.set_xlabel('Average Volatility (ft)')
            ax.set_ylabel('Metro Region (MSA)')
            ax.grid(True, alpha=0.3, axis='x')
        
        plt.tight_layout()
        plot_file = plots_dir / "change_point_analysis.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"  ✓ Saved: {plot_file.name}")
        
    except Exception as e:
        print(f"  Warning: Could not create change point analysis: {e}")
        import traceback
        traceback.print_exc()
    
    # --- Plot 13: Sustainability Index & Risk Assessment ---
    print("\n  Creating sustainability index and risk assessment...")
    try:
        fig, axes = plt.subplots(2, 2, figsize=(16, 14))
        
        # Calculate sustainability metrics for each region
        sustainability_metrics = []
        for msa in msa_order:
            region_data = msa_data[msa_data['MSA'] == msa]
            if len(region_data) < 100:
                continue
            
            annual = region_data.groupby('year')['lev_va'].mean()
            if len(annual) < 10:
                continue
            
            # Calculate metrics
            # 1. Long-term trend (slope)
            years = np.arange(len(annual))
            slope, intercept, r_value, p_value, std_err = scipy_stats.linregress(years, annual.values)
            
            # 2. Recent trend (last 10 years)
            recent = annual.tail(10)
            if len(recent) >= 5:
                recent_slope, _, _, _, _ = scipy_stats.linregress(np.arange(len(recent)), recent.values)
            else:
                recent_slope = slope
            
            # 3. Variability (CV)
            cv = (annual.std() / annual.mean()) * 100 if annual.mean() != 0 else 0
            
            # 4. Recovery indicator (is recent better than long-term trend?)
            recovery = 1 if recent_slope < slope else 0
            
            # 5. Data quality score (based on record count and time span)
            n_records = len(region_data)
            years_span = annual.index.max() - annual.index.min()
            data_score = min(100, (n_records / 10000) * 50 + (years_span / 50) * 50)
            
            # Calculate composite sustainability index (0-100)
            # Lower slope = better (less depletion), lower CV = better (more stable)
            # Normalize slope: assume -2 to +2 ft/year range
            slope_score = max(0, min(100, 50 - slope * 25))  # 0 slope = 50, negative = better
            cv_score = max(0, min(100, 100 - cv))  # lower CV = better
            recent_score = max(0, min(100, 50 - recent_slope * 25))
            
            sustainability_index = (slope_score * 0.4 + cv_score * 0.3 + recent_score * 0.2 + data_score * 0.1)
            
            sustainability_metrics.append({
                'MSA': msa,
                'sustainability_index': round(sustainability_index, 1),
                'long_term_slope': round(slope, 4),
                'recent_slope': round(recent_slope, 4),
                'cv_percent': round(cv, 1),
                'data_quality': round(data_score, 1),
                'recovery_indicator': recovery,
                'risk_level': 'Low' if sustainability_index >= 60 else ('Medium' if sustainability_index >= 40 else 'High')
            })
        
        sus_df = pd.DataFrame(sustainability_metrics)
        
        # Plot 13a: Sustainability Index by region
        ax = axes[0, 0]
        sus_df_sorted = sus_df.sort_values('sustainability_index', ascending=True)
        
        colors = []
        for idx in sus_df_sorted['sustainability_index']:
            if idx >= 60:
                colors.append('#2166ac')  # Blue - good
            elif idx >= 40:
                colors.append('#fdae61')  # Orange - medium
            else:
                colors.append('#d73027')  # Red - poor
        
        bars = ax.barh(sus_df_sorted['MSA'], sus_df_sorted['sustainability_index'], 
                      color=colors, edgecolor='black')
        ax.axvline(60, color='green', linestyle='--', linewidth=2, label='Good threshold')
        ax.axvline(40, color='orange', linestyle='--', linewidth=2, label='Warning threshold')
        ax.set_xlim(0, 100)
        ax.set_title('Groundwater Sustainability Index by Region\n(0-100 scale, higher = more sustainable)', 
                    fontsize=12, fontweight='bold')
        ax.set_xlabel('Sustainability Index')
        ax.set_ylabel('Metro Region (MSA)')
        ax.legend(loc='lower right', fontsize=9)
        ax.grid(True, alpha=0.3, axis='x')
        
        # Add index values on bars
        for bar, val in zip(bars, sus_df_sorted['sustainability_index']):
            ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2, 
                   f'{val:.0f}', va='center', fontsize=10, fontweight='bold')
        
        # Plot 13b: Risk matrix (slope vs variability)
        ax = axes[0, 1]
        scatter = ax.scatter(sus_df['long_term_slope'], sus_df['cv_percent'],
                            s=sus_df['sustainability_index'] * 3,
                            c=sus_df['sustainability_index'], cmap='RdYlGn',
                            edgecolors='black', alpha=0.8, vmin=0, vmax=100)
        
        for _, row in sus_df.iterrows():
            ax.annotate(row['MSA'], (row['long_term_slope'], row['cv_percent']),
                       fontsize=8, ha='center', va='bottom')
        
        ax.axhline(sus_df['cv_percent'].median(), color='gray', linestyle='--', alpha=0.5)
        ax.axvline(0, color='black', linestyle='-', linewidth=1)
        
        # Add quadrant labels
        ax.text(0.95, 0.95, 'High Risk\n(Declining + Variable)', transform=ax.transAxes, 
               ha='right', va='top', fontsize=9, color='red', fontweight='bold')
        ax.text(0.05, 0.95, 'Moderate Risk\n(Rising + Variable)', transform=ax.transAxes,
               ha='left', va='top', fontsize=9, color='orange', fontweight='bold')
        ax.text(0.05, 0.05, 'Low Risk\n(Rising + Stable)', transform=ax.transAxes,
               ha='left', va='bottom', fontsize=9, color='green', fontweight='bold')
        
        ax.set_title('Risk Matrix: Trend vs Variability\n(bubble size = sustainability index)', 
                    fontsize=12, fontweight='bold')
        ax.set_xlabel('Long-term Slope (ft/year)\n(+ = Declining, - = Rising)')
        ax.set_ylabel('Coefficient of Variation (%)')
        plt.colorbar(scatter, ax=ax, label='Sustainability Index')
        ax.grid(True, alpha=0.3)
        
        # Plot 13c: Component breakdown
        ax = axes[1, 0]
        component_data = sus_df[['MSA', 'sustainability_index']].copy()
        component_data['Trend Score'] = sus_df['long_term_slope'].apply(lambda x: max(0, min(100, 50 - x * 25)) * 0.4)
        component_data['Stability Score'] = sus_df['cv_percent'].apply(lambda x: max(0, min(100, 100 - x)) * 0.3)
        component_data['Recent Score'] = sus_df['recent_slope'].apply(lambda x: max(0, min(100, 50 - x * 25)) * 0.2)
        component_data['Data Score'] = sus_df['data_quality'] * 0.1
        
        component_data = component_data.set_index('MSA')
        component_data[['Trend Score', 'Stability Score', 'Recent Score', 'Data Score']].plot(
            kind='barh', stacked=True, ax=ax, 
            color=['#4575b4', '#91bfdb', '#fee090', '#fc8d59'],
            edgecolor='black', linewidth=0.5
        )
        ax.set_title('Sustainability Index Components by Region', fontsize=12, fontweight='bold')
        ax.set_xlabel('Component Score')
        ax.set_ylabel('Metro Region (MSA)')
        ax.legend(title='Component', bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3, axis='x')
        
        # Plot 13d: Risk level summary (pie chart)
        ax = axes[1, 1]
        risk_counts = sus_df['risk_level'].value_counts()
        colors_pie = {'Low': '#2166ac', 'Medium': '#fdae61', 'High': '#d73027'}
        pie_colors = [colors_pie.get(r, 'gray') for r in risk_counts.index]
        
        wedges, texts, autotexts = ax.pie(risk_counts.values, labels=risk_counts.index, 
                                          autopct='%1.0f%%', colors=pie_colors,
                                          explode=[0.05] * len(risk_counts),
                                          textprops={'fontsize': 11})
        ax.set_title('Regional Risk Level Distribution', fontsize=12, fontweight='bold')
        
        # Add region names in legend
        legend_labels = []
        for risk in ['Low', 'Medium', 'High']:
            regions = sus_df[sus_df['risk_level'] == risk]['MSA'].tolist()
            if regions:
                legend_labels.append(f"{risk}: {', '.join(regions)}")
        ax.text(0, -1.3, '\n'.join(legend_labels), ha='center', va='top', fontsize=9,
               transform=ax.transAxes, wrap=True)
        
        plt.tight_layout()
        plot_file = plots_dir / "sustainability_index.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"  ✓ Saved: {plot_file.name}")
        
        # Save sustainability metrics to CSV
        sus_df.to_csv(output_dir / "sustainability_metrics.csv", index=False)
        print(f"  ✓ Saved: sustainability_metrics.csv")
        
    except Exception as e:
        print(f"  Warning: Could not create sustainability index: {e}")
        import traceback
        traceback.print_exc()
    
    # --- Plot 14: Future Projections ---
    print("\n  Creating future projections analysis...")
    try:
        fig, axes = plt.subplots(2, 2, figsize=(16, 14))
        
        # Projection parameters
        projection_years = [5, 10, 20]
        current_year = msa_data['year'].max()
        
        projection_results = []
        
        # Plot 14a: Linear projections for all regions
        ax = axes[0, 0]
        colors = plt.cm.tab10(np.linspace(0, 1, len(msa_order)))
        
        for idx, msa in enumerate(msa_order):
            if msa not in annual_means.columns:
                continue
            series = annual_means[msa].dropna()
            if len(series) < 10:
                continue
            
            # Fit linear trend
            years = series.index.values
            values = series.values
            slope, intercept, r_value, _, _ = scipy_stats.linregress(years, values)
            
            # Historical data
            ax.plot(years, values, color=colors[idx], linewidth=1.5, alpha=0.7, label=msa)
            
            # Projection
            future_years = np.arange(current_year + 1, current_year + 21)
            projected = intercept + slope * future_years
            ax.plot(future_years, projected, color=colors[idx], linestyle='--', linewidth=1, alpha=0.5)
            
            # Store projections
            for proj_yr in projection_years:
                proj_value = intercept + slope * (current_year + proj_yr)
                current_value = series.iloc[-1] if len(series) > 0 else np.nan
                projection_results.append({
                    'MSA': msa,
                    'projection_year': proj_yr,
                    'target_year': current_year + proj_yr,
                    'current_level': round(current_value, 2),
                    'projected_level': round(proj_value, 2),
                    'change': round(proj_value - current_value, 2),
                    'slope': round(slope, 4),
                    'r_squared': round(r_value**2, 3)
                })
        
        ax.axvline(current_year, color='red', linestyle='-', linewidth=2, label='Current Year')
        ax.set_title(f'Water Level Projections (20-Year Outlook)\nBased on Linear Trend', 
                    fontsize=12, fontweight='bold')
        ax.set_xlabel('Year')
        ax.set_ylabel('Water Level (ft below surface)')
        ax.legend(title='MSA', fontsize=8, loc='upper left', ncol=2)
        ax.grid(True, alpha=0.3)
        
        # Add shaded projection zone
        ax.axvspan(current_year, current_year + 20, alpha=0.1, color='gray', label='Projection Period')
        
        # Plot 14b: Projected change by region (bar chart)
        ax = axes[0, 1]
        proj_df = pd.DataFrame(projection_results)
        proj_10yr = proj_df[proj_df['projection_year'] == 10].copy()
        proj_10yr = proj_10yr.sort_values('change')
        
        colors_bar = ['#d73027' if c > 0 else '#4575b4' for c in proj_10yr['change']]
        ax.barh(proj_10yr['MSA'], proj_10yr['change'], color=colors_bar, edgecolor='black')
        ax.axvline(0, color='black', linewidth=1)
        ax.set_title('Projected 10-Year Change in Water Level', fontsize=12, fontweight='bold')
        ax.set_xlabel('Projected Change (ft)\n(+ = Deepening, - = Rising)')
        ax.set_ylabel('Metro Region (MSA)')
        ax.grid(True, alpha=0.3, axis='x')
        
        # Add values on bars
        for i, (_, row) in enumerate(proj_10yr.iterrows()):
            ax.text(row['change'] + (0.5 if row['change'] >= 0 else -0.5), i, 
                   f"{row['change']:+.1f} ft", va='center', 
                   ha='left' if row['change'] >= 0 else 'right', fontsize=9)
        
        # Plot 14c: Projection confidence (R² values)
        ax = axes[1, 0]
        r2_data = proj_df[proj_df['projection_year'] == 10][['MSA', 'r_squared']].drop_duplicates()
        r2_data = r2_data.sort_values('r_squared', ascending=True)
        
        colors_r2 = plt.cm.RdYlGn(r2_data['r_squared'])
        ax.barh(r2_data['MSA'], r2_data['r_squared'], color=colors_r2, edgecolor='black')
        ax.axvline(0.5, color='orange', linestyle='--', linewidth=2, label='Moderate fit (R²=0.5)')
        ax.axvline(0.7, color='green', linestyle='--', linewidth=2, label='Good fit (R²=0.7)')
        ax.set_xlim(0, 1)
        ax.set_title('Projection Reliability (R² of Linear Fit)', fontsize=12, fontweight='bold')
        ax.set_xlabel('R² Value (higher = more reliable projection)')
        ax.set_ylabel('Metro Region (MSA)')
        ax.legend(loc='lower right', fontsize=9)
        ax.grid(True, alpha=0.3, axis='x')
        
        # Plot 14d: Multi-horizon projections table visualization
        ax = axes[1, 1]
        ax.axis('off')
        
        # Create summary table
        table_data = []
        for msa in msa_order:
            msa_proj = proj_df[proj_df['MSA'] == msa]
            if msa_proj.empty:
                continue
            row = [msa]
            for yr in projection_years:
                yr_data = msa_proj[msa_proj['projection_year'] == yr]
                if not yr_data.empty:
                    row.append(f"{yr_data['change'].values[0]:+.1f}")
                else:
                    row.append('N/A')
            table_data.append(row)
        
        if table_data:
            table = ax.table(cellText=table_data,
                           colLabels=['Region', '5-Year', '10-Year', '20-Year'],
                           loc='center',
                           cellLoc='center',
                           colWidths=[0.3, 0.2, 0.2, 0.2])
            table.auto_set_font_size(False)
            table.set_fontsize(10)
            table.scale(1.2, 1.8)
            
            # Color cells based on values
            for i, row in enumerate(table_data):
                for j in range(1, 4):
                    try:
                        val = float(row[j].replace('+', ''))
                        if val > 5:
                            color = '#ffcccc'  # Light red
                        elif val > 0:
                            color = '#fff3cd'  # Light yellow
                        elif val > -5:
                            color = '#d4edda'  # Light green
                        else:
                            color = '#cce5ff'  # Light blue
                        table[(i+1, j)].set_facecolor(color)
                    except:
                        pass
            
            ax.set_title('Projected Water Level Changes (ft)\n(+ = Deepening, - = Rising)', 
                        fontsize=12, fontweight='bold', pad=20)
        
        plt.tight_layout()
        plot_file = plots_dir / "future_projections.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"  ✓ Saved: {plot_file.name}")
        
        # Save projections to CSV
        proj_df.to_csv(output_dir / "water_level_projections.csv", index=False)
        print(f"  ✓ Saved: water_level_projections.csv")
        
    except Exception as e:
        print(f"  Warning: Could not create future projections: {e}")
        import traceback
        traceback.print_exc()
    
    # --- Plot 15: Comprehensive Statistics Summary ---
    print("\n  Creating comprehensive statistics summary...")
    try:
        # Calculate comprehensive statistics for each region
        comprehensive_stats = []
        for msa in msa_order:
            region_data = msa_data[msa_data['MSA'] == msa]
            if region_data.empty:
                continue
            
            levels = region_data['lev_va'].dropna()
            annual = region_data.groupby('year')['lev_va'].mean()
            
            # Basic stats
            n_wells = region_data['site_no'].nunique()
            n_records = len(region_data)
            year_min = region_data['year'].min()
            year_max = region_data['year'].max()
            
            # Water level stats
            mean_level = levels.mean()
            median_level = levels.median()
            std_level = levels.std()
            min_level = levels.min()
            max_level = levels.max()
            p10 = levels.quantile(0.1)
            p90 = levels.quantile(0.9)
            iqr = levels.quantile(0.75) - levels.quantile(0.25)
            
            # Trend stats
            if len(annual) >= 5:
                slope, _, r_value, p_value, _ = scipy_stats.linregress(
                    np.arange(len(annual)), annual.values
                )
            else:
                slope, r_value, p_value = np.nan, np.nan, np.nan
            
            # Recent change
            if len(annual) >= 10:
                recent_5yr = annual.tail(5).mean()
                historical = annual.head(len(annual)-5).mean()
                recent_change = recent_5yr - historical
            else:
                recent_change = np.nan
            
            comprehensive_stats.append({
                'MSA': msa,
                'Wells': n_wells,
                'Records': n_records,
                'Start Year': int(year_min),
                'End Year': int(year_max),
                'Years Span': int(year_max - year_min),
                'Mean (ft)': round(mean_level, 2),
                'Median (ft)': round(median_level, 2),
                'Std Dev (ft)': round(std_level, 2),
                'Min (ft)': round(min_level, 2),
                'Max (ft)': round(max_level, 2),
                'P10 (ft)': round(p10, 2),
                'P90 (ft)': round(p90, 2),
                'IQR (ft)': round(iqr, 2),
                'Trend (ft/yr)': round(slope, 4) if pd.notna(slope) else np.nan,
                'R²': round(r_value**2, 3) if pd.notna(r_value) else np.nan,
                'Trend p-value': round(p_value, 4) if pd.notna(p_value) else np.nan,
                'Recent Change (ft)': round(recent_change, 2) if pd.notna(recent_change) else np.nan
            })
        
        stats_df = pd.DataFrame(comprehensive_stats)
        
        # Create figure with table and summary charts
        fig = plt.figure(figsize=(20, 16))
        gs = fig.add_gridspec(3, 2, height_ratios=[1.5, 1, 1], hspace=0.3, wspace=0.3)
        
        # Top: Statistics table
        ax_table = fig.add_subplot(gs[0, :])
        ax_table.axis('off')
        
        # Select key columns for display
        display_cols = ['MSA', 'Wells', 'Records', 'Years Span', 'Mean (ft)', 'Std Dev (ft)', 
                       'Trend (ft/yr)', 'R²', 'Recent Change (ft)']
        display_df = stats_df[display_cols].copy()
        
        table = ax_table.table(cellText=display_df.values,
                              colLabels=display_df.columns,
                              loc='center',
                              cellLoc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1.2, 1.6)
        
        # Style header
        for j in range(len(display_cols)):
            table[(0, j)].set_facecolor('#4472C4')
            table[(0, j)].set_text_props(color='white', fontweight='bold')
        
        # Color trend cells
        for i in range(len(display_df)):
            trend_val = display_df.iloc[i]['Trend (ft/yr)']
            if pd.notna(trend_val):
                if trend_val > 0.5:
                    table[(i+1, 6)].set_facecolor('#ffcccc')
                elif trend_val < -0.5:
                    table[(i+1, 6)].set_facecolor('#cce5ff')
        
        ax_table.set_title('COMPREHENSIVE REGIONAL GROUNDWATER STATISTICS', 
                          fontsize=14, fontweight='bold', pad=20)
        
        # Bottom left: Records vs Wells scatter
        ax = fig.add_subplot(gs[1, 0])
        scatter = ax.scatter(stats_df['Wells'], stats_df['Records'], 
                            s=stats_df['Years Span'] * 5,
                            c=stats_df['Mean (ft)'], cmap='viridis',
                            edgecolors='black', alpha=0.7)
        for _, row in stats_df.iterrows():
            ax.annotate(row['MSA'], (row['Wells'], row['Records']),
                       fontsize=8, ha='center', va='bottom')
        ax.set_title('Data Volume by Region\n(bubble size = years span, color = mean depth)', 
                    fontsize=11, fontweight='bold')
        ax.set_xlabel('Number of Wells')
        ax.set_ylabel('Number of Records')
        plt.colorbar(scatter, ax=ax, label='Mean Depth (ft)')
        ax.grid(True, alpha=0.3)
        
        # Bottom middle: Trend significance
        ax = fig.add_subplot(gs[1, 1])
        sig_df = stats_df[stats_df['Trend p-value'].notna()].copy()
        sig_df['Significant'] = sig_df['Trend p-value'] < 0.05
        colors_sig = ['#d73027' if s and t > 0 else '#4575b4' if s and t < 0 else '#999999' 
                     for s, t in zip(sig_df['Significant'], sig_df['Trend (ft/yr)'])]
        
        ax.barh(sig_df['MSA'], sig_df['Trend (ft/yr)'], color=colors_sig, edgecolor='black')
        ax.axvline(0, color='black', linewidth=1)
        ax.set_title('Long-term Trends with Statistical Significance', fontsize=11, fontweight='bold')
        ax.set_xlabel('Trend (ft/year)')
        ax.set_ylabel('Metro Region (MSA)')
        
        # Add significance markers
        for i, (_, row) in enumerate(sig_df.iterrows()):
            marker = '**' if row['Trend p-value'] < 0.01 else '*' if row['Trend p-value'] < 0.05 else ''
            ax.text(row['Trend (ft/yr)'], i, marker, va='center', fontsize=12, fontweight='bold')
        
        ax.grid(True, alpha=0.3, axis='x')
        
        # Legend
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor='#d73027', label='Significant decline (p<0.05)'),
            Patch(facecolor='#4575b4', label='Significant rise (p<0.05)'),
            Patch(facecolor='#999999', label='Not significant')
        ]
        ax.legend(handles=legend_elements, loc='lower right', fontsize=8)
        
        # Bottom: Key metrics comparison
        ax = fig.add_subplot(gs[2, :])
        
        # Normalize metrics for radar-like comparison
        metrics_to_compare = ['Wells', 'Records', 'Years Span', 'Mean (ft)', 'Std Dev (ft)']
        normalized = stats_df[['MSA'] + metrics_to_compare].copy()
        for col in metrics_to_compare:
            col_min, col_max = normalized[col].min(), normalized[col].max()
            if col_max > col_min:
                normalized[col] = (normalized[col] - col_min) / (col_max - col_min)
            else:
                normalized[col] = 0.5
        
        x = np.arange(len(metrics_to_compare))
        width = 0.8 / len(normalized)
        
        for i, (_, row) in enumerate(normalized.iterrows()):
            ax.bar(x + i * width, row[metrics_to_compare].values, width, 
                  label=row['MSA'], alpha=0.8)
        
        ax.set_xticks(x + width * len(normalized) / 2)
        ax.set_xticklabels(metrics_to_compare)
        ax.set_title('Normalized Metrics Comparison Across Regions', fontsize=11, fontweight='bold')
        ax.set_xlabel('Metric')
        ax.set_ylabel('Normalized Value (0-1)')
        ax.legend(title='MSA', bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plot_file = plots_dir / "comprehensive_statistics.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"  ✓ Saved: {plot_file.name}")
        
        # Save comprehensive statistics to CSV
        stats_df.to_csv(output_dir / "comprehensive_statistics.csv", index=False)
        print(f"  ✓ Saved: comprehensive_statistics.csv")
        
        # Print summary table to console
        print("\n" + "=" * 100)
        print("COMPREHENSIVE REGIONAL STATISTICS SUMMARY")
        print("=" * 100)
        print(stats_df[['MSA', 'Wells', 'Records', 'Years Span', 'Mean (ft)', 'Trend (ft/yr)', 'R²']].to_string(index=False))
        print("=" * 100)
        
    except Exception as e:
        print(f"  Warning: Could not create comprehensive statistics: {e}")
        import traceback
        traceback.print_exc()
    
    # -------------------------------------------------------------------------
    # Step 6: Summary Report
    # -------------------------------------------------------------------------
    print("\n" + "-" * 70)
    print("Step 6: Summary Report")
    print("-" * 70)
    
    # Calculate additional statistics
    n_records = len(data)
    n_wells = data['site_no'].nunique()
    n_zipcodes_data = data['source_zipcode'].nunique()
    
    # Date range in data
    if 'lev_dt' in data.columns:
        data_dates = pd.to_datetime(data['lev_dt'], format='mixed', errors='coerce')
        min_date = data_dates.min()
        max_date = data_dates.max()
        date_range_str = f"{min_date.strftime('%Y-%m-%d') if pd.notna(min_date) else 'N/A'} to {max_date.strftime('%Y-%m-%d') if pd.notna(max_date) else 'N/A'}"
    else:
        date_range_str = "N/A"
    
    # Get MSA regions represented
    msa_regions = csv_data['MSA'].nunique() if 'MSA' in csv_data.columns else 'N/A'
    
    print(f"""
╔══════════════════════════════════════════════════════════════════════════╗
║                          WORKFLOW COMPLETE                                ║
╠══════════════════════════════════════════════════════════════════════════╣
║  INPUT CONFIGURATION                                                      ║
║    CSV File:          {csv_file.name:<50}║
║    Metro Areas (MSA): {msa_regions:<50}║
║    Zip Codes in CSV:  {len(unique_zipcodes):<50}║
║    Buffer Radius:     {BUFFER_MILES} miles{' ' * 43}║
║    Query Date Range:  {START_DATE} to present{' ' * 29}║
║    Data Sources:      {', '.join(DATA_SOURCES):<50}║
║                                                                           ║
║  RETRIEVED DATA                                                           ║
║    Total Records:       {n_records:,}{' ' * (48 - len(f'{n_records:,}'))}║
║    Unique Wells:        {n_wells:,}{' ' * (48 - len(f'{n_wells:,}'))}║
║    Zip Codes with Data: {n_zipcodes_data:<47}║
║    Actual Date Range:   {date_range_str:<47}║
║                                                                           ║
║  VISUALIZATION (15 figures generated)                                     ║
║    1. regional_trends_by_msa.png     - Trend analysis by MSA              ║
║    2. data_quality_analysis.png      - Data quality & coverage metrics    ║
║    3. regional_distributions.png     - Water level distributions          ║
║    4. regional_temporal_patterns.png - Temporal patterns & decadal change ║
║    5. monthly_boxplots_by_region.png - Monthly seasonal patterns          ║
║    6. annual_boxplots_by_region.png  - Annual variability by region       ║
║    7. regional_correlation_clustering.png - Correlation & clustering      ║
║    8. extreme_events_analysis.png    - Extreme events & anomalies         ║
║    9. rolling_trend_analysis.png     - Rate of change analysis            ║
║   10. geographic_grouping_analysis.png - Geographic patterns              ║
║   11. regional_summary_dashboard.png - Summary scorecard                  ║
║   12. change_point_analysis.png      - Regime shift detection             ║
║   13. sustainability_index.png       - Sustainability & risk assessment   ║
║   14. future_projections.png         - Water level projections            ║
║   15. comprehensive_statistics.png   - Statistical summary                ║
║                                                                           ║
║  OUTPUT FILES                                                             ║
║    Combined Data:   {combined_file.name:<52}║
║    Per-Zipcode:     {data_per_zipcode_dir.name}/gw_data_*.parquet{' ' * 24}║
║    Wells GeoJSON:   groundwater_wells.geojson{' ' * 27}║
║    Aggregated:      monthly_aggregated.csv, annual_aggregated.csv{' ' * 7}║
║    Analysis CSVs:   sustainability_metrics.csv, projections, stats{' ' * 5}║
║    Plots:           {plots_dir.name}/*.png (15 files){' ' * 32}║
╚══════════════════════════════════════════════════════════════════════════╝
""")
    
    print(f"All output saved to: {output_dir}")
    print("\nDone!")


if __name__ == "__main__":
    main()
