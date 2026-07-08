#!/usr/bin/env python
"""
Example: Basic Usage of pyGWRetrieval

This script demonstrates the basic functionality of the pyGWRetrieval package
for retrieving and analyzing groundwater level data from USGS NWIS.

USGS Data Columns Retrieved:
----------------------------
- site_no: USGS site identification number
- lev_dt: Date of water level measurement (YYYY-MM-DD)
- lev_tm: Time of measurement (HH:MM)
- lev_va: Water level value in FEET BELOW LAND SURFACE
          (lower values = shallower water, higher values = deeper water)
- lev_acy_cd: Water level accuracy code
- lev_src_cd: Source of water level data
- lev_meth_cd: Method of measurement code (S=steel tape, E=electric tape, etc.)
- lev_status_cd: Status of the site at time of measurement
- station_nm: Station name
- dec_lat_va: Decimal latitude (degrees)
- dec_long_va: Decimal longitude (degrees)
"""

from pyGWRetrieval import (
    GroundwaterRetrieval,
    TemporalAggregator,
    GroundwaterPlotter,
    setup_logging,
)
import matplotlib.pyplot as plt
import logging


def main():
    """Main function demonstrating basic pyGWRetrieval usage."""
    
    # Set up logging to see progress
    setup_logging(level=logging.INFO)
    
    print("=" * 60)
    print("pyGWRetrieval - Basic Usage Example")
    print("=" * 60)
    
    # -------------------------------------------------------------------------
    # Example 1: Query by Zip Code
    # -------------------------------------------------------------------------
    print("\n--- Example 1: Query by Zip Code ---")
    
    # Create a retrieval instance with a date range
    gw = GroundwaterRetrieval(
        start_date='2015-01-01',
        end_date='2023-12-31'
    )
    
    # Get groundwater data within 15 miles of Carson City, NV (89701)
    # This includes the capital city and surrounding areas
    data = gw.get_data_by_zipcode('89701', buffer_miles=15)
    
    if data.empty:
        print("No data retrieved. Try a different location or date range.")
        return
    
    # Display summary
    print(f"\nRetrieved {len(data)} records from {data['site_no'].nunique()} wells")
    
    # Get data summary
    summary = gw.get_data_summary()
    print(f"Date range: {summary['date_range']['start']} to {summary['date_range']['end']}")
    
    # View first few records
    # Key columns:
    #   site_no: USGS well ID
    #   lev_dt: Measurement date
    #   lev_va: Depth to water (feet below land surface)
    print("\nFirst few records:")
    print(data[['site_no', 'lev_dt', 'lev_va']].head(10))
    
    # -------------------------------------------------------------------------
    # Example 2: Temporal Aggregation
    # -------------------------------------------------------------------------
    print("\n--- Example 2: Temporal Aggregation ---")
    
    # Create temporal aggregator
    aggregator = TemporalAggregator(data)
    
    # Monthly aggregation
    monthly = aggregator.to_monthly(agg_func='mean')
    print(f"\nMonthly records: {len(monthly)}")
    print(monthly.head())
    
    # Annual aggregation
    annual = aggregator.to_annual(agg_func='mean')
    print(f"\nAnnual records: {len(annual)}")
    print(annual.head())
    
    # Water year aggregation
    water_year = aggregator.to_annual(water_year=True)
    print(f"\nWater year records: {len(water_year)}")
    print(water_year.head())
    
    # Growing season (April - September)
    growing = aggregator.to_growing_season(start_month=4, end_month=9)
    print(f"\nGrowing season records: {len(growing)}")
    print(growing.head())
    
    # -------------------------------------------------------------------------
    # Example 3: Basic Visualization
    # -------------------------------------------------------------------------
    print("\n--- Example 3: Basic Visualization ---")
    
    # Create plotter
    plotter = GroundwaterPlotter(data)
    
    # Plot time series for all wells (up to 10)
    fig1 = plotter.plot_time_series(
        title='Groundwater Levels - Carson City Area'
    )
    plt.savefig('output/time_series.png', dpi=300, bbox_inches='tight')
    print("Saved: output/time_series.png")
    plt.close()
    
    # Plot single well if we have data
    wells = data['site_no'].unique()
    if len(wells) > 0:
        fig2 = plotter.plot_single_well(
            wells[0],
            show_trend=True,
            show_stats=True
        )
        plt.savefig('output/single_well.png', dpi=300, bbox_inches='tight')
        print("Saved: output/single_well.png")
        plt.close()
    
    # Monthly boxplot
    fig3 = plotter.plot_monthly_boxplot()
    plt.savefig('output/monthly_boxplot.png', dpi=300, bbox_inches='tight')
    print("Saved: output/monthly_boxplot.png")
    plt.close()
    
    # Annual summary
    fig4 = plotter.plot_annual_summary()
    plt.savefig('output/annual_summary.png', dpi=300, bbox_inches='tight')
    print("Saved: output/annual_summary.png")
    plt.close()
    
    # -------------------------------------------------------------------------
    # Example 4: Spatial Map Visualization
    # -------------------------------------------------------------------------
    print("\n--- Example 4: Spatial Map Visualization ---")
    
    # Create a spatial map showing wells colored by water level
    # For a single zip code, this will automatically zoom to show local detail
    try:
        from pyGWRetrieval import plot_wells_map
        
        # Plot wells on a map with basemap
        # - Auto-zoom adjusts based on spatial extent
        # - Colors show mean water level (red=deep, blue=shallow)
        fig5 = plot_wells_map(
            data,
            agg_func='mean',  # Show mean water level per well
            title='Groundwater Wells - Carson City Area\n(colored by mean water level, ft below surface)',
            cmap='RdYlBu_r',  # Red=deep water, Blue=shallow
            add_basemap=True  # Add OpenStreetMap-style basemap
        )
        plt.savefig('output/spatial_map.png', dpi=300, bbox_inches='tight')
        print("Saved: output/spatial_map.png")
        plt.close()
    except ImportError:
        print("Note: Spatial maps require contextily. Install with: pip install contextily")
    except Exception as e:
        print(f"Could not create spatial map: {e}")
    
    # -------------------------------------------------------------------------
    # Example 5: Export Data
    # -------------------------------------------------------------------------
    print("\n--- Example 5: Export Data ---")
    
    # Export raw data to CSV
    gw.to_csv('output/groundwater_raw.csv')
    print("Saved: output/groundwater_raw.csv")
    
    # Export monthly data
    monthly.to_csv('output/groundwater_monthly.csv', index=False)
    print("Saved: output/groundwater_monthly.csv")
    
    # Export annual data
    annual.to_csv('output/groundwater_annual.csv', index=False)
    print("Saved: output/groundwater_annual.csv")
    
    # Export wells as GeoJSON
    gw.save_wells_to_file('output/wells.geojson')
    print("Saved: output/wells.geojson")
    
    # -------------------------------------------------------------------------
    # Example 6: Statistics and Trend Analysis
    # -------------------------------------------------------------------------
    print("\n--- Example 6: Statistics and Trend Analysis ---")
    
    # Calculate statistics
    stats = aggregator.calculate_statistics()
    print("\nWell Statistics:")
    print(stats.head())
    
    # Calculate trends
    trends = aggregator.get_trends(period='annual')
    print("\nTrend Analysis:")
    print(trends[['site_no', 'slope', 'r_squared', 'p_value', 'trend_direction']].head())
    
    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("=" * 60)
    print("\nOutput files created in 'output/' directory:")
    print("  - time_series.png")
    print("  - single_well.png")
    print("  - monthly_boxplot.png")
    print("  - annual_summary.png")
    print("  - spatial_map.png")
    print("  - groundwater_raw.csv")
    print("  - groundwater_monthly.csv")
    print("  - groundwater_annual.csv")
    print("  - wells.geojson")


if __name__ == '__main__':
    # Create output directory
    import os
    os.makedirs('output', exist_ok=True)
    
    main()
