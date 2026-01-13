#!/usr/bin/env python
"""
Example: Temporal Analysis with pyGWRetrieval

This script demonstrates various temporal aggregation and analysis features:
- Monthly, annual, and seasonal aggregations
- Water year calculations
- Custom period analysis
- Trend detection

Key Data Columns Used:
----------------------
- site_no: USGS well identification number
- lev_dt: Date of measurement (used for temporal grouping)
- lev_va: Water level in FEET BELOW LAND SURFACE
          - Lower values = water closer to surface (shallower)
          - Higher values = water deeper underground
          - Increasing trend = water table declining (getting deeper)
          - Decreasing trend = water table rising (getting shallower)

Aggregated Output Columns:
--------------------------
- value: Aggregated water level (mean/median/min/max of lev_va)
- count: Number of measurements in the period
- year/month: Time period identifiers
"""

from pyGWRetrieval import (
    GroundwaterRetrieval,
    TemporalAggregator,
    GroundwaterPlotter,
    setup_logging,
)
from pyGWRetrieval.temporal import aggregate_by_period
import matplotlib.pyplot as plt
import pandas as pd
import logging


def example_monthly_analysis(data: pd.DataFrame):
    """Example: Monthly aggregation and analysis."""
    
    print("\n--- Monthly Analysis ---")
    
    aggregator = TemporalAggregator(data)
    
    # Mean monthly values
    monthly_mean = aggregator.to_monthly(agg_func='mean')
    print(f"Monthly mean records: {len(monthly_mean)}")
    
    # Median monthly values (more robust to outliers)
    monthly_median = aggregator.to_monthly(agg_func='median')
    print(f"Monthly median records: {len(monthly_median)}")
    
    # Compare mean vs median for first well
    wells = monthly_mean['site_no'].unique()
    if len(wells) > 0:
        well = wells[0]
        mean_vals = monthly_mean[monthly_mean['site_no'] == well]['value']
        median_vals = monthly_median[monthly_median['site_no'] == well]['value']
        
        print(f"\nWell {well} comparison:")
        print(f"  Mean of means: {mean_vals.mean():.2f}")
        print(f"  Mean of medians: {median_vals.mean():.2f}")
        print(f"  Difference: {abs(mean_vals.mean() - median_vals.mean()):.2f}")
    
    return monthly_mean


def example_annual_analysis(data: pd.DataFrame):
    """Example: Annual aggregation including water year."""
    
    print("\n--- Annual Analysis ---")
    
    aggregator = TemporalAggregator(data)
    
    # Calendar year aggregation
    annual = aggregator.to_annual(agg_func='mean')
    print(f"Calendar year records: {len(annual)}")
    
    # Water year aggregation (Oct-Sep)
    water_year = aggregator.to_annual(water_year=True, water_year_start_month=10)
    print(f"Water year records: {len(water_year)}")
    
    # Compare calendar vs water year
    print("\nCalendar Year Summary:")
    print(annual.groupby('year')['value'].mean().tail(5))
    
    print("\nWater Year Summary:")
    print(water_year.groupby('year')['value'].mean().tail(5))
    
    return annual, water_year


def example_seasonal_analysis(data: pd.DataFrame):
    """Example: Seasonal and growing season analysis."""
    
    print("\n--- Seasonal Analysis ---")
    
    aggregator = TemporalAggregator(data)
    
    # Standard growing season (Apr-Sep)
    growing = aggregator.to_growing_season(start_month=4, end_month=9)
    print(f"Growing season (Apr-Sep) records: {len(growing)}")
    
    # Western US growing season (predefined)
    growing_west = aggregator.to_growing_season(region='western_us')
    print(f"Western US growing season records: {len(growing_west)}")
    
    # Define custom seasons
    seasons = {
        'winter': [12, 1, 2],
        'spring': [3, 4, 5],
        'summer': [6, 7, 8],
        'fall': [9, 10, 11],
    }
    
    seasonal_data = {}
    for season_name, months in seasons.items():
        seasonal = aggregator.to_custom_period(months, period_name=season_name)
        seasonal_data[season_name] = seasonal
        print(f"{season_name.capitalize()} records: {len(seasonal)}")
    
    return seasonal_data


def example_trend_analysis(data: pd.DataFrame):
    """Example: Trend analysis for wells."""
    
    print("\n--- Trend Analysis ---")
    
    aggregator = TemporalAggregator(data)
    
    # Annual trends
    annual_trends = aggregator.get_trends(period='annual')
    
    if annual_trends.empty:
        print("Insufficient data for trend analysis")
        return None
    
    print(f"\nAnalyzed trends for {len(annual_trends)} wells")
    
    # Display trend summary
    print("\nTrend Summary:")
    print(annual_trends[['site_no', 'slope', 'r_squared', 'p_value', 'trend_direction']])
    
    # Count trends
    increasing = (annual_trends['trend_direction'] == 'increasing').sum()
    decreasing = (annual_trends['trend_direction'] == 'decreasing').sum()
    
    print(f"\nTrend Direction Summary:")
    print(f"  Increasing (water table dropping): {increasing} wells")
    print(f"  Decreasing (water table rising): {decreasing} wells")
    
    # Significant trends (p < 0.05)
    significant = annual_trends[annual_trends['p_value'] < 0.05]
    print(f"\nStatistically significant trends (p<0.05): {len(significant)} wells")
    
    if not significant.empty:
        print("\nSignificant trends:")
        print(significant[['site_no', 'slope', 'r_squared', 'p_value', 'trend_direction']])
    
    return annual_trends


def example_statistics(data: pd.DataFrame):
    """Example: Comprehensive statistics calculation."""
    
    print("\n--- Statistical Analysis ---")
    
    aggregator = TemporalAggregator(data)
    
    # Per-well statistics
    well_stats = aggregator.calculate_statistics()
    print("\nPer-Well Statistics:")
    print(well_stats.head())
    
    # Overall statistics
    print(f"\nOverall Statistics:")
    print(f"  Total observations: {len(data)}")
    print(f"  Number of wells: {data['site_no'].nunique()}")
    print(f"  Mean depth: {data['lev_va'].mean():.2f}")
    print(f"  Std deviation: {data['lev_va'].std():.2f}")
    print(f"  Min depth: {data['lev_va'].min():.2f}")
    print(f"  Max depth: {data['lev_va'].max():.2f}")
    
    return well_stats


def example_resampling(data: pd.DataFrame):
    """Example: Using pandas resampling for flexible aggregation."""
    
    print("\n--- Flexible Resampling ---")
    
    aggregator = TemporalAggregator(data)
    
    # Weekly resampling
    weekly = aggregator.to_weekly()
    print(f"Weekly records: {len(weekly)}")
    
    # Quarterly resampling
    quarterly = aggregator.resample('Q')
    print(f"Quarterly records: {len(quarterly)}")
    
    # Bi-weekly resampling
    biweekly = aggregator.resample('2W')
    print(f"Bi-weekly records: {len(biweekly)}")
    
    return weekly, quarterly


def create_temporal_visualizations(
    data: pd.DataFrame,
    monthly: pd.DataFrame,
    seasonal_data: dict
):
    """Create visualizations for temporal analysis."""
    
    print("\n--- Creating Visualizations ---")
    
    # Create figure with multiple subplots
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # 1. Monthly time series
    ax1 = axes[0, 0]
    for well in monthly['site_no'].unique()[:3]:  # First 3 wells
        well_data = monthly[monthly['site_no'] == well]
        ax1.plot(well_data['date'], well_data['value'], label=well[:15], alpha=0.7)
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Depth to Water (ft)')
    ax1.set_title('Monthly Groundwater Levels')
    ax1.invert_yaxis()
    ax1.legend(fontsize=8)
    ax1.grid(True, alpha=0.3)
    
    # 2. Monthly boxplot
    ax2 = axes[0, 1]
    monthly_grouped = monthly.groupby('month')['value'].apply(list)
    ax2.boxplot([monthly_grouped.get(m, []) for m in range(1, 13)],
                labels=['J', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S', 'O', 'N', 'D'])
    ax2.set_xlabel('Month')
    ax2.set_ylabel('Depth to Water (ft)')
    ax2.set_title('Monthly Distribution')
    ax2.invert_yaxis()
    
    # 3. Seasonal comparison
    ax3 = axes[1, 0]
    seasons = ['winter', 'spring', 'summer', 'fall']
    colors = ['blue', 'green', 'red', 'orange']
    positions = range(len(seasons))
    
    seasonal_means = []
    for season in seasons:
        if season in seasonal_data:
            seasonal_means.append(seasonal_data[season]['value'].mean())
        else:
            seasonal_means.append(0)
    
    ax3.bar(positions, seasonal_means, color=colors, alpha=0.7)
    ax3.set_xticks(positions)
    ax3.set_xticklabels([s.capitalize() for s in seasons])
    ax3.set_ylabel('Mean Depth to Water (ft)')
    ax3.set_title('Seasonal Average Depth')
    ax3.invert_yaxis()
    
    # 4. Annual trend
    ax4 = axes[1, 1]
    annual = monthly.groupby(['site_no', 'year'])['value'].mean().reset_index()
    annual_mean = annual.groupby('year')['value'].mean()
    ax4.plot(annual_mean.index, annual_mean.values, 'b-o', linewidth=2)
    ax4.fill_between(annual_mean.index, annual_mean.values, alpha=0.3)
    ax4.set_xlabel('Year')
    ax4.set_ylabel('Mean Depth to Water (ft)')
    ax4.set_title('Annual Average Depth')
    ax4.invert_yaxis()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('output/temporal_analysis.png', dpi=150, bbox_inches='tight')
    print("Saved: output/temporal_analysis.png")
    plt.close()


def main():
    """Run all temporal analysis examples."""
    
    setup_logging(level=logging.INFO)
    
    print("=" * 60)
    print("pyGWRetrieval - Temporal Analysis Example")
    print("=" * 60)
    
    # Retrieve data
    print("\n--- Retrieving Data ---")
    gw = GroundwaterRetrieval(
        start_date='2010-01-01',
        end_date='2023-12-31'
    )
    
    data = gw.get_data_by_zipcode('89701', buffer_miles=20)
    
    if data.empty:
        print("No data retrieved. Please try a different location.")
        return
    
    print(f"Retrieved {len(data)} records from {data['site_no'].nunique()} wells")
    
    # Run examples
    monthly = example_monthly_analysis(data)
    annual, water_year = example_annual_analysis(data)
    seasonal_data = example_seasonal_analysis(data)
    trends = example_trend_analysis(data)
    stats = example_statistics(data)
    weekly, quarterly = example_resampling(data)
    
    # Create visualizations
    create_temporal_visualizations(data, monthly, seasonal_data)
    
    # Save aggregated data
    print("\n--- Saving Aggregated Data ---")
    monthly.to_csv('output/monthly_data.csv', index=False)
    annual.to_csv('output/annual_data.csv', index=False)
    water_year.to_csv('output/water_year_data.csv', index=False)
    
    if trends is not None:
        trends.to_csv('output/trend_analysis.csv', index=False)
    
    print("\nSaved files:")
    print("  - output/monthly_data.csv")
    print("  - output/annual_data.csv")
    print("  - output/water_year_data.csv")
    print("  - output/trend_analysis.csv")
    print("  - output/temporal_analysis.png")
    
    print("\n" + "=" * 60)
    print("Temporal analysis complete!")
    print("=" * 60)


if __name__ == '__main__':
    import os
    os.makedirs('output', exist_ok=True)
    
    main()
