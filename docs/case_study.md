# Case Study: Regional Groundwater Analysis

## Overview

This case study demonstrates a comprehensive regional groundwater analysis across nine major U.S. Metropolitan Statistical Areas (MSAs) using the pyGWRetrieval package.

## Study Parameters

| Parameter | Value |
|-----------|-------|
| **Temporal Coverage** | 1970-01-01 to 2025-11-17 (55 years) |
| **Data Source** | USGS NWIS Field Measurements (`gwlevels`) |
| **Buffer Radius** | 25 miles per zip code |
| **Metropolitan Areas** | 9 |
| **Zip Codes Analyzed** | 99 |

## Metropolitan Areas Analyzed

| MSA | Primary State(s) | Wells | Records |
|-----|------------------|-------|---------|
| New York | NY, NJ | 6,375 | 3,025,035 |
| Miami | FL | 1,718 | 1,408,186 |
| Washington | DC, VA, MD | 8,184 | 1,342,912 |
| Houston | TX | 2,283 | 651,355 |
| Boston | MA | 2,273 | 583,494 |
| Philadelphia | PA, NJ, DE | 11,177 | 496,196 |
| San Francisco | CA | 1,073 | 320,785 |
| Chicago | IL | 956 | 91,506 |
| Dallas | TX | 140 | 76,458 |

**Total: 7,995,927 records from 33,018 monitoring wells**

## Key Findings

### Regional Trends

| Region | Trend (ft/yr) | Direction | Significance |
|--------|---------------|-----------|--------------|
| Dallas | -10.64 | **Rising** | p < 0.001 |
| Chicago | -2.26 | **Rising** | p < 0.001 |
| San Francisco | -1.03 | **Rising** | p < 0.001 |
| Philadelphia | -0.23 | **Rising** | p < 0.001 |
| New York | +0.35 | Falling | p < 0.001 |
| Washington | +1.10 | **Falling** | p < 0.001 |
| Houston | +0.07 | Stable | Not significant |
| Boston | -0.003 | Stable | Not significant |
| Miami | -0.03 | Stable | p < 0.001 |

!!! success "Recovery Success Stories"
    - **Dallas**: Remarkable recovery (+10.6 ft/year rising), likely due to effective groundwater management
    - **Chicago**: Significant improvement (-2.3 ft/year rising), possibly due to reduced industrial pumping
    - **San Francisco**: Moderate recovery (-1.0 ft/year rising), reflecting drought response measures

!!! warning "Areas of Concern"
    - **Washington DC**: Only region with significant declining trend (+1.1 ft/year deepening)

### Sustainability Index

Composite sustainability scores (0-100 scale):

| Region | Index | Risk Level |
|--------|-------|------------|
| Dallas | 73.0 | Low |
| Chicago | 68.1 | Low |
| Philadelphia | 68.0 | Low |
| Miami | 66.9 | Low |
| Boston | 65.9 | Low |
| New York | 63.3 | Low |
| Houston | 62.7 | Low |
| San Francisco | 60.0 | Medium |
| Washington | 40.4 | Medium |

### Future Projections (10-Year)

| Region | Projected Change | Confidence (R²) |
|--------|------------------|-----------------|
| Dallas | -115.0 ft (rising) | 0.73 (Good) |
| Chicago | -43.5 ft (rising) | 0.73 (Good) |
| San Francisco | -16.8 ft (rising) | 0.59 (Moderate) |
| New York | +7.4 ft (deepening) | 0.56 (Moderate) |
| Washington | -5.2 ft (rising) | 0.71 (Good) |

## Generated Visualizations

The analysis produces 15 publication-ready figures:

1. **Regional Trends** (`regional_trends_by_msa.png`) - Trend analysis by MSA
2. **Data Quality** (`data_quality_analysis.png`) - Coverage and density metrics
3. **Distributions** (`regional_distributions.png`) - Statistical distributions
4. **Temporal Patterns** (`regional_temporal_patterns.png`) - Decadal changes
5. **Monthly Boxplots** (`monthly_boxplots_by_region.png`) - Seasonal patterns
6. **Annual Boxplots** (`annual_boxplots_by_region.png`) - Inter-annual variability
7. **Correlation** (`regional_correlation_clustering.png`) - Regional relationships
8. **Extreme Events** (`extreme_events_analysis.png`) - Drought analysis
9. **Rate of Change** (`rolling_trend_analysis.png`) - Trend acceleration
10. **Geographic** (`geographic_grouping_analysis.png`) - Coastal vs inland
11. **Dashboard** (`regional_summary_dashboard.png`) - Summary scorecard
12. **Change Points** (`change_point_analysis.png`) - Regime shifts
13. **Sustainability** (`sustainability_index.png`) - Risk assessment
14. **Projections** (`future_projections.png`) - Water level forecasts
15. **Statistics** (`comprehensive_statistics.png`) - Summary tables

## Output Files

| File | Description | Size |
|------|-------------|------|
| `all_groundwater_data.parquet` | Complete dataset | ~80 MB |
| `data_by_zipcode/*.parquet` | Per-zipcode files | ~130 MB |
| `groundwater_wells.geojson` | Well locations | ~25 MB |
| `sustainability_metrics.csv` | Risk scores | ~4 KB |
| `water_level_projections.csv` | Forecasts | ~4 KB |
| `comprehensive_statistics.csv` | Full statistics | ~4 KB |
| `ANALYSIS_REPORT.md` | Complete report | ~50 KB |
| `plots/*.png` | 15 figures | ~15 MB |

**Total output: ~275 MB**

## Running the Analysis

```python
# Run the full workflow
cd examples
python full_workflow_csv_zipcodes.py
```

Or programmatically:

```python
from pyGWRetrieval import GroundwaterRetrieval, TemporalAggregator

# Initialize
gw = GroundwaterRetrieval(
    start_date='1970-01-01',
    data_sources=['gwlevels']
)

# Retrieve data for multiple zip codes
data = gw.get_data_by_zipcodes_csv(
    'AirbnbMSACity_with_ZipCode.csv',
    zipcode_column='ZipCode',
    buffer_miles=25,
    parallel=True
)

# Aggregate and analyze
aggregator = TemporalAggregator(data)
annual = aggregator.to_annual()
trends = aggregator.get_trends(period='annual', parallel=True)
```

## Citation

```bibtex
@software{pyGWRetrieval,
  author = {Sayantan Majumdar},
  title = {pyGWRetrieval: Scalable Retrieval and Analysis of USGS Groundwater Level Data},
  year = {2026},
  url = {https://github.com/montimaj/pyGWRetrieval}
}
```
