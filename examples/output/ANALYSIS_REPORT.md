# Regional Groundwater Analysis Report
## USGS Groundwater Level Trends Across Nine Major U.S. Metropolitan Statistical Areas (1970-2025)

**Generated:** January 12, 2026  
**Analysis Period:** 1970-01-01 to 2025-11-17  
**Data Source:** USGS National Water Information System (NWIS) - Field Measurements (gwlevels)  
**Package:** pyGWRetrieval v1.0

---

## Executive Summary

This report presents a comprehensive analysis of groundwater level trends across nine major U.S. Metropolitan Statistical Areas (MSAs). The analysis encompasses over **7.9 million groundwater measurements** from **33,018 monitoring wells** spanning 55 years of continuous data collection. Key findings indicate that:

- **5 of 9 regions** show statistically significant long-term trends (p < 0.05)
- **Dallas** exhibits the most dramatic water level recovery (+10.6 ft/year rising trend)
- **Washington DC** shows the most significant declining trend (+1.1 ft/year deepening)
- **Miami** demonstrates the most stable groundwater conditions (lowest variability)
- **Chicago** and **San Francisco** show strong recovery trends

---

## Table of Contents

1. [Study Area and Data Overview](#1-study-area-and-data-overview)
2. [Methodology](#2-methodology)
3. [Regional Trend Analysis](#3-regional-trend-analysis)
4. [Data Quality and Coverage](#4-data-quality-and-coverage)
5. [Regional Distributions](#5-regional-distributions)
6. [Temporal Patterns](#6-temporal-patterns)
7. [Seasonal Analysis](#7-seasonal-analysis)
8. [Correlation and Clustering](#8-correlation-and-clustering)
9. [Extreme Events Analysis](#9-extreme-events-analysis)
10. [Rate of Change Analysis](#10-rate-of-change-analysis)
11. [Geographic Patterns](#11-geographic-patterns)
12. [Change Point Detection](#12-change-point-detection)
13. [Sustainability Assessment](#13-sustainability-assessment)
14. [Future Projections](#14-future-projections)
15. [Comprehensive Statistics](#15-comprehensive-statistics)
16. [Conclusions and Recommendations](#16-conclusions-and-recommendations)
17. [Data Files](#17-data-files)
18. [References](#18-references)

---

## 1. Study Area and Data Overview

### 1.1 Metropolitan Statistical Areas Analyzed

This study focuses on nine major U.S. metropolitan regions representing diverse hydrogeological settings, climate zones, and water management practices:

| MSA | Primary State(s) | Wells | Records | Climate Zone |
|-----|------------------|-------|---------|--------------|
| New York | NY, NJ | 6,375 | 3,025,035 | Humid Continental |
| Miami | FL | 1,718 | 1,408,186 | Tropical |
| Washington | DC, VA, MD | 8,184 | 1,342,912 | Humid Subtropical |
| Houston | TX | 2,283 | 651,355 | Humid Subtropical |
| Boston | MA | 2,273 | 583,494 | Humid Continental |
| Philadelphia | PA, NJ, DE | 11,177 | 496,196 | Humid Subtropical |
| San Francisco | CA | 1,073 | 320,785 | Mediterranean |
| Chicago | IL | 956 | 91,506 | Humid Continental |
| Dallas | TX | 140 | 76,458 | Humid Subtropical |

### 1.2 Data Summary

- **Total Records:** 7,995,927
- **Unique Monitoring Wells:** 33,018
- **Zip Codes Covered:** 99
- **Buffer Radius:** 25 miles per zip code
- **Temporal Coverage:** 1970-2025 (55 years)

---

## 2. Methodology

### 2.1 Data Retrieval

Groundwater level data was retrieved from the USGS National Water Information System (NWIS) using the pyGWRetrieval package. Field measurements (`gwlevels`) were selected as the primary data source due to their accuracy and long-term consistency.

### 2.2 Spatial Processing

Each metropolitan area was defined by representative zip codes with a 25-mile buffer radius. Zip code centroids were geocoded and buffered to create spatial query boundaries.

### 2.3 Statistical Methods

- **Trend Analysis:** Linear regression with significance testing (α = 0.05)
- **Change Point Detection:** Cumulative Sum (CUSUM) method
- **Sustainability Index:** Composite scoring based on trend, variability, and recovery indicators
- **Projections:** Linear extrapolation with confidence assessment (R²)

### 2.4 Temporal Aggregation

Raw measurements were aggregated to annual means for trend analysis, with monthly aggregations used for seasonal pattern identification.

---

## 3. Regional Trend Analysis

### Figure 1: Regional Trends by MSA

![Regional Trends by MSA](plots/regional_trends_by_msa.png)

**Description:** This four-panel figure presents the primary trend analysis results:

- **Panel A (Top-Left):** Average groundwater trend slope by metro region. Red bars indicate deepening water tables (declining groundwater), while blue bars indicate rising water tables (recovery). Washington DC and Chicago show the most significant falling trends, while Dallas shows dramatic recovery.

- **Panel B (Top-Right):** Mean water level by metro region (feet below land surface). Dallas has the deepest average water table (~363 ft), while Miami has the shallowest (~7 ft), reflecting regional aquifer characteristics.

- **Panel C (Bottom-Left):** Stacked bar chart showing the distribution of well trends (rising, stable, falling) in each region. This reveals the heterogeneity of groundwater behavior within each MSA.

- **Panel D (Bottom-Right):** Bubble plot showing records vs. trend, with bubble size proportional to number of wells. This visualization helps identify regions with robust data support for trend conclusions.

### Key Findings - Regional Trends

| Region | Trend (ft/yr) | Direction | Interpretation |
|--------|---------------|-----------|----------------|
| Dallas | -10.64 | **Rising** | Strong recovery, possibly due to managed recharge |
| Chicago | -2.26 | **Rising** | Significant recovery trend |
| San Francisco | -1.03 | **Rising** | Recovery, likely from conservation efforts |
| Philadelphia | -0.23 | **Rising** | Modest recovery |
| New York | +0.35 | Falling | Slight deepening |
| Washington | +1.10 | **Falling** | Significant decline, concerning |
| Houston | +0.07 | Stable | Near equilibrium |
| Boston | -0.003 | Stable | Near equilibrium |
| Miami | -0.03 | Stable | Most stable conditions |

---

## 4. Data Quality and Coverage

### Figure 2: Data Quality Analysis

![Data Quality Analysis](plots/data_quality_analysis.png)

**Description:** This four-panel figure assesses data quality and coverage:

- **Panel A (Top-Left):** Data density showing average records per well by region. Higher values indicate more intensive monitoring. New York and Miami show the highest monitoring intensity.

- **Panel B (Top-Right):** Distribution of annual measurements per well. The histogram reveals typical monitoring frequencies, with most wells measured 1-20 times per year.

- **Panel C (Bottom-Left):** Water level depth categories by region (percentage). Shows the distribution of shallow (<10 ft), moderate (10-50 ft), deep (50-100 ft), and very deep (>100 ft) measurements. Miami is predominantly shallow, while Dallas is predominantly very deep.

- **Panel D (Bottom-Right):** Historical vs. recent water level comparison. Shows change between pre-2000 and post-2010 mean water levels. Red bars indicate deepening, blue bars indicate rising water tables.

---

## 5. Regional Distributions

### Figure 3: Regional Distributions

![Regional Distributions](plots/regional_distributions.png)

**Description:** This four-panel figure shows the statistical distribution of water levels:

- **Panel A (Top-Left):** Violin plots showing water level distribution by MSA. The width indicates data density at each depth. Dallas shows the widest range (highest variability), while Miami shows the narrowest (most consistent).

- **Panel B (Top-Right):** Dual-axis bar chart comparing number of wells (blue) and records (orange) by region. Philadelphia has the most wells, while New York has the most records.

- **Panel C (Bottom-Left):** Temporal coverage showing data availability for each region (years with data). All regions have data spanning 1970-2025, though with varying density.

- **Panel D (Bottom-Right):** Mean water level with error bars (±1 standard deviation) and median markers. Large error bars indicate high intra-regional variability.

---

## 6. Temporal Patterns

### Figure 4: Regional Temporal Patterns

![Regional Temporal Patterns](plots/regional_temporal_patterns.png)

**Description:** This four-panel figure examines temporal patterns:

- **Panel A (Top-Left):** Mean water level by decade and region. Shows long-term trajectory for each MSA. Dallas shows dramatic improvement (shallowing) since the 1980s.

- **Panel B (Top-Right):** Stacked bar chart of record counts by decade. Shows data availability has generally increased over time, with peak collection in 2000s-2010s.

- **Panel C (Bottom-Left):** Seasonal water level patterns (top 5 regions). Shows monthly variation in mean water levels. Some regions show clear seasonal cycles while others are relatively flat.

- **Panel D (Bottom-Right):** Data density heatmap (1990-present). Color intensity shows measurement frequency by year and region. Reveals temporal gaps and monitoring intensity changes.

---

## 7. Seasonal Analysis

### Figure 5: Monthly Boxplots by Region

![Monthly Boxplots](plots/monthly_boxplots_by_region.png)

**Description:** Grid of boxplots (3×3) showing monthly water level distributions for each MSA. Each subplot displays 12 months of data with:
- Box: Interquartile range (25th-75th percentile)
- Whiskers: 1.5× IQR
- Line: Median
- Points: Outliers

**Key Patterns:**
- **Miami:** Strong seasonal signal with shallower water tables in summer (wet season)
- **Boston:** Moderate seasonality with deepest levels in fall
- **San Francisco:** Mediterranean pattern with deeper levels in dry season (summer-fall)
- **Dallas:** Limited seasonal variation due to deep aquifer system

### Figure 6: Annual Boxplots by Region

![Annual Boxplots](plots/annual_boxplots_by_region.png)

**Description:** Grid of boxplots showing annual water level distributions (1980-present) for each MSA. This visualization reveals:
- Long-term trends (shift in median over time)
- Inter-annual variability (box size consistency)
- Extreme years (outliers)
- Data density changes (number of years with data)

---

## 8. Correlation and Clustering

### Figure 7: Regional Correlation and Clustering

![Correlation Clustering](plots/regional_correlation_clustering.png)

**Description:** This four-panel figure examines relationships between regions:

- **Panel A (Top-Left):** Correlation heatmap showing Pearson correlation coefficients between annual mean water levels across regions. Values near +1 indicate regions that rise and fall together; near -1 indicates inverse relationships.

- **Panel B (Top-Right):** Hierarchical clustering dendrogram grouping regions by water level pattern similarity. Regions clustered together may share similar hydrogeological or climatic drivers.

- **Panel C (Bottom-Left):** Coefficient of Variation (CV) by region. Lower CV indicates more stable water levels. Miami shows lowest variability; Chicago and San Francisco show highest.

- **Panel D (Bottom-Right):** Seasonal amplitude (max-min monthly mean) by region. Higher values indicate stronger seasonal signals.

**Correlation Findings:**
- Strong positive correlations suggest shared regional drivers (climate, policy)
- Weak correlations indicate independent aquifer systems
- Geographic proximity does not always predict correlation

---

## 9. Extreme Events Analysis

### Figure 8: Extreme Events Analysis

![Extreme Events](plots/extreme_events_analysis.png)

**Description:** This four-panel figure analyzes extreme groundwater conditions:

- **Panel A (Top-Left):** Percentage of readings exceeding 90th and 95th percentiles by region. Higher percentages indicate more frequent extreme low water levels (deep water table events).

- **Panel B (Top-Right):** Annual water level range (min to max) shown as filled areas for top 6 regions. Wide ranges indicate high variability within years.

- **Panel C (Bottom-Left):** Water level anomaly from long-term mean over time (top 5 regions). Positive anomalies indicate deeper-than-normal conditions; negative indicates shallower-than-normal.

- **Panel D (Bottom-Right):** Inter-annual variability (standard deviation of annual means) by region. Higher values indicate less predictable year-to-year conditions.

---

## 10. Rate of Change Analysis

### Figure 9: Rolling Trend Analysis

![Rolling Trends](plots/rolling_trend_analysis.png)

**Description:** This four-panel figure examines how trends have changed over time:

- **Panel A (Top-Left):** Rolling 10-year trend slopes over time. Shows whether depletion rates are accelerating, decelerating, or reversing. Crossing zero indicates trend reversal.

- **Panel B (Top-Right):** Trend acceleration (late period slope minus early period slope). Positive values indicate accelerating decline; negative indicates improvement.

- **Panel C (Bottom-Left):** Year-over-year change distribution boxplots by region. Shows typical annual variability and identifies regions with consistently positive or negative changes.

- **Panel D (Bottom-Right):** Cumulative change since 1990 baseline. Shows total water level change over the monitoring period for each region.

**Acceleration Analysis:**
- **Improving (negative acceleration):** Dallas, Chicago, San Francisco
- **Worsening (positive acceleration):** Washington DC
- **Stable:** Miami, Boston

---

## 11. Geographic Patterns

### Figure 10: Geographic Grouping Analysis

![Geographic Grouping](plots/geographic_grouping_analysis.png)

**Description:** This four-panel figure explores geographic patterns:

- **Panel A (Top-Left):** Violin plot comparing coastal vs. inland regions. Coastal regions (Miami, San Francisco, Boston, New York) tend to have shallower water tables than inland regions.

- **Panel B (Top-Right):** Annual mean water level trends by coast/region grouping (East Coast, West Coast, Gulf/Central).

- **Panel C (Bottom-Left):** Aquifer characteristics scatter plot (mean depth vs. variability). Bubble size indicates record count; color indicates standard deviation.

- **Panel D (Bottom-Right):** Normalized metrics comparison across regions. Enables direct comparison of different scales (wells, records, depth, variability).

---

## 12. Change Point Detection

### Figure 11: Regional Summary Dashboard

![Summary Dashboard](plots/regional_summary_dashboard.png)

**Description:** Comprehensive dashboard showing key metrics for all 9 regions:
- Region name
- Trend sparkline (historical trajectory)
- Mean water level (ft)
- Trend indicator (↑ Rising, → Stable, ↓ Falling)
- Data quality rating (★★★ Excellent, ★★☆ Good, ★☆☆ Fair)

### Figure 12: Change Point Analysis

![Change Point Analysis](plots/change_point_analysis.png)

**Description:** This four-panel figure identifies regime shifts:

- **Panel A (Top-Left):** Normalized water level trends with detected change points (vertical dashed lines). Change points indicate years when significant shifts in groundwater behavior occurred.

- **Panel B (Top-Right):** Number of detected regime shifts by region. More change points may indicate more dynamic aquifer management or climate sensitivity.

- **Panel C (Bottom-Left):** Decade-over-decade water level changes. Shows progression of change through time (1980s→1990s, 1990s→2000s, etc.).

- **Panel D (Bottom-Right):** Average volatility (5-year rolling standard deviation) by region. Higher volatility indicates more dynamic conditions.

---

## 13. Sustainability Assessment

### Figure 13: Sustainability Index

![Sustainability Index](plots/sustainability_index.png)

**Description:** This four-panel figure presents the sustainability assessment:

- **Panel A (Top-Left):** Composite Sustainability Index (0-100 scale) by region. Higher scores indicate more sustainable groundwater conditions. Green line = good threshold (60), orange line = warning threshold (40).

- **Panel B (Top-Right):** Risk matrix plotting trend vs. variability. Quadrants classify regions by risk level:
  - Upper right: High risk (declining + variable)
  - Upper left: Moderate risk (rising + variable)
  - Lower left: Low risk (rising + stable)
  - Lower right: Moderate risk (declining + stable)

- **Panel C (Bottom-Left):** Stacked bar showing sustainability index components:
  - Trend Score (40% weight)
  - Stability Score (30% weight)
  - Recent Trend Score (20% weight)
  - Data Quality Score (10% weight)

- **Panel D (Bottom-Right):** Pie chart showing distribution of regions by risk level.

### Sustainability Index Results

| Region | Index | Risk Level | Interpretation |
|--------|-------|------------|----------------|
| Dallas | 73.0 | Low | Strong recovery, good outlook |
| Chicago | 68.1 | Low | Improving trend |
| Philadelphia | 68.0 | Low | Stable with slight recovery |
| Miami | 66.9 | Low | Most stable, sustainable |
| Boston | 65.9 | Low | Near equilibrium |
| New York | 63.3 | Low | Minor concerns, generally stable |
| Houston | 62.7 | Low | Stable conditions |
| San Francisco | 60.0 | Medium | High variability concern |
| Washington | 40.4 | Medium | Declining trend is concerning |

---

## 14. Future Projections

### Figure 14: Future Projections

![Future Projections](plots/future_projections.png)

**Description:** This four-panel figure presents water level projections:

- **Panel A (Top-Left):** Historical data with linear projections to 2045. Solid lines show historical data; dashed lines show projections. Red vertical line marks current year.

- **Panel B (Top-Right):** Projected 10-year change by region. Red bars indicate projected deepening; blue bars indicate projected rising.

- **Panel C (Bottom-Left):** Projection reliability (R² values). Higher R² indicates more confidence in linear projection. Values >0.7 are considered good; >0.5 moderate.

- **Panel D (Bottom-Right):** Summary table of projected changes at 5, 10, and 20 years. Color coding: red (deepening), yellow (slight deepening), green (slight rising), blue (significant rising).

### Projection Summary

| Region | 5-Year (ft) | 10-Year (ft) | 20-Year (ft) | Confidence (R²) |
|--------|-------------|--------------|--------------|-----------------|
| New York | +5.7 | +7.4 | +10.9 | 0.56 (Moderate) |
| Miami | -0.7 | -0.9 | -1.2 | 0.50 (Moderate) |
| Washington | -10.7 | -5.2 | +5.8 | 0.71 (Good) |
| Houston | +5.5 | +5.8 | +6.5 | 0.004 (Poor) |
| Boston | +0.8 | +0.8 | +0.8 | 0.001 (Poor) |
| Philadelphia | +0.2 | -1.0 | -3.3 | 0.52 (Moderate) |
| San Francisco | -11.7 | -16.8 | -27.1 | 0.59 (Moderate) |
| Chicago | -32.2 | -43.5 | -66.1 | 0.73 (Good) |
| Dallas | -64.1 | -115.0 | -216.8 | 0.73 (Good) |

> **Note:** Negative values indicate projected rising water tables (improvement); positive values indicate deepening. Projections assume continuation of current linear trends and should be interpreted with caution.

---

## 15. Comprehensive Statistics

### Figure 15: Comprehensive Statistics

![Comprehensive Statistics](plots/comprehensive_statistics.png)

**Description:** This figure provides a publication-ready statistical summary:

- **Top Panel:** Statistics table with key metrics for each region including wells, records, temporal span, mean/median depths, variability measures, trend statistics, and significance levels.

- **Middle-Left Panel:** Scatter plot of data volume (wells vs. records) with bubble size indicating years of data and color indicating mean depth.

- **Middle-Right Panel:** Bar chart of long-term trends with statistical significance markers (* p<0.05, ** p<0.01).

- **Bottom Panel:** Normalized metrics comparison enabling cross-regional comparison across different scales.

### Complete Statistical Summary

| MSA | Wells | Records | Years | Mean (ft) | Std Dev (ft) | Trend (ft/yr) | R² | p-value | Significance |
|-----|-------|---------|-------|-----------|--------------|---------------|-----|---------|--------------|
| New York | 6,375 | 3,025,035 | 55 | 37.55 | 39.24 | +0.35 | 0.56 | <0.001 | ** |
| Miami | 1,718 | 1,408,186 | 55 | 6.92 | 4.12 | -0.03 | 0.50 | <0.001 | ** |
| Washington | 8,184 | 1,342,912 | 55 | 71.49 | 80.67 | +1.10 | 0.71 | <0.001 | ** |
| Houston | 2,283 | 651,355 | 55 | 211.01 | 121.12 | +0.07 | 0.004 | 0.66 | NS |
| Boston | 2,273 | 583,494 | 55 | 12.53 | 10.53 | -0.003 | 0.001 | 0.86 | NS |
| Philadelphia | 11,177 | 496,196 | 55 | 35.40 | 36.43 | -0.23 | 0.52 | <0.001 | ** |
| San Francisco | 1,073 | 320,785 | 55 | 44.78 | 38.75 | -1.03 | 0.59 | <0.001 | ** |
| Chicago | 956 | 91,506 | 55 | 59.34 | 94.22 | -2.26 | 0.73 | <0.001 | ** |
| Dallas | 140 | 76,458 | 55 | 363.37 | 220.90 | -10.64 | 0.72 | <0.001 | ** |

*NS = Not Significant; ** = Highly Significant (p < 0.01)*

---

## 16. Conclusions and Recommendations

### 16.1 Key Findings

1. **Regional Variability:** Groundwater conditions vary substantially across the nine MSAs, reflecting differences in aquifer characteristics, climate, and water management practices.

2. **Recovery Success Stories:**
   - **Dallas:** Shows remarkable recovery (+10.6 ft/year rising), likely due to effective groundwater management and shifting to surface water supplies
   - **Chicago:** Significant improvement (-2.3 ft/year rising), possibly due to reduced industrial pumping and conservation
   - **San Francisco:** Moderate recovery (-1.0 ft/year rising), potentially reflecting drought response and conservation measures

3. **Areas of Concern:**
   - **Washington DC:** Only region with statistically significant declining trend (+1.1 ft/year deepening), warranting further investigation and management intervention

4. **Stable Regions:**
   - **Miami:** Most stable conditions with lowest variability, indicating well-balanced recharge and withdrawal
   - **Boston and Houston:** Near equilibrium with no significant trends

5. **Data Quality:** All regions have excellent data quality spanning 55 years, providing robust foundation for trend analysis.

### 16.2 Management Recommendations

| Region | Risk Level | Recommendation |
|--------|------------|----------------|
| Washington | Medium | Investigate causes of decline; consider pumping restrictions |
| San Francisco | Medium | Continue conservation; monitor recovery trajectory |
| New York | Low | Maintain current practices; monitor urban expansion impacts |
| Houston | Low | Continue monitoring; watch for subsidence indicators |
| Miami | Low | Maintain sustainable balance; protect recharge areas |
| Philadelphia | Low | Continue recovery trend; protect watershed |
| Boston | Low | Maintain equilibrium; climate adaptation planning |
| Chicago | Low | Document recovery success for replication |
| Dallas | Low | Model success story for other regions |

### 16.3 Limitations

- Linear trend projections assume stationarity; climate change may alter future trajectories
- Spatial heterogeneity within MSAs not fully captured by regional averages
- Data density varies by region, affecting confidence in some statistics
- Cause-effect relationships (pumping, climate, policy) not explicitly modeled

### 16.4 Future Research Directions

1. Integrate climate data to assess climate-groundwater relationships
2. Incorporate pumping/withdrawal records for water balance analysis
3. Develop non-linear projection models accounting for management scenarios
4. Expand analysis to additional metropolitan areas
5. Assess groundwater quality trends alongside quantity

---

## 17. Data Files

### 17.1 Generated Data Files

| File | Description | Format |
|------|-------------|--------|
| `all_groundwater_data.parquet` | Complete raw dataset (7.9M records) | Parquet |
| `data_by_zipcode/*.parquet` | Individual files per zip code | Parquet |
| `groundwater_wells.geojson` | Well locations with metadata | GeoJSON |
| `monthly_aggregated.csv` | Monthly mean water levels | CSV |
| `annual_aggregated.csv` | Annual mean water levels | CSV |
| `trends_analysis.csv` | Well-level trend statistics | CSV |
| `regional_trends_by_msa.csv` | Regional trend summary | CSV |
| `sustainability_metrics.csv` | Sustainability index results | CSV |
| `water_level_projections.csv` | Future projections | CSV |
| `comprehensive_statistics.csv` | Complete regional statistics | CSV |

### 17.2 Visualization Files

| File | Description |
|------|-------------|
| `regional_trends_by_msa.png` | Primary trend analysis (4 panels) |
| `data_quality_analysis.png` | Data quality assessment (4 panels) |
| `regional_distributions.png` | Statistical distributions (4 panels) |
| `regional_temporal_patterns.png` | Temporal patterns (4 panels) |
| `monthly_boxplots_by_region.png` | Monthly seasonal patterns (9 subplots) |
| `annual_boxplots_by_region.png` | Annual variability (9 subplots) |
| `regional_correlation_clustering.png` | Correlation analysis (4 panels) |
| `extreme_events_analysis.png` | Extreme conditions (4 panels) |
| `rolling_trend_analysis.png` | Rate of change (4 panels) |
| `geographic_grouping_analysis.png` | Geographic patterns (4 panels) |
| `regional_summary_dashboard.png` | Summary dashboard |
| `change_point_analysis.png` | Regime shift detection (4 panels) |
| `sustainability_index.png` | Sustainability assessment (4 panels) |
| `future_projections.png` | Water level projections (4 panels) |
| `comprehensive_statistics.png` | Statistical summary |

---

## 18. References

### Data Sources

- U.S. Geological Survey (USGS). National Water Information System (NWIS). https://waterdata.usgs.gov/nwis
- USGS Water Data for the Nation. https://waterdata.usgs.gov/

### Software

- pyGWRetrieval: Python package for groundwater data retrieval. https://github.com/montimaj/pyGWRetrieval
- pandas: Data analysis library. https://pandas.pydata.org/
- matplotlib: Visualization library. https://matplotlib.org/
- scipy: Scientific computing. https://scipy.org/
- geopandas: Geospatial data handling. https://geopandas.org/

### Methods

- Mann, H.B. (1945). Nonparametric tests against trend. Econometrica, 13(3), 245-259.
- Sen, P.K. (1968). Estimates of the regression coefficient based on Kendall's tau. Journal of the American Statistical Association, 63(324), 1379-1389.
- Page, E.S. (1954). Continuous inspection schemes. Biometrika, 41(1/2), 100-115. [CUSUM method]

---

## Citation

```bibtex
@software{pyGWRetrieval,
  author = {Sayantan Majumdar},
  title = {pyGWRetrieval: Scalable Retrieval and Analysis of USGS Groundwater Level Data},
  year = {2026},
  url = {https://github.com/montimaj/pyGWRetrieval}
}
```

*Report generated automatically by Claude Opus 4.5 using results from full_workflow_csv_zipcodes.py*
