# Metropolitan Groundwater Analysis & General Usage

This folder holds a regional groundwater **case study across 9 U.S. metropolitan
areas** plus a set of general-purpose usage scripts.

> **Note**: the committed outputs in `output/` were produced with pyGWRetrieval
> [v0.1.0](https://github.com/montimaj/pyGWRetrieval/releases/tag/v0.1.0) (NWIS
> backend). Results are substantially unchanged on the v0.2.0 Water Data OGC API
> backend, but exact counts will differ if you re-run.

## Case study: 9 U.S. Metropolitan Statistical Areas

[`full_workflow_csv_zipcodes.py`](full_workflow_csv_zipcodes.py) runs a complete
pipeline over nine MSAs (New York, Miami, Washington DC, Houston, Boston,
Philadelphia, San Francisco, Chicago, Dallas): retrieval from a CSV of zip
codes, per-zip-code export, temporal aggregation, trend/statistics, and
publication-ready figures.

```bash
cd examples/msa_analysis
python full_workflow_csv_zipcodes.py   # reads AirbnbMSACity_with_ZipCode.csv, writes output/
```

| Metric | Value |
|--------|-------|
| Total records | 7,995,927 |
| Monitoring wells | 33,018 |
| Temporal coverage | 1970–2025 |
| Metropolitan areas | 9 |
| Zip codes analyzed | 99 |

The full auto-generated report is in
[`output/ANALYSIS_REPORT.md`](output/ANALYSIS_REPORT.md).

## General usage examples

| Script | Description |
|--------|-------------|
| [`basic_usage.py`](basic_usage.py) | Basic data retrieval and visualization |
| [`temporal_analysis.py`](temporal_analysis.py) | Temporal aggregation and trend analysis |
| [`advanced_spatial.py`](advanced_spatial.py) | Advanced spatial queries |
| [`multi_source_example.py`](multi_source_example.py) | Retrieving from multiple USGS sources (gwlevels / dv / iv) |
| [`test_data_retrieval.py`](test_data_retrieval.py) | Minimal retrieval sanity check |

## Inputs & outputs

- `AirbnbMSACity_with_ZipCode.csv` — the zip codes analyzed in the case study.
- `output/` — combined and per-zip-code data (Parquet), aggregations, analysis
  CSVs, figures, and the markdown report.
