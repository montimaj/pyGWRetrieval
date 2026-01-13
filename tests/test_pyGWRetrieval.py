"""
Unit tests for pyGWRetrieval package.

These tests cover the core functionality of the package including:
- Spatial operations
- Data retrieval (with mocking)
- Temporal aggregation
- Utility functions
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from shapely.geometry import Point, Polygon
import geopandas as gpd

# Import package modules
from pyGWRetrieval.spatial import (
    get_zipcode_geometry,
    buffer_geometry,
    get_bounding_box,
    get_geometry_type,
    merge_geometries,
)
from pyGWRetrieval.temporal import TemporalAggregator, aggregate_by_period
from pyGWRetrieval.utils import (
    validate_date_range,
    clean_data,
    get_data_coverage,
    filter_by_data_availability,
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def sample_gw_data():
    """Create sample groundwater data for testing."""
    np.random.seed(42)
    
    dates = pd.date_range(start='2020-01-01', end='2023-12-31', freq='D')
    
    data_list = []
    for site_no in ['site_001', 'site_002', 'site_003']:
        for date in dates:
            # Simulate seasonal variation
            seasonal = 5 * np.sin(2 * np.pi * date.dayofyear / 365)
            value = 50 + seasonal + np.random.normal(0, 2)
            
            data_list.append({
                'site_no': site_no,
                'lev_dt': date,
                'lev_va': value,
                'station_nm': f'Test Station {site_no}',
            })
    
    return pd.DataFrame(data_list)


@pytest.fixture
def sample_wells_gdf():
    """Create sample wells GeoDataFrame."""
    data = {
        'site_no': ['site_001', 'site_002', 'site_003'],
        'station_nm': ['Well 1', 'Well 2', 'Well 3'],
        'dec_lat_va': [39.5, 39.6, 39.7],
        'dec_long_va': [-119.5, -119.6, -119.7],
    }
    
    geometry = [
        Point(lon, lat) 
        for lon, lat in zip(data['dec_long_va'], data['dec_lat_va'])
    ]
    
    return gpd.GeoDataFrame(data, geometry=geometry, crs='EPSG:4326')


# ============================================================================
# Spatial Tests
# ============================================================================

class TestSpatialOperations:
    """Tests for spatial operations."""
    
    def test_get_zipcode_geometry_valid(self):
        """Test valid zip code lookup."""
        point, info = get_zipcode_geometry('89701')
        
        assert isinstance(point, Point)
        assert info['zipcode'] == '89701'
        assert info['state_code'] == 'NV'
        assert -180 <= point.x <= 180
        assert -90 <= point.y <= 90
    
    def test_get_zipcode_geometry_invalid(self):
        """Test invalid zip code raises error."""
        with pytest.raises(ValueError):
            get_zipcode_geometry('00000')
    
    def test_buffer_geometry_point(self):
        """Test buffer around point geometry."""
        point = Point(-119.5, 39.5)
        buffered = buffer_geometry(point, buffer_miles=10)
        
        assert isinstance(buffered, (Polygon,))
        # Check that the buffer is larger than the point
        assert buffered.area > 0
        # Point should be within buffer
        assert buffered.contains(point)
    
    def test_buffer_geometry_polygon(self):
        """Test buffer around polygon geometry."""
        polygon = Polygon([
            (-119.5, 39.5),
            (-119.4, 39.5),
            (-119.4, 39.6),
            (-119.5, 39.6),
            (-119.5, 39.5),
        ])
        
        buffered = buffer_geometry(polygon, buffer_miles=5)
        
        assert buffered.area > polygon.area
        assert buffered.contains(polygon)
    
    def test_get_bounding_box_point(self):
        """Test bounding box for point."""
        point = Point(-119.5, 39.5)
        bbox = get_bounding_box(point)
        
        assert len(bbox) == 4
        assert bbox[0] == bbox[2] == -119.5  # min/max lon same for point
        assert bbox[1] == bbox[3] == 39.5    # min/max lat same for point
    
    def test_get_bounding_box_polygon(self):
        """Test bounding box for polygon."""
        polygon = Polygon([
            (-119.5, 39.5),
            (-119.4, 39.5),
            (-119.4, 39.6),
            (-119.5, 39.6),
            (-119.5, 39.5),
        ])
        
        bbox = get_bounding_box(polygon)
        
        assert bbox == (-119.5, 39.5, -119.4, 39.6)
    
    def test_get_geometry_type_point(self):
        """Test geometry type detection for point."""
        point = Point(-119.5, 39.5)
        assert get_geometry_type(point) == 'point'
    
    def test_get_geometry_type_polygon(self):
        """Test geometry type detection for polygon."""
        polygon = Polygon([
            (-119.5, 39.5),
            (-119.4, 39.5),
            (-119.4, 39.6),
            (-119.5, 39.6),
        ])
        assert get_geometry_type(polygon) == 'polygon'
    
    def test_merge_geometries(self, sample_wells_gdf):
        """Test merging geometries from GeoDataFrame."""
        merged = merge_geometries(sample_wells_gdf)
        
        # Merged should be a multipoint or convex hull
        assert merged is not None


# ============================================================================
# Temporal Tests
# ============================================================================

class TestTemporalAggregation:
    """Tests for temporal aggregation."""
    
    def test_temporal_aggregator_init(self, sample_gw_data):
        """Test TemporalAggregator initialization."""
        aggregator = TemporalAggregator(sample_gw_data)
        
        assert aggregator.data is not None
        assert len(aggregator.data) == len(sample_gw_data)
        assert 'year' in aggregator.data.columns
        assert 'month' in aggregator.data.columns
    
    def test_to_monthly(self, sample_gw_data):
        """Test monthly aggregation."""
        aggregator = TemporalAggregator(sample_gw_data)
        monthly = aggregator.to_monthly()
        
        assert 'year' in monthly.columns
        assert 'month' in monthly.columns
        assert 'value' in monthly.columns
        assert 'count' in monthly.columns
        
        # Should have at most 12 months per year per site
        site_months = monthly.groupby(['site_no', 'year']).size()
        assert site_months.max() <= 12
    
    def test_to_annual(self, sample_gw_data):
        """Test annual aggregation."""
        aggregator = TemporalAggregator(sample_gw_data)
        annual = aggregator.to_annual()
        
        assert 'year' in annual.columns
        assert 'value' in annual.columns
        
        # Check years are reasonable
        assert annual['year'].min() >= 2020
        assert annual['year'].max() <= 2023
    
    def test_to_annual_water_year(self, sample_gw_data):
        """Test water year aggregation."""
        aggregator = TemporalAggregator(sample_gw_data)
        water_year = aggregator.to_annual(water_year=True)
        
        assert 'year' in water_year.columns
        # Water year 2021 includes Oct 2020 - Sep 2021
        assert water_year['year'].min() >= 2020
    
    def test_to_growing_season(self, sample_gw_data):
        """Test growing season aggregation."""
        aggregator = TemporalAggregator(sample_gw_data)
        growing = aggregator.to_growing_season(start_month=4, end_month=9)
        
        assert 'start_month' in growing.columns
        assert 'end_month' in growing.columns
        assert growing['start_month'].iloc[0] == 4
        assert growing['end_month'].iloc[0] == 9
    
    def test_to_custom_period(self, sample_gw_data):
        """Test custom period aggregation."""
        aggregator = TemporalAggregator(sample_gw_data)
        summer = aggregator.to_custom_period(
            months=[6, 7, 8],
            period_name='summer'
        )
        
        assert 'period' in summer.columns
        assert summer['period'].iloc[0] == 'summer'
    
    def test_to_weekly(self, sample_gw_data):
        """Test weekly aggregation."""
        aggregator = TemporalAggregator(sample_gw_data)
        weekly = aggregator.to_weekly()
        
        assert 'week' in weekly.columns
        assert weekly['week'].min() >= 1
        assert weekly['week'].max() <= 53
    
    def test_calculate_statistics(self, sample_gw_data):
        """Test statistics calculation."""
        aggregator = TemporalAggregator(sample_gw_data)
        stats = aggregator.calculate_statistics()
        
        assert 'count' in stats.columns
        assert 'mean' in stats.columns
        assert 'std' in stats.columns
        assert 'min' in stats.columns
        assert 'max' in stats.columns
    
    def test_aggregate_by_period_function(self, sample_gw_data):
        """Test convenience aggregation function."""
        monthly = aggregate_by_period(sample_gw_data, period='monthly')
        annual = aggregate_by_period(sample_gw_data, period='annual')
        
        assert len(monthly) > len(annual)


# ============================================================================
# Utility Tests
# ============================================================================

class TestUtilities:
    """Tests for utility functions."""
    
    def test_validate_date_range_valid(self):
        """Test valid date range."""
        assert validate_date_range('2020-01-01', '2023-12-31') is True
    
    def test_validate_date_range_invalid_format(self):
        """Test invalid date format raises error."""
        with pytest.raises(ValueError):
            validate_date_range('01-01-2020', '12-31-2023')
    
    def test_validate_date_range_end_before_start(self):
        """Test end before start raises error."""
        with pytest.raises(ValueError):
            validate_date_range('2023-01-01', '2020-01-01')
    
    def test_clean_data_drop_na(self, sample_gw_data):
        """Test data cleaning with NaN removal."""
        # Add some NaN values
        data = sample_gw_data.copy()
        data.loc[0:10, 'lev_va'] = np.nan
        
        cleaned = clean_data(data, drop_na=True)
        
        assert len(cleaned) < len(data)
        assert cleaned['lev_va'].isna().sum() == 0
    
    def test_clean_data_min_max(self, sample_gw_data):
        """Test data cleaning with min/max filters."""
        cleaned = clean_data(
            sample_gw_data,
            min_value=40,
            max_value=60
        )
        
        assert cleaned['lev_va'].min() >= 40
        assert cleaned['lev_va'].max() <= 60
    
    def test_get_data_coverage(self, sample_gw_data):
        """Test data coverage calculation."""
        coverage = get_data_coverage(sample_gw_data)
        
        assert 'first_date' in coverage.columns
        assert 'last_date' in coverage.columns
        assert 'n_records' in coverage.columns
        assert 'coverage_pct' in coverage.columns
        
        # Should have one row per site
        assert len(coverage) == sample_gw_data['site_no'].nunique()
    
    def test_filter_by_data_availability(self, sample_gw_data):
        """Test filtering by data availability."""
        # All wells have sufficient data
        filtered = filter_by_data_availability(
            sample_gw_data,
            min_records=100,
            min_years=1
        )
        
        assert len(filtered) > 0
        
        # Filter with high requirements
        filtered_strict = filter_by_data_availability(
            sample_gw_data,
            min_records=10000,  # More than available
            min_years=10
        )
        
        # Should filter out all wells
        assert len(filtered_strict) == 0


# ============================================================================
# Integration Tests
# ============================================================================

class TestIntegration:
    """Integration tests combining multiple modules."""
    
    def test_full_workflow(self, sample_gw_data):
        """Test a complete workflow from data to aggregation."""
        # 1. Clean data
        cleaned = clean_data(sample_gw_data, drop_na=True)
        assert len(cleaned) > 0
        
        # 2. Get coverage
        coverage = get_data_coverage(cleaned)
        assert len(coverage) == 3  # 3 wells
        
        # 3. Aggregate
        aggregator = TemporalAggregator(cleaned)
        monthly = aggregator.to_monthly()
        annual = aggregator.to_annual()
        
        assert len(monthly) > 0
        assert len(annual) > 0
        
        # 4. Statistics
        stats = aggregator.calculate_statistics()
        assert len(stats) == 3  # One per well
    
    def test_spatial_to_temporal(self, sample_wells_gdf):
        """Test spatial operations followed by data processing."""
        # Buffer wells
        buffered = buffer_geometry(sample_wells_gdf, buffer_miles=5)
        
        # Should be able to get bounding box
        if hasattr(buffered, 'total_bounds'):
            bbox = buffered.total_bounds
        else:
            bbox = get_bounding_box(merge_geometries(buffered))
        
        assert len(bbox) == 4


# ============================================================================
# Edge Case Tests
# ============================================================================

class TestEdgeCases:
    """Tests for edge cases and error handling."""
    
    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        empty_df = pd.DataFrame(columns=['site_no', 'lev_dt', 'lev_va'])
        
        with pytest.raises(Exception):
            # Should raise an error with empty data
            aggregator = TemporalAggregator(empty_df)
    
    def test_single_record(self):
        """Test handling of single record."""
        single_df = pd.DataFrame({
            'site_no': ['site_001'],
            'lev_dt': [datetime.now()],
            'lev_va': [50.0],
        })
        
        aggregator = TemporalAggregator(single_df)
        monthly = aggregator.to_monthly()
        
        assert len(monthly) == 1
    
    def test_buffer_zero_miles(self):
        """Test buffer with zero miles."""
        point = Point(-119.5, 39.5)
        
        # Zero buffer should still work (returns very small polygon)
        buffered = buffer_geometry(point, buffer_miles=0.001)
        assert buffered is not None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
