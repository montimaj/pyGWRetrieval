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
        """An empty DataFrame aggregates to an empty result, not an error."""
        empty_df = pd.DataFrame(columns=['site_no', 'lev_dt', 'lev_va'])

        # Construction succeeds as long as the required columns are present...
        aggregator = TemporalAggregator(empty_df)
        # ...and aggregation returns an empty DataFrame rather than raising.
        monthly = aggregator.to_monthly()
        assert monthly.empty
    
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


class TestBBoxTiling:
    """Tests for automatic chunking of oversized bounding boxes."""

    def _retriever(self):
        from pyGWRetrieval import GroundwaterRetrieval
        return GroundwaterRetrieval()

    def test_small_bbox_not_tiled(self):
        """A box within the NWIS limit is returned unchanged as one tile."""
        gw = self._retriever()
        box = (-120.0, 39.0, -119.0, 40.0)  # 1 sq-deg
        tiles = gw._tile_bbox(*box)
        assert len(tiles) == 1
        assert tiles[0] == box

    def test_large_bbox_is_tiled(self):
        """An oversized box is split into multiple sub-boxes."""
        gw = self._retriever()
        tiles = gw._tile_bbox(-112.3286, 35.5584, -105.6264, 43.4522)  # ~53 sq-deg
        assert len(tiles) > 1

    def test_all_tiles_under_limit(self):
        """Every produced tile stays under the NWIS area limit."""
        gw = self._retriever()
        tiles = gw._tile_bbox(-112.3286, 35.5584, -105.6264, 43.4522)
        for min_lon, min_lat, max_lon, max_lat in tiles:
            area = (max_lon - min_lon) * (max_lat - min_lat)
            assert area <= gw.MAX_BBOX_SQ_DEGREES + 1e-9

    def test_tiles_cover_original_bbox(self):
        """The union of tiles exactly spans the original box extent."""
        gw = self._retriever()
        box = (-112.3286, 35.5584, -105.6264, 43.4522)
        tiles = gw._tile_bbox(*box)
        assert min(t[0] for t in tiles) == pytest.approx(box[0])
        assert min(t[1] for t in tiles) == pytest.approx(box[1])
        assert max(t[2] for t in tiles) == pytest.approx(box[2])
        assert max(t[3] for t in tiles) == pytest.approx(box[3])

    def test_oversized_bbox_queries_each_tile_and_dedupes(self):
        """Legacy NWIS fallback tiles the bbox and de-duplicates wells."""
        from unittest.mock import patch

        gw = self._retriever()

        def fake_single(min_lon, min_lat, max_lon, max_lat):
            # Every tile reports a shared well plus a tile-unique one.
            df = pd.DataFrame({
                'site_no': ['shared', f'{min_lon:.2f}_{min_lat:.2f}'],
                'dec_long_va': [min_lon, min_lon],
                'dec_lat_va': [min_lat, min_lat],
            })
            return gpd.GeoDataFrame(
                df,
                geometry=[Point(min_lon, min_lat), Point(min_lon, min_lat)],
                crs='EPSG:4326',
            )

        # The tiling path is now the legacy NWIS fallback (the default path uses
        # the modern OGC API, which chunks natively).
        with patch.object(gw, '_get_wells_by_single_bbox', side_effect=fake_single) as m:
            wells = gw._get_wells_by_bbox_nwis(-112.3286, 35.5584, -105.6264, 43.4522)

        assert m.call_count > 1  # tiled into multiple queries
        # 'shared' appears once despite being returned by every tile
        assert (wells['site_no'] == 'shared').sum() == 1


class TestInterpolation:
    """Tests for the water-table depth interpolator."""

    def _sample_wtd(self):
        # A handful of wells spread across a small area, one record each.
        rng = [(39.2, -108.9, 10.0), (39.6, -108.5, 20.0), (39.4, -108.7, 15.0),
               (39.8, -108.3, 25.0), (39.1, -108.2, 12.0), (39.7, -108.8, 18.0)]
        rows = []
        for i, (lat, lon, val) in enumerate(rng):
            rows.append({'site_no': f'USGS-{i}', 'datetime': datetime(2020, 1, 1),
                         'lev_va': val, 'dec_lat_va': lat, 'dec_long_va': lon})
        return pd.DataFrame(rows)

    def test_idw_grid_shape_and_range(self):
        from pyGWRetrieval import WaterTableInterpolator
        interp = WaterTableInterpolator(self._sample_wtd())
        res = interp.interpolate(period='all', method='idw', grid_size_m=5000)['all']
        assert res.grid.ndim == 2
        assert res.grid.shape == (res.y.size, res.x.size)
        finite = res.grid[np.isfinite(res.grid)]
        # IDW is bounded by the observed value range.
        assert finite.min() >= 10.0 - 1e-6
        assert finite.max() <= 25.0 + 1e-6
        assert res.crs.startswith('EPSG:')

    def test_grid_resolution_in_meters(self):
        from pyGWRetrieval import WaterTableInterpolator
        interp = WaterTableInterpolator(self._sample_wtd())
        res = interp.interpolate(period='all', method='idw', grid_size_m=2000)['all']
        # Cell spacing along x should equal the requested size (meters).
        assert abs((res.x[1] - res.x[0]) - 2000) < 1e-6

    def test_period_all_single_window(self):
        from pyGWRetrieval import WaterTableInterpolator
        interp = WaterTableInterpolator(self._sample_wtd())
        grids = interp.interpolate(period='all', method='idw', grid_size_m=5000)
        assert list(grids.keys()) == ['all']

    def test_invalid_method_raises(self):
        from pyGWRetrieval import WaterTableInterpolator
        interp = WaterTableInterpolator(self._sample_wtd())
        with pytest.raises(ValueError):
            interp.interpolate(method='bogus')

    def test_nearest_via_scipy(self):
        pytest.importorskip('scipy')
        from pyGWRetrieval import WaterTableInterpolator
        interp = WaterTableInterpolator(self._sample_wtd())
        res = interp.interpolate(period='all', method='nearest', grid_size_m=5000)['all']
        finite = res.grid[np.isfinite(res.grid)]
        # Nearest-neighbor only ever returns observed values.
        assert set(np.round(finite, 6)).issubset({10.0, 12.0, 15.0, 18.0, 20.0, 25.0})

    def test_fill_outside_backfills_linear_hull(self):
        pytest.importorskip('scipy')
        from pyGWRetrieval import WaterTableInterpolator
        interp = WaterTableInterpolator(self._sample_wtd())
        plain = interp.interpolate(period='all', method='linear', grid_size_m=3000,
                                   clip_to_boundary=False)['all']
        filled = interp.interpolate(period='all', method='linear', grid_size_m=3000,
                                    fill_outside='nearest', clip_to_boundary=False)['all']
        # Linear alone leaves out-of-hull NaNs; backfilling removes them.
        assert np.isnan(plain.grid).any()
        assert not np.isnan(filled.grid).any()

    def test_idw_at_points(self):
        pytest.importorskip('scipy')
        from pyGWRetrieval import idw_at_points
        src = np.array([[0.0, 0.0], [100.0, 0.0]])
        val = np.array([10.0, 20.0])
        # Exact hit returns the observed value; midpoint is the equal-weight mean.
        out = idw_at_points(src, val, np.array([[0.0, 0.0], [50.0, 0.0], [100.0, 0.0]]),
                            n_neighbors=2)
        assert out[0] == 10.0 and out[2] == 20.0
        assert abs(out[1] - 15.0) < 1e-9

    def test_idw_at_points_leave_one_out(self):
        pytest.importorskip('scipy')
        from pyGWRetrieval import idw_at_points
        xy = np.array([[0.0, 0.0], [10.0, 0.0], [20.0, 0.0], [30.0, 0.0]])
        val = np.array([1.0, 2.0, 3.0, 4.0])
        # Each point predicted from the others (never returns its own value here).
        loo = idw_at_points(xy, val, xy, n_neighbors=3, leave_one_out=True)
        assert loo.shape == (4,)
        assert np.isfinite(loo).all()


class TestWaterdataStandardization:
    """Tests for mapping the Water Data OGC long-format frame to package schema."""

    def test_standardize_maps_columns(self):
        from pyGWRetrieval import GroundwaterRetrieval
        raw = pd.DataFrame({
            'monitoring_location_id': ['USGS-123', 'USGS-456'],
            'parameter_code': ['72019', '62611'],
            'time': pd.to_datetime(['2020-01-01', '2020-01-02'], utc=True),
            'value': ['12.5', '3000.0'],
        })
        out = GroundwaterRetrieval._standardize_waterdata(raw)
        assert list(out['site_no']) == ['USGS-123', 'USGS-456']
        assert list(out['parameter_cd']) == ['72019', '62611']
        assert list(out['lev_va']) == [12.5, 3000.0]
        assert list(out['value']) == [12.5, 3000.0]
        assert out['datetime'].dt.tz is None  # tz-naive
        assert 'lev_dt' in out.columns

    def test_standardize_empty(self):
        from pyGWRetrieval import GroundwaterRetrieval
        assert GroundwaterRetrieval._standardize_waterdata(pd.DataFrame()).empty
        assert GroundwaterRetrieval._standardize_waterdata(None).empty


class TestValueCoalescing:
    """Tests for parameter-aware value/lev_va coalescing of dv/iv frames (legacy)."""

    def _retriever(self):
        from pyGWRetrieval import GroundwaterRetrieval
        return GroundwaterRetrieval()

    def test_dv_prefers_depth_to_water_and_records_param(self):
        """dv coalesce prefers 72019 (depth) and records parameter_cd."""
        gw = self._retriever()
        df = pd.DataFrame({
            '72019_Mean': [374.5, None, 10.0],
            '72019_Mean_cd': ['A', 'A', 'A'],
            '62611_Maximum': [None, 8800.0, 9999.0],
            '62611_Maximum_cd': [None, 'A', 'A'],
        })
        out = gw._coalesce_value_columns(df.copy(), source='dv')
        # Row 2 has only 62611; rows 0 and 2 prefer 72019 even when both present.
        assert list(out['parameter_cd']) == ['72019', '62611', '72019']
        assert list(out['value']) == [374.5, 8800.0, 10.0]
        assert list(out['lev_va']) == [374.5, 8800.0, 10.0]

    def test_dv_prefers_mean_statistic(self):
        """When a parameter has several statistics, the mean is chosen."""
        gw = self._retriever()
        df = pd.DataFrame({
            '72019_Minimum': [1.0],
            '72019_Mean': [2.0],
            '72019_Maximum': [3.0],
        })
        out = gw._coalesce_value_columns(df.copy(), source='dv')
        assert out['value'].iloc[0] == 2.0
        assert out['parameter_cd'].iloc[0] == '72019'

    def test_iv_bare_parameter_columns(self):
        """iv frames use bare parameter-code columns; _cd columns are ignored."""
        gw = self._retriever()
        df = pd.DataFrame({
            '72019': [5.0, None],
            '72019_cd': ['A', 'A'],
            '62611': [None, 9000.0],
        })
        out = gw._coalesce_value_columns(df.copy(), source='iv')
        assert list(out['parameter_cd']) == ['72019', '62611']
        assert list(out['value']) == [5.0, 9000.0]

    def test_no_parameter_columns_is_noop(self):
        """A frame without any parameter columns is returned unchanged."""
        gw = self._retriever()
        df = pd.DataFrame({'site_no': ['x'], 'datetime': [pd.Timestamp('2020-01-01')]})
        out = gw._coalesce_value_columns(df.copy(), source='dv')
        assert 'value' not in out.columns


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
