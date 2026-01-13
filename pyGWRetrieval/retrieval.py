"""
Groundwater data retrieval module for pyGWRetrieval.

This module provides the main GroundwaterRetrieval class for downloading
groundwater level data from the USGS National Water Information System (NWIS)
using the dataretrieval-python API.

Features:
    - Query groundwater wells by bounding box or state
    - Download data from multiple sources:
      * Field groundwater-level measurements (gwlevels)
      * Daily values (dv)
      * Instantaneous/current observations (iv)
    - Filter by date range
    - Export to CSV or Parquet formats

Data Sources:
    - gwlevels: Field measurements of groundwater levels (discrete)
    - dv: Daily statistical values (mean, min, max)
    - iv: Instantaneous values (typically 15-minute intervals)

Note on Data Aggregation:
    All three data types (gwlevels, dv, iv) are stored as-is after download
    without any aggregation or transformation. Daily values (dv) are pre-computed
    by USGS from continuous sensor data. Instantaneous values (iv) retain their
    original high-frequency resolution (typically 15-60 minute intervals).
    If aggregation is needed, use the TemporalAggregator class from the
    temporal module.

Dependencies:
    - dataretrieval
    - pandas
    - geopandas
"""

import logging
import warnings
from datetime import datetime
from pathlib import Path
from typing import Optional, Union, List, Tuple, Dict, Literal

import pandas as pd
import geopandas as gpd
import dataretrieval.nwis as nwis
from shapely.geometry import Point, Polygon, MultiPolygon

# Suppress warnings from dataretrieval/pandas about mixed types and incomplete dates
warnings.filterwarnings('ignore', message='.*mixed types.*', category=pd.errors.DtypeWarning)
warnings.filterwarnings('ignore', message='.*incomplete dates.*', category=UserWarning)

from .spatial import (
    get_zipcode_geometry,
    get_geometry_from_geojson,
    get_geometry_from_shapefile,
    buffer_geometry,
    get_bounding_box,
    get_geometry_type,
    merge_geometries,
)
from .utils import save_to_csv, save_to_parquet, validate_date_range
from .parallel import (
    check_dask_available,
    parallel_map,
    parallel_batch_process,
    process_zipcode_worker,
    process_site_batch_worker,
    DASK_AVAILABLE,
)

logger = logging.getLogger(__name__)

# Type alias for data sources
DataSourceType = Literal['all', 'gwlevels', 'dv', 'iv']


class GroundwaterRetrieval:
    """
    Main class for retrieving groundwater level data from USGS NWIS.

    This class provides methods to query groundwater monitoring wells and
    download water level data based on geographic inputs such as zip codes,
    GeoJSON files, or shapefiles.

    Data Sources
    ------------
    The class can retrieve data from three USGS data sources:
    
    - **gwlevels**: Field groundwater-level measurements (discrete measurements)
      - Manual measurements taken by field personnel
      - Most accurate but least frequent
      - Contains: lev_dt, lev_tm, lev_va, lev_status_cd, lev_meth_cd
      
    - **dv**: Daily values (daily statistical summaries)
      - Daily mean, min, max from continuous monitoring
      - Good for long-term trend analysis
      - Contains: datetime, value (parameter code 72019 for GW level)
      
    - **iv**: Instantaneous values (current/historical observations)
      - High-frequency data (typically 15-minute intervals)
      - Best for detailed temporal analysis
      - Contains: datetime, value with timestamp precision

    Parameters
    ----------
    start_date : str, optional
        Start date for data retrieval in 'YYYY-MM-DD' format.
        Default is '1900-01-01'.
    end_date : str, optional
        End date for data retrieval in 'YYYY-MM-DD' format.
        Default is today's date.
    data_sources : str or list, optional
        Data source(s) to retrieve. Options:
        - 'all': Retrieve from all sources (default)
        - 'gwlevels': Field groundwater-level measurements only
        - 'dv': Daily values only
        - 'iv': Instantaneous values only
        - List of sources: e.g., ['gwlevels', 'dv']

    Attributes
    ----------
    start_date : str
        Start date for queries.
    end_date : str
        End date for queries.
    data_sources : list
        List of data sources to query.
    wells : gpd.GeoDataFrame
        GeoDataFrame of discovered wells.
    data : pd.DataFrame
        DataFrame containing retrieved groundwater level data.

    Examples
    --------
    >>> # Get all data sources (default)
    >>> gw = GroundwaterRetrieval(start_date='2020-01-01')
    >>> data = gw.get_data_by_zipcode('89701', buffer_miles=10)
    
    >>> # Get only field measurements
    >>> gw = GroundwaterRetrieval(data_sources='gwlevels')
    >>> data = gw.get_data_by_zipcode('89701', buffer_miles=10)
    
    >>> # Get daily values and instantaneous values
    >>> gw = GroundwaterRetrieval(data_sources=['dv', 'iv'])
    >>> data = gw.get_data_by_zipcode('89701', buffer_miles=10)
    """

    # USGS parameter codes for groundwater level
    GW_LEVEL_PARAM = '72019'  # Depth to water level, feet below land surface
    GW_LEVEL_PARAMS = ['72019', '72020', '62610', '62611']  # Various GW level codes
    
    # Site type for groundwater wells
    SITE_TYPE = 'GW'
    
    # Valid data sources
    VALID_SOURCES = ['gwlevels', 'dv', 'iv']

    def __init__(
        self,
        start_date: str = '1900-01-01',
        end_date: Optional[str] = None,
        data_sources: Union[str, List[str]] = 'all'
    ):
        """Initialize the GroundwaterRetrieval instance."""
        self.start_date = start_date
        self.end_date = end_date or datetime.now().strftime('%Y-%m-%d')
        
        validate_date_range(self.start_date, self.end_date)
        
        # Parse data sources
        if data_sources == 'all':
            self.data_sources = self.VALID_SOURCES.copy()
        elif isinstance(data_sources, str):
            if data_sources not in self.VALID_SOURCES:
                raise ValueError(
                    f"Invalid data source: {data_sources}. "
                    f"Valid options: {self.VALID_SOURCES} or 'all'"
                )
            self.data_sources = [data_sources]
        elif isinstance(data_sources, list):
            invalid = [s for s in data_sources if s not in self.VALID_SOURCES]
            if invalid:
                raise ValueError(
                    f"Invalid data source(s): {invalid}. "
                    f"Valid options: {self.VALID_SOURCES}"
                )
            self.data_sources = data_sources
        else:
            raise ValueError(
                f"data_sources must be 'all', a string, or a list. Got: {type(data_sources)}"
            )
        
        self.wells: Optional[gpd.GeoDataFrame] = None
        self.data: Optional[pd.DataFrame] = None
        self._geometry: Optional[Union[Polygon, MultiPolygon]] = None
        
        logger.info(
            f"Initialized GroundwaterRetrieval for period {self.start_date} to {self.end_date}, "
            f"data sources: {self.data_sources}"
        )

    def get_data_by_zipcode(
        self,
        zipcode: str,
        buffer_miles: float = 10.0,
        country: str = "US"
    ) -> pd.DataFrame:
        """
        Retrieve groundwater data for wells within a buffer around a zip code.

        Parameters
        ----------
        zipcode : str
            The zip code to search around.
        buffer_miles : float, optional
            Buffer distance in miles around the zip code centroid.
            Default is 10 miles.
        country : str, optional
            Country code for zip code lookup. Default is "US".

        Returns
        -------
        pd.DataFrame
            DataFrame containing groundwater level data with columns:
            - site_no: USGS site number
            - datetime: Date/time of measurement
            - value: Water level value
            - parameter_cd: Parameter code
            - station_nm: Station name
            - dec_lat_va: Decimal latitude
            - dec_long_va: Decimal longitude

        Examples
        --------
        >>> gw = GroundwaterRetrieval()
        >>> data = gw.get_data_by_zipcode('89701', buffer_miles=15)
        >>> print(f"Retrieved {len(data)} records from {data['site_no'].nunique()} wells")
        """
        logger.info(f"Searching for groundwater data near zip code {zipcode}")
        
        # Get zip code geometry
        point, info = get_zipcode_geometry(zipcode, country)
        logger.info(f"Location: {info['place_name']}, {info['state_name']}")
        
        # Create buffer
        self._geometry = buffer_geometry(point, buffer_miles)
        
        # Get bounding box
        bbox = get_bounding_box(self._geometry)
        
        # Find wells and get data
        return self._retrieve_data_by_bbox(bbox, self._geometry)

    def get_data_by_zipcodes_csv(
        self,
        filepath: Union[str, Path],
        zipcode_column: str,
        buffer_miles: float = 10.0,
        country: str = "US",
        merge_results: bool = True,
        parallel: bool = True,
        n_workers: Optional[int] = None,
        scheduler: str = 'threads'
    ) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Retrieve groundwater data for multiple zip codes from a CSV file.

        Parameters
        ----------
        filepath : Union[str, Path]
            Path to the CSV file containing zip codes.
        zipcode_column : str
            Name of the column containing zip codes.
        buffer_miles : float, optional
            Buffer distance in miles around each zip code centroid.
            Default is 10 miles.
        country : str, optional
            Country code for zip code lookup. Default is "US".
        merge_results : bool, optional
            If True, merge all results into a single DataFrame.
            If False, return a dictionary with zip code as key.
            Default is True.
        parallel : bool, optional
            If True, process zip codes in parallel using Dask.
            Default is True.
        n_workers : int, optional
            Number of parallel workers. Default is auto-detected.
        scheduler : str, optional
            Dask scheduler ('threads', 'processes', 'synchronous').
            Default is 'threads'.

        Returns
        -------
        Union[pd.DataFrame, Dict[str, pd.DataFrame]]
            If merge_results=True: Single DataFrame with all data and 
            'source_zipcode' column indicating the zip code source.
            If merge_results=False: Dictionary mapping zip codes to DataFrames.

        Raises
        ------
        FileNotFoundError
            If the CSV file does not exist.
        ValueError
            If the specified column is not found in the CSV.

        Examples
        --------
        >>> gw = GroundwaterRetrieval()
        >>> # CSV with column 'zip' containing zip codes - parallel processing
        >>> data = gw.get_data_by_zipcodes_csv('locations.csv', zipcode_column='zip')
        >>> print(f"Retrieved data from {data['source_zipcode'].nunique()} zip codes")
        
        >>> # Sequential processing (disable parallel)
        >>> data = gw.get_data_by_zipcodes_csv(
        ...     'locations.csv', 
        ...     zipcode_column='zip',
        ...     parallel=False
        ... )
        
        >>> # Parallel with specific worker count
        >>> data = gw.get_data_by_zipcodes_csv(
        ...     'locations.csv',
        ...     zipcode_column='zip',
        ...     parallel=True,
        ...     n_workers=4
        ... )
        """
        filepath = Path(filepath)
        
        if not filepath.exists():
            raise FileNotFoundError(f"CSV file not found: {filepath}")
        
        # Read CSV file
        csv_data = pd.read_csv(filepath)
        
        if zipcode_column not in csv_data.columns:
            raise ValueError(
                f"Column '{zipcode_column}' not found in CSV. "
                f"Available columns: {list(csv_data.columns)}"
            )
        
        # Get unique zip codes
        zipcodes = csv_data[zipcode_column].dropna().astype(str).unique()
        logger.info(f"Processing {len(zipcodes)} unique zip codes from {filepath}")
        logger.info(f"Data sources: {self.data_sources}")
        
        results = {}
        all_wells = gpd.GeoDataFrame()
        
        # Check if parallel processing is available and requested
        use_parallel = parallel and DASK_AVAILABLE and len(zipcodes) > 1
        
        if use_parallel:
            logger.info(f"Using parallel processing with scheduler='{scheduler}'")
            
            # Prepare arguments for parallel processing (include data_sources)
            args_list = [
                (zipcode, buffer_miles, country, self.start_date, self.end_date, self.data_sources)
                for zipcode in zipcodes
            ]
            
            # Process in parallel
            parallel_results = parallel_map(
                process_zipcode_worker,
                args_list,
                n_workers=n_workers,
                show_progress=True,
                scheduler=scheduler
            )
            
            # Collect results
            for zipcode, data, wells in parallel_results:
                if data is not None and not data.empty:
                    results[zipcode] = data
                    if wells is not None:
                        all_wells = pd.concat([all_wells, wells], ignore_index=True)
        else:
            if parallel and not DASK_AVAILABLE:
                logger.warning("Dask not available, falling back to sequential processing")
            
            # Sequential processing
            for i, zipcode in enumerate(zipcodes, 1):
                logger.info(f"Processing zip code {i}/{len(zipcodes)}: {zipcode}")
                
                try:
                    # Get data for this zip code
                    data = self.get_data_by_zipcode(
                        zipcode=str(zipcode).zfill(5),  # Ensure 5-digit format
                        buffer_miles=buffer_miles,
                        country=country
                    )
                    
                    if not data.empty:
                        data['source_zipcode'] = zipcode
                        results[zipcode] = data
                        
                        # Accumulate wells
                        if self.wells is not None and not self.wells.empty:
                            wells_copy = self.wells.copy()
                            wells_copy['source_zipcode'] = zipcode
                            all_wells = pd.concat([all_wells, wells_copy], ignore_index=True)
                    else:
                        logger.warning(f"No data found for zip code {zipcode}")
                        
                except Exception as e:
                    logger.error(f"Error processing zip code {zipcode}: {e}")
                    continue
        
        # Store combined wells
        if not all_wells.empty:
            # Remove duplicate wells (may appear in multiple zip code buffers)
            self.wells = all_wells.drop_duplicates(subset=['site_no'])
        
        if not results:
            logger.warning("No data retrieved for any zip codes")
            self.data = pd.DataFrame()
            return self.data if merge_results else {}
        
        if merge_results:
            # Combine all results
            self.data = pd.concat(results.values(), ignore_index=True)
            logger.info(
                f"Retrieved {len(self.data)} total records from "
                f"{self.data['site_no'].nunique()} wells across "
                f"{len(results)} zip codes"
            )
            return self.data
        else:
            return results

    def get_data_by_geojson(
        self,
        filepath: Union[str, Path],
        buffer_miles: Optional[float] = None,
        layer: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Retrieve groundwater data based on geometries from a GeoJSON file.

        For point geometries, a buffer is required. For polygon geometries,
        data is retrieved for wells within the polygons.

        Parameters
        ----------
        filepath : Union[str, Path]
            Path to the GeoJSON file.
        buffer_miles : float, optional
            Buffer distance in miles for point geometries.
            Required for point data, ignored for polygons.
        layer : str, optional
            Layer name for multi-layer GeoJSON files.

        Returns
        -------
        pd.DataFrame
            DataFrame containing groundwater level data.

        Raises
        ------
        ValueError
            If point geometries are provided without a buffer distance.

        Examples
        --------
        >>> gw = GroundwaterRetrieval()
        >>> # For polygon (e.g., basin boundary)
        >>> data = gw.get_data_by_geojson('basin.geojson')
        >>> # For points with buffer
        >>> data = gw.get_data_by_geojson('wells.geojson', buffer_miles=5)
        """
        logger.info(f"Loading geometry from GeoJSON: {filepath}")
        
        gdf = get_geometry_from_geojson(filepath, layer)
        return self._process_geodataframe(gdf, buffer_miles)

    def get_data_by_shapefile(
        self,
        filepath: Union[str, Path],
        buffer_miles: Optional[float] = None
    ) -> pd.DataFrame:
        """
        Retrieve groundwater data based on geometries from a shapefile.

        For point geometries, a buffer is required. For polygon geometries,
        data is retrieved for wells within the polygons.

        Parameters
        ----------
        filepath : Union[str, Path]
            Path to the shapefile (.shp file).
        buffer_miles : float, optional
            Buffer distance in miles for point geometries.
            Required for point data, ignored for polygons.

        Returns
        -------
        pd.DataFrame
            DataFrame containing groundwater level data.

        Raises
        ------
        ValueError
            If point geometries are provided without a buffer distance.

        Examples
        --------
        >>> gw = GroundwaterRetrieval()
        >>> data = gw.get_data_by_shapefile('state_boundary.shp')
        """
        logger.info(f"Loading geometry from shapefile: {filepath}")
        
        gdf = get_geometry_from_shapefile(filepath)
        return self._process_geodataframe(gdf, buffer_miles)

    def get_data_by_state(
        self,
        state_code: str
    ) -> pd.DataFrame:
        """
        Retrieve groundwater data for an entire state.

        Parameters
        ----------
        state_code : str
            Two-letter state code (e.g., 'NV', 'CA').

        Returns
        -------
        pd.DataFrame
            DataFrame containing groundwater level data for the state.

        Examples
        --------
        >>> gw = GroundwaterRetrieval(start_date='2023-01-01')
        >>> data = gw.get_data_by_state('NV')
        """
        logger.info(f"Retrieving groundwater data for state: {state_code}")
        
        # Get wells in the state
        self.wells = self._get_wells_by_state(state_code)
        
        if self.wells.empty:
            logger.warning(f"No groundwater wells found in {state_code}")
            self.data = pd.DataFrame()
            return self.data
        
        # Get data for wells
        self.data = self._get_data_for_wells(self.wells['site_no'].tolist())
        
        return self.data

    def get_data_by_sites(
        self,
        site_numbers: List[str]
    ) -> pd.DataFrame:
        """
        Retrieve groundwater data for specific site numbers.

        Parameters
        ----------
        site_numbers : List[str]
            List of USGS site numbers.

        Returns
        -------
        pd.DataFrame
            DataFrame containing groundwater level data.

        Examples
        --------
        >>> gw = GroundwaterRetrieval()
        >>> data = gw.get_data_by_sites(['390000119000001', '390000119000002'])
        """
        logger.info(f"Retrieving data for {len(site_numbers)} sites")
        
        # Get site info
        self.wells = self._get_site_info(site_numbers)
        
        # Get data
        self.data = self._get_data_for_wells(site_numbers)
        
        return self.data

    def _process_geodataframe(
        self,
        gdf: gpd.GeoDataFrame,
        buffer_miles: Optional[float]
    ) -> pd.DataFrame:
        """Process a GeoDataFrame and retrieve data based on geometry type."""
        geom_type = get_geometry_type(gdf)
        
        if geom_type == 'point':
            if buffer_miles is None:
                raise ValueError(
                    "Buffer distance (buffer_miles) is required for point geometries"
                )
            # Buffer and merge points
            buffered_gdf = buffer_geometry(gdf, buffer_miles)
            self._geometry = merge_geometries(buffered_gdf)
        else:
            # Merge polygon geometries
            self._geometry = merge_geometries(gdf)
        
        # Get bounding box
        bbox = get_bounding_box(self._geometry)
        
        return self._retrieve_data_by_bbox(bbox, self._geometry)

    def _retrieve_data_by_bbox(
        self,
        bbox: Tuple[float, float, float, float],
        clip_geometry: Optional[Union[Polygon, MultiPolygon]] = None
    ) -> pd.DataFrame:
        """
        Retrieve groundwater data for a bounding box.

        Parameters
        ----------
        bbox : Tuple[float, float, float, float]
            Bounding box as (min_lon, min_lat, max_lon, max_lat).
        clip_geometry : Optional geometry
            Geometry to clip results to (for non-rectangular areas).

        Returns
        -------
        pd.DataFrame
            Retrieved groundwater level data.
        """
        min_lon, min_lat, max_lon, max_lat = bbox
        
        logger.info(f"Searching for wells in bbox: {bbox}")
        
        # Get wells in bounding box
        self.wells = self._get_wells_by_bbox(min_lon, min_lat, max_lon, max_lat)
        
        if self.wells.empty:
            logger.warning("No groundwater wells found in the specified area")
            self.data = pd.DataFrame()
            return self.data
        
        # Clip to actual geometry if provided
        if clip_geometry is not None:
            original_count = len(self.wells)
            self.wells = self._clip_wells_to_geometry(self.wells, clip_geometry)
            logger.info(f"Clipped from {original_count} to {len(self.wells)} wells within geometry")
        
        if self.wells.empty:
            logger.warning("No groundwater wells found within the specified geometry")
            self.data = pd.DataFrame()
            return self.data
        
        logger.info(f"Found {len(self.wells)} groundwater wells")
        
        # Get data for the wells
        site_numbers = self.wells['site_no'].tolist()
        self.data = self._get_data_for_wells(site_numbers)
        
        return self.data

    def _get_wells_by_bbox(
        self,
        min_lon: float,
        min_lat: float,
        max_lon: float,
        max_lat: float
    ) -> gpd.GeoDataFrame:
        """Get groundwater wells within a bounding box."""
        try:
            # Round coordinates to 4 decimal places to avoid USGS API issues
            # with high-precision floating point numbers
            bbox_str = f"{min_lon:.4f},{min_lat:.4f},{max_lon:.4f},{max_lat:.4f}"
            
            # Query NWIS for sites
            sites, _ = nwis.what_sites(
                bBox=bbox_str,
                siteType=self.SITE_TYPE,
                hasDataTypeCd='gw'
            )
            
            if sites.empty:
                return gpd.GeoDataFrame()
            
            # Create GeoDataFrame
            sites = sites.reset_index()
            geometry = [
                Point(lon, lat) 
                for lon, lat in zip(sites['dec_long_va'], sites['dec_lat_va'])
            ]
            gdf = gpd.GeoDataFrame(sites, geometry=geometry, crs='EPSG:4326')
            
            return gdf
            
        except Exception as e:
            logger.error(f"Error querying NWIS for wells: {e}")
            return gpd.GeoDataFrame()

    def _get_wells_by_state(
        self,
        state_code: str
    ) -> gpd.GeoDataFrame:
        """Get groundwater wells for a state."""
        try:
            sites, _ = nwis.what_sites(
                stateCd=state_code,
                siteType=self.SITE_TYPE,
                hasDataTypeCd='gw'
            )
            
            if sites.empty:
                return gpd.GeoDataFrame()
            
            sites = sites.reset_index()
            geometry = [
                Point(lon, lat) 
                for lon, lat in zip(sites['dec_long_va'], sites['dec_lat_va'])
            ]
            gdf = gpd.GeoDataFrame(sites, geometry=geometry, crs='EPSG:4326')
            
            return gdf
            
        except Exception as e:
            logger.error(f"Error querying NWIS for wells in {state_code}: {e}")
            return gpd.GeoDataFrame()

    def _get_site_info(
        self,
        site_numbers: List[str]
    ) -> gpd.GeoDataFrame:
        """Get site information for specific sites."""
        try:
            sites, _ = nwis.get_info(sites=site_numbers)
            
            if sites.empty:
                return gpd.GeoDataFrame()
            
            sites = sites.reset_index()
            geometry = [
                Point(lon, lat) 
                for lon, lat in zip(sites['dec_long_va'], sites['dec_lat_va'])
            ]
            gdf = gpd.GeoDataFrame(sites, geometry=geometry, crs='EPSG:4326')
            
            return gdf
            
        except Exception as e:
            logger.error(f"Error getting site info: {e}")
            return gpd.GeoDataFrame()

    def _clip_wells_to_geometry(
        self,
        wells: gpd.GeoDataFrame,
        geometry: Union[Polygon, MultiPolygon]
    ) -> gpd.GeoDataFrame:
        """Clip wells GeoDataFrame to a geometry."""
        mask = wells.geometry.within(geometry)
        return wells[mask].copy()

    def _get_data_for_wells(
        self,
        site_numbers: List[str],
        batch_size: int = 100,
        parallel: bool = True,
        n_workers: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get groundwater level data for a list of wells from all configured sources.

        Parameters
        ----------
        site_numbers : List[str]
            List of USGS site numbers.
        batch_size : int, optional
            Number of sites to query at once. Default is 100.
        parallel : bool, optional
            If True, process batches in parallel. Default is True.
        n_workers : int, optional
            Number of parallel workers.

        Returns
        -------
        pd.DataFrame
            Combined groundwater level data from all sources.
        """
        all_source_data = []
        
        for source in self.data_sources:
            logger.info(f"Retrieving data from source: {source}")
            
            try:
                source_data = self._get_data_from_source(
                    source=source,
                    site_numbers=site_numbers,
                    batch_size=batch_size,
                    parallel=parallel,
                    n_workers=n_workers
                )
                
                if not source_data.empty:
                    source_data['data_source'] = source
                    all_source_data.append(source_data)
                    logger.info(f"  Retrieved {len(source_data):,} records from {source}")
                else:
                    logger.info(f"  No data found from {source}")
                    
            except Exception as e:
                logger.warning(f"Error retrieving data from {source}: {e}")
                continue
        
        if not all_source_data:
            logger.warning("No groundwater level data retrieved from any source")
            return pd.DataFrame()
        
        # Combine all data
        combined = pd.concat(all_source_data, ignore_index=True)
        
        # Remove any duplicate columns that may have been created during concat
        combined = combined.loc[:, ~combined.columns.duplicated()]
        
        # Add well info
        if self.wells is not None and not self.wells.empty:
            well_info = self.wells[['site_no', 'station_nm', 'dec_lat_va', 'dec_long_va']].drop_duplicates(subset='site_no')
            # Only merge columns that don't already exist in combined
            existing_cols = set(combined.columns)
            cols_to_add = [c for c in ['station_nm', 'dec_lat_va', 'dec_long_va'] if c not in existing_cols]
            if cols_to_add:
                merge_cols = ['site_no'] + cols_to_add
                combined = combined.merge(well_info[merge_cols], on='site_no', how='left')
        
        logger.info(
            f"Retrieved {len(combined):,} total records from {combined['site_no'].nunique()} wells "
            f"across {len(self.data_sources)} data source(s)"
        )
        
        return combined

    def _get_data_from_source(
        self,
        source: str,
        site_numbers: List[str],
        batch_size: int = 100,
        parallel: bool = True,
        n_workers: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get groundwater data from a specific source.

        Parameters
        ----------
        source : str
            Data source: 'gwlevels', 'dv', or 'iv'.
        site_numbers : List[str]
            List of USGS site numbers.
        batch_size : int
            Number of sites to query at once.
        parallel : bool
            If True, process batches in parallel.
        n_workers : int, optional
            Number of parallel workers.

        Returns
        -------
        pd.DataFrame
            Data from the specified source.
        """
        # Create batches
        batches = [
            site_numbers[i:i + batch_size] 
            for i in range(0, len(site_numbers), batch_size)
        ]
        
        use_parallel = parallel and DASK_AVAILABLE and len(batches) > 1
        
        if use_parallel:
            logger.debug(f"Processing {len(batches)} batches in parallel for {source}")
            
            # Prepare arguments for parallel processing
            args_list = [
                (batch, self.start_date, self.end_date, source)
                for batch in batches
            ]
            
            # Process batches in parallel
            all_data = parallel_map(
                self._process_source_batch,
                args_list,
                n_workers=n_workers,
                show_progress=False,
                scheduler='threads'
            )
            
            # Filter out empty results
            all_data = [df for df in all_data if df is not None and not df.empty]
        else:
            all_data = []
            
            # Process in batches sequentially
            for i, batch in enumerate(batches):
                logger.debug(f"Retrieving {source} data for batch {i+1}/{len(batches)}")
                
                try:
                    data = self._fetch_source_data(source, batch)
                    
                    if data is not None and not data.empty:
                        all_data.append(data)
                        
                except Exception as e:
                    logger.warning(f"Error retrieving {source} data for batch: {e}")
                    continue
        
        if not all_data:
            return pd.DataFrame()
        
        return pd.concat(all_data, ignore_index=True)

    def _process_source_batch(self, args: tuple) -> pd.DataFrame:
        """Worker function for parallel batch processing."""
        batch, start_date, end_date, source = args
        
        # Temporarily set dates for this batch
        orig_start, orig_end = self.start_date, self.end_date
        self.start_date, self.end_date = start_date, end_date
        
        try:
            return self._fetch_source_data(source, batch)
        finally:
            self.start_date, self.end_date = orig_start, orig_end

    def _fetch_source_data(
        self,
        source: str,
        site_numbers: List[str]
    ) -> pd.DataFrame:
        """
        Fetch data from a specific USGS source.

        Parameters
        ----------
        source : str
            Data source: 'gwlevels', 'dv', or 'iv'.
        site_numbers : List[str]
            List of USGS site numbers.

        Returns
        -------
        pd.DataFrame
            Retrieved data, standardized with common columns.
        """
        try:
            if source == 'gwlevels':
                # Field groundwater-level measurements
                data, _ = nwis.get_gwlevels(
                    sites=site_numbers,
                    start=self.start_date,
                    end=self.end_date,
                    datetime_index=False
                )
                
                if not data.empty:
                    data = data.reset_index()
                    # Standardize date column
                    if 'lev_dt' in data.columns:
                        data['datetime'] = pd.to_datetime(data['lev_dt'], errors='coerce')
                    # Standardize value column
                    if 'lev_va' in data.columns:
                        data['value'] = data['lev_va']
                        
            elif source == 'dv':
                # Daily values
                data, _ = nwis.get_dv(
                    sites=site_numbers,
                    start=self.start_date,
                    end=self.end_date,
                    parameterCd=self.GW_LEVEL_PARAMS,
                    datetime_index=False
                )
                
                if not data.empty:
                    data = data.reset_index()
                    # Standardize columns
                    if 'datetime' not in data.columns and 'index' in data.columns:
                        data['datetime'] = pd.to_datetime(data['index'], errors='coerce')
                    # Find value column (could be named by parameter code)
                    value_cols = [c for c in data.columns if any(p in c for p in self.GW_LEVEL_PARAMS)]
                    if value_cols:
                        data['value'] = data[value_cols[0]]
                        data['lev_va'] = data[value_cols[0]]  # Also set lev_va for compatibility
                        
            elif source == 'iv':
                # Instantaneous values (current/historical observations)
                data, _ = nwis.get_iv(
                    sites=site_numbers,
                    start=self.start_date,
                    end=self.end_date,
                    parameterCd=self.GW_LEVEL_PARAMS,
                    datetime_index=False
                )
                
                if not data.empty:
                    data = data.reset_index()
                    # Standardize columns
                    if 'datetime' not in data.columns and 'index' in data.columns:
                        data['datetime'] = pd.to_datetime(data['index'], errors='coerce')
                    # Find value column
                    value_cols = [c for c in data.columns if any(p in c for p in self.GW_LEVEL_PARAMS)]
                    if value_cols:
                        data['value'] = data[value_cols[0]]
                        data['lev_va'] = data[value_cols[0]]  # Also set lev_va for compatibility
            else:
                logger.warning(f"Unknown data source: {source}")
                return pd.DataFrame()
            
            return data if data is not None and not data.empty else pd.DataFrame()
            
        except Exception as e:
            logger.debug(f"Error fetching {source} data: {e}")
            return pd.DataFrame()

    def get_available_sources_for_sites(
        self,
        site_numbers: List[str]
    ) -> Dict[str, List[str]]:
        """
        Check which data sources are available for given sites.

        Parameters
        ----------
        site_numbers : List[str]
            List of USGS site numbers.

        Returns
        -------
        Dict[str, List[str]]
            Dictionary mapping source names to lists of sites that have data.
        """
        available = {}
        
        for source in self.VALID_SOURCES:
            sites_with_data = []
            
            for site in site_numbers[:10]:  # Check first 10 sites as sample
                try:
                    data = self._fetch_source_data(source, [site])
                    if not data.empty:
                        sites_with_data.append(site)
                except Exception:
                    continue
            
            if sites_with_data:
                available[source] = sites_with_data
                logger.info(f"Source '{source}' has data for {len(sites_with_data)} sites (sample)")
        
        return available

    def to_csv(
        self,
        filepath: Union[str, Path],
        data: Optional[pd.DataFrame] = None
    ) -> None:
        """
        Save data to a CSV file.

        Parameters
        ----------
        filepath : Union[str, Path]
            Output file path.
        data : pd.DataFrame, optional
            Data to save. If None, uses self.data.
        """
        data = data if data is not None else self.data
        if data is None or data.empty:
            logger.warning("No data to save")
            return
        save_to_csv(data, filepath)

    def to_parquet(
        self,
        filepath: Union[str, Path],
        data: Optional[pd.DataFrame] = None
    ) -> None:
        """
        Save data to a Parquet file.

        Parameters
        ----------
        filepath : Union[str, Path]
            Output file path.
        data : pd.DataFrame, optional
            Data to save. If None, uses self.data.
        """
        data = data if data is not None else self.data
        if data is None or data.empty:
            logger.warning("No data to save")
            return
        save_to_parquet(data, filepath)

    def save_data_per_zipcode(
        self,
        output_dir: Union[str, Path],
        file_format: str = 'csv',
        prefix: str = 'gw_data',
        data: Optional[pd.DataFrame] = None
    ) -> Dict[str, Path]:
        """
        Save groundwater data to separate files for each zip code.

        This method is particularly useful after retrieving data using
        get_data_by_zipcodes_csv(), as it allows organizing the output
        by geographic area.

        Parameters
        ----------
        output_dir : Union[str, Path]
            Directory where files will be saved. Will be created if
            it doesn't exist.
        file_format : str, optional
            Output file format. Options are 'csv' or 'parquet'.
            Default is 'csv'.
        prefix : str, optional
            Prefix for output filenames. Files will be named as
            '{prefix}_{zipcode}.{format}'. Default is 'gw_data'.
        data : pd.DataFrame, optional
            Data to save. If None, uses self.data. Must contain a
            'source_zipcode' column.

        Returns
        -------
        Dict[str, Path]
            Dictionary mapping zip codes to their output file paths.

        Raises
        ------
        ValueError
            If the data doesn't contain a 'source_zipcode' column,
            or if an invalid file format is specified.

        Examples
        --------
        >>> gw = GroundwaterRetrieval()
        >>> data = gw.get_data_by_zipcodes_csv('locations.csv', zipcode_column='zip')
        >>> saved_files = gw.save_data_per_zipcode('output/', file_format='csv')
        >>> for zipcode, filepath in saved_files.items():
        ...     print(f"{zipcode}: {filepath}")
        
        >>> # Save as parquet with custom prefix
        >>> saved_files = gw.save_data_per_zipcode(
        ...     'output/',
        ...     file_format='parquet',
        ...     prefix='groundwater'
        ... )
        """
        data = data if data is not None else self.data
        
        if data is None or data.empty:
            logger.warning("No data to save")
            return {}
        
        if 'source_zipcode' not in data.columns:
            raise ValueError(
                "Data must contain a 'source_zipcode' column. "
                "Use get_data_by_zipcodes_csv() to retrieve data with zip code tracking, "
                "or manually add a 'source_zipcode' column to your data."
            )
        
        file_format = file_format.lower()
        if file_format not in ['csv', 'parquet']:
            raise ValueError(f"Invalid file format: {file_format}. Must be 'csv' or 'parquet'.")
        
        # Create output directory
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        saved_files = {}
        zipcodes = data['source_zipcode'].unique()
        
        logger.info(f"Saving data for {len(zipcodes)} zip codes to {output_dir}")
        
        for zipcode in zipcodes:
            zipcode_data = data[data['source_zipcode'] == zipcode]
            
            # Generate filename
            filename = f"{prefix}_{zipcode}.{file_format}"
            filepath = output_dir / filename
            
            # Save file
            if file_format == 'csv':
                save_to_csv(zipcode_data, filepath)
            else:
                save_to_parquet(zipcode_data, filepath)
            
            saved_files[str(zipcode)] = filepath
            logger.info(
                f"Saved {len(zipcode_data)} records for zip code {zipcode} to {filepath}"
            )
        
        logger.info(f"Successfully saved data for {len(saved_files)} zip codes")
        return saved_files

    def get_wells_geodataframe(self) -> gpd.GeoDataFrame:
        """
        Get the discovered wells as a GeoDataFrame.

        Returns
        -------
        gpd.GeoDataFrame
            GeoDataFrame of groundwater wells.
        """
        return self.wells

    def save_wells_to_file(
        self,
        filepath: Union[str, Path],
        driver: str = 'GeoJSON'
    ) -> None:
        """
        Save wells to a geospatial file.

        Parameters
        ----------
        filepath : Union[str, Path]
            Output file path.
        driver : str, optional
            Output format driver. Default is 'GeoJSON'.
            Options include 'GeoJSON', 'ESRI Shapefile', 'GPKG'.
        """
        if self.wells is None or self.wells.empty:
            logger.warning("No wells to save")
            return
        
        self.wells.to_file(filepath, driver=driver)
        logger.info(f"Saved {len(self.wells)} wells to {filepath}")

    def get_data_summary(self) -> dict:
        """
        Get a summary of the retrieved data.

        Returns
        -------
        dict
            Dictionary containing data summary statistics.
        """
        if self.data is None or self.data.empty:
            return {'status': 'No data available'}
        
        summary = {
            'total_records': len(self.data),
            'num_wells': self.data['site_no'].nunique(),
            'date_range': {
                'start': str(self.data['lev_dt'].min()) if 'lev_dt' in self.data.columns else None,
                'end': str(self.data['lev_dt'].max()) if 'lev_dt' in self.data.columns else None,
            },
            'wells': self.data['site_no'].unique().tolist() if len(self.data['site_no'].unique()) <= 20 else f"{self.data['site_no'].nunique()} wells",
        }
        
        return summary
