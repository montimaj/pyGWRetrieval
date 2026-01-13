"""
Utility functions for pyGWRetrieval.

This module provides helper functions for:
- File I/O operations (CSV, Parquet)
- Date validation
- Logging configuration
- Data validation and cleaning
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)


def save_to_csv(
    data: pd.DataFrame,
    filepath: Union[str, Path],
    index: bool = False,
    **kwargs
) -> None:
    """
    Save DataFrame to a CSV file.

    Parameters
    ----------
    data : pd.DataFrame
        Data to save.
    filepath : Union[str, Path]
        Output file path.
    index : bool, optional
        Include index in output. Default is False.
    **kwargs
        Additional arguments passed to pd.DataFrame.to_csv().

    Examples
    --------
    >>> save_to_csv(gw_data, 'output/groundwater.csv')
    """
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    data.to_csv(filepath, index=index, **kwargs)
    logger.info(f"Saved {len(data)} records to {filepath}")


def save_to_parquet(
    data: pd.DataFrame,
    filepath: Union[str, Path],
    compression: str = 'snappy',
    **kwargs
) -> None:
    """
    Save DataFrame to a Parquet file.

    Parameters
    ----------
    data : pd.DataFrame
        Data to save.
    filepath : Union[str, Path]
        Output file path.
    compression : str, optional
        Compression algorithm. Default is 'snappy'.
        Options: 'snappy', 'gzip', 'brotli', None.
    **kwargs
        Additional arguments passed to pd.DataFrame.to_parquet().

    Examples
    --------
    >>> save_to_parquet(gw_data, 'output/groundwater.parquet')
    """
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    data.to_parquet(filepath, compression=compression, **kwargs)
    logger.info(f"Saved {len(data)} records to {filepath}")


def load_from_csv(
    filepath: Union[str, Path],
    parse_dates: Optional[list] = None,
    **kwargs
) -> pd.DataFrame:
    """
    Load DataFrame from a CSV file.

    Parameters
    ----------
    filepath : Union[str, Path]
        Input file path.
    parse_dates : list, optional
        Columns to parse as dates.
    **kwargs
        Additional arguments passed to pd.read_csv().

    Returns
    -------
    pd.DataFrame
        Loaded data.
    """
    filepath = Path(filepath)
    
    if not filepath.exists():
        raise FileNotFoundError(f"CSV file not found: {filepath}")
    
    if parse_dates is None:
        parse_dates = ['lev_dt']
    
    data = pd.read_csv(filepath, parse_dates=parse_dates, **kwargs)
    logger.info(f"Loaded {len(data)} records from {filepath}")
    
    return data


def load_from_parquet(
    filepath: Union[str, Path],
    **kwargs
) -> pd.DataFrame:
    """
    Load DataFrame from a Parquet file.

    Parameters
    ----------
    filepath : Union[str, Path]
        Input file path.
    **kwargs
        Additional arguments passed to pd.read_parquet().

    Returns
    -------
    pd.DataFrame
        Loaded data.
    """
    filepath = Path(filepath)
    
    if not filepath.exists():
        raise FileNotFoundError(f"Parquet file not found: {filepath}")
    
    data = pd.read_parquet(filepath, **kwargs)
    logger.info(f"Loaded {len(data)} records from {filepath}")
    
    return data


def validate_date_range(
    start_date: str,
    end_date: str,
    date_format: str = '%Y-%m-%d'
) -> bool:
    """
    Validate that date range is valid.

    Parameters
    ----------
    start_date : str
        Start date string.
    end_date : str
        End date string.
    date_format : str, optional
        Expected date format. Default is '%Y-%m-%d'.

    Returns
    -------
    bool
        True if valid.

    Raises
    ------
    ValueError
        If dates are invalid or start_date > end_date.

    Examples
    --------
    >>> validate_date_range('2020-01-01', '2023-12-31')
    True
    """
    try:
        start = datetime.strptime(start_date, date_format)
        end = datetime.strptime(end_date, date_format)
    except ValueError as e:
        raise ValueError(f"Invalid date format. Expected {date_format}. Error: {e}")
    
    if start > end:
        raise ValueError(f"Start date ({start_date}) must be before end date ({end_date})")
    
    return True


def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[Union[str, Path]] = None,
    format_string: Optional[str] = None
) -> None:
    """
    Configure logging for the package.

    Parameters
    ----------
    level : int, optional
        Logging level. Default is logging.INFO.
    log_file : Union[str, Path], optional
        Path to log file. If None, logs to console only.
    format_string : str, optional
        Custom format string for log messages.

    Examples
    --------
    >>> setup_logging(level=logging.DEBUG, log_file='pyGWRetrieval.log')
    """
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Configure root logger for the package
    package_logger = logging.getLogger('pyGWRetrieval')
    package_logger.setLevel(level)
    
    # Clear existing handlers
    package_logger.handlers = []
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter(format_string))
    package_logger.addHandler(console_handler)
    
    # File handler if specified
    if log_file is not None:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(logging.Formatter(format_string))
        package_logger.addHandler(file_handler)
    
    logger.info("Logging configured")


def clean_data(
    data: pd.DataFrame,
    value_column: str = 'lev_va',
    drop_na: bool = True,
    remove_negative: bool = False,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None
) -> pd.DataFrame:
    """
    Clean groundwater data by removing invalid values.

    Parameters
    ----------
    data : pd.DataFrame
        Input data to clean.
    value_column : str, optional
        Name of the value column. Default is 'lev_va'.
    drop_na : bool, optional
        Drop rows with NaN values. Default is True.
    remove_negative : bool, optional
        Remove negative values. Default is False.
    min_value : float, optional
        Minimum acceptable value.
    max_value : float, optional
        Maximum acceptable value.

    Returns
    -------
    pd.DataFrame
        Cleaned data.

    Examples
    --------
    >>> clean = clean_data(gw_data, min_value=0, max_value=1000)
    """
    cleaned = data.copy()
    original_len = len(cleaned)
    
    # Drop NaN values
    if drop_na:
        cleaned = cleaned.dropna(subset=[value_column])
    
    # Remove negative values
    if remove_negative:
        cleaned = cleaned[cleaned[value_column] >= 0]
    
    # Apply min/max filters
    if min_value is not None:
        cleaned = cleaned[cleaned[value_column] >= min_value]
    
    if max_value is not None:
        cleaned = cleaned[cleaned[value_column] <= max_value]
    
    removed = original_len - len(cleaned)
    if removed > 0:
        logger.info(f"Removed {removed} records during cleaning")
    
    return cleaned


def get_data_coverage(
    data: pd.DataFrame,
    site_column: str = 'site_no',
    date_column: str = 'lev_dt'
) -> pd.DataFrame:
    """
    Calculate data coverage statistics for each well.

    Parameters
    ----------
    data : pd.DataFrame
        Input groundwater data.
    site_column : str, optional
        Site column name. Default is 'site_no'.
    date_column : str, optional
        Date column name. Default is 'lev_dt'.

    Returns
    -------
    pd.DataFrame
        Coverage statistics for each well including:
        - first_date: First measurement date
        - last_date: Last measurement date
        - n_records: Number of records
        - date_range_days: Total days between first and last measurement
        - coverage_pct: Percentage of days with data
    """
    if not pd.api.types.is_datetime64_any_dtype(data[date_column]):
        data = data.copy()
        data[date_column] = pd.to_datetime(data[date_column])
    
    coverage = data.groupby(site_column).agg(
        first_date=(date_column, 'min'),
        last_date=(date_column, 'max'),
        n_records=(date_column, 'count'),
        n_unique_dates=(date_column, 'nunique')
    ).reset_index()
    
    coverage['date_range_days'] = (
        coverage['last_date'] - coverage['first_date']
    ).dt.days + 1
    
    coverage['coverage_pct'] = (
        coverage['n_unique_dates'] / coverage['date_range_days'] * 100
    ).round(2)
    
    return coverage


def merge_with_site_info(
    data: pd.DataFrame,
    site_info: pd.DataFrame,
    site_column: str = 'site_no'
) -> pd.DataFrame:
    """
    Merge groundwater data with site information.

    Parameters
    ----------
    data : pd.DataFrame
        Groundwater level data.
    site_info : pd.DataFrame
        Site information (from wells GeoDataFrame).
    site_column : str, optional
        Site column name. Default is 'site_no'.

    Returns
    -------
    pd.DataFrame
        Merged data with site information.
    """
    # Select relevant columns from site info
    info_cols = [site_column]
    optional_cols = ['station_nm', 'dec_lat_va', 'dec_long_va', 
                    'alt_va', 'well_depth_va', 'hole_depth_va']
    
    for col in optional_cols:
        if col in site_info.columns:
            info_cols.append(col)
    
    site_subset = site_info[info_cols].drop_duplicates()
    
    merged = data.merge(site_subset, on=site_column, how='left')
    
    return merged


def filter_by_data_availability(
    data: pd.DataFrame,
    min_records: int = 10,
    min_years: int = 1,
    site_column: str = 'site_no',
    date_column: str = 'lev_dt'
) -> pd.DataFrame:
    """
    Filter data to keep only wells with sufficient data.

    Parameters
    ----------
    data : pd.DataFrame
        Input groundwater data.
    min_records : int, optional
        Minimum number of records required. Default is 10.
    min_years : int, optional
        Minimum years of data required. Default is 1.
    site_column : str, optional
        Site column name. Default is 'site_no'.
    date_column : str, optional
        Date column name. Default is 'lev_dt'.

    Returns
    -------
    pd.DataFrame
        Filtered data.
    """
    coverage = get_data_coverage(data, site_column, date_column)
    
    # Calculate years of data
    coverage['years'] = coverage['date_range_days'] / 365.25
    
    # Filter
    valid_sites = coverage[
        (coverage['n_records'] >= min_records) &
        (coverage['years'] >= min_years)
    ][site_column].tolist()
    
    filtered = data[data[site_column].isin(valid_sites)]
    
    removed_sites = len(coverage) - len(valid_sites)
    if removed_sites > 0:
        logger.info(
            f"Filtered out {removed_sites} wells with insufficient data "
            f"(< {min_records} records or < {min_years} years)"
        )
    
    return filtered
