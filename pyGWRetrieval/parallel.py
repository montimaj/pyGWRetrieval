"""
Parallel processing module for pyGWRetrieval.

This module provides utilities for parallelizing data retrieval and processing
operations using Dask. It supports:
- Parallel retrieval of data from multiple zip codes
- Parallel batch processing of wells
- Dask DataFrame operations for large datasets
- Parallel file I/O operations

Dependencies:
    - dask
    - dask[distributed] (optional, for distributed computing)
"""

import logging
import warnings
from pathlib import Path
from typing import Optional, Union, List, Dict, Callable, Any, Tuple
from functools import partial

import pandas as pd
import numpy as np

# Suppress warnings from dataretrieval/pandas about mixed types and incomplete dates
warnings.filterwarnings('ignore', message='.*mixed types.*', category=pd.errors.DtypeWarning)
warnings.filterwarnings('ignore', message='.*incomplete dates.*', category=UserWarning)

logger = logging.getLogger(__name__)

# Try to import Dask - handle gracefully if not installed
try:
    import dask
    import dask.dataframe as dd
    from dask import delayed, compute
    from dask.diagnostics import ProgressBar
    DASK_AVAILABLE = True
except ImportError:
    DASK_AVAILABLE = False
    logger.warning(
        "Dask is not installed. Parallel processing features will be disabled. "
        "Install with: pip install dask[complete]"
    )


def check_dask_available() -> bool:
    """Check if Dask is available for parallel processing."""
    return DASK_AVAILABLE


def get_dask_client(
    n_workers: Optional[int] = None,
    threads_per_worker: int = 2,
    memory_limit: str = 'auto'
) -> Optional[Any]:
    """
    Get or create a Dask distributed client.

    Parameters
    ----------
    n_workers : int, optional
        Number of worker processes. Default is number of CPU cores.
    threads_per_worker : int, optional
        Threads per worker. Default is 2.
    memory_limit : str, optional
        Memory limit per worker. Default is 'auto'.

    Returns
    -------
    Optional[Client]
        Dask distributed client, or None if distributed is not available.
    """
    if not DASK_AVAILABLE:
        return None
    
    try:
        from dask.distributed import Client, LocalCluster
        
        cluster = LocalCluster(
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
            memory_limit=memory_limit
        )
        client = Client(cluster)
        logger.info(f"Created Dask client with {len(client.scheduler_info()['workers'])} workers")
        logger.info(f"Dashboard available at: {client.dashboard_link}")
        return client
    except ImportError:
        logger.info("Dask distributed not available, using threaded scheduler")
        return None
    except Exception as e:
        logger.warning(f"Could not create distributed client: {e}")
        return None


def parallel_map(
    func: Callable,
    items: List[Any],
    n_workers: Optional[int] = None,
    show_progress: bool = True,
    scheduler: str = 'threads'
) -> List[Any]:
    """
    Apply a function to items in parallel using Dask.

    Parameters
    ----------
    func : Callable
        Function to apply to each item.
    items : List[Any]
        List of items to process.
    n_workers : int, optional
        Number of parallel workers.
    show_progress : bool, optional
        Show progress bar. Default is True.
    scheduler : str, optional
        Dask scheduler to use ('threads', 'processes', 'synchronous').
        Default is 'threads'.

    Returns
    -------
    List[Any]
        Results from applying func to each item.

    Examples
    --------
    >>> results = parallel_map(process_zipcode, zipcodes, n_workers=4)
    """
    if not DASK_AVAILABLE:
        logger.warning("Dask not available, falling back to sequential processing")
        return [func(item) for item in items]
    
    # Create delayed tasks
    tasks = [delayed(func)(item) for item in items]
    
    # Compute with progress bar
    if show_progress:
        with ProgressBar():
            results = compute(*tasks, scheduler=scheduler, num_workers=n_workers)
    else:
        results = compute(*tasks, scheduler=scheduler, num_workers=n_workers)
    
    return list(results)


def parallel_batch_process(
    func: Callable,
    items: List[Any],
    batch_size: int = 10,
    n_workers: Optional[int] = None,
    show_progress: bool = True,
    scheduler: str = 'threads'
) -> List[Any]:
    """
    Process items in batches with parallel execution.

    Parameters
    ----------
    func : Callable
        Function to apply to each batch.
    items : List[Any]
        List of items to process.
    batch_size : int, optional
        Size of each batch. Default is 10.
    n_workers : int, optional
        Number of parallel workers.
    show_progress : bool, optional
        Show progress bar. Default is True.
    scheduler : str, optional
        Dask scheduler. Default is 'threads'.

    Returns
    -------
    List[Any]
        Flattened results from all batches.
    """
    if not DASK_AVAILABLE:
        logger.warning("Dask not available, falling back to sequential processing")
        results = []
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            results.extend(func(batch))
        return results
    
    # Create batches
    batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
    
    # Create delayed tasks for each batch
    tasks = [delayed(func)(batch) for batch in batches]
    
    # Compute
    if show_progress:
        with ProgressBar():
            batch_results = compute(*tasks, scheduler=scheduler, num_workers=n_workers)
    else:
        batch_results = compute(*tasks, scheduler=scheduler, num_workers=n_workers)
    
    # Flatten results
    results = []
    for batch_result in batch_results:
        if isinstance(batch_result, list):
            results.extend(batch_result)
        else:
            results.append(batch_result)
    
    return results


def to_dask_dataframe(
    df: pd.DataFrame,
    npartitions: Optional[int] = None
) -> Union[pd.DataFrame, 'dd.DataFrame']:
    """
    Convert pandas DataFrame to Dask DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input pandas DataFrame.
    npartitions : int, optional
        Number of partitions. Default is based on data size.

    Returns
    -------
    Union[pd.DataFrame, dd.DataFrame]
        Dask DataFrame if Dask available, otherwise original DataFrame.
    """
    if not DASK_AVAILABLE:
        return df
    
    if npartitions is None:
        # Estimate partitions based on data size
        npartitions = max(1, len(df) // 100000)  # ~100k rows per partition
    
    return dd.from_pandas(df, npartitions=npartitions)


def from_dask_dataframe(ddf: Union[pd.DataFrame, 'dd.DataFrame']) -> pd.DataFrame:
    """
    Convert Dask DataFrame back to pandas DataFrame.

    Parameters
    ----------
    ddf : Union[pd.DataFrame, dd.DataFrame]
        Input DataFrame (pandas or Dask).

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame.
    """
    if not DASK_AVAILABLE:
        return ddf
    
    if isinstance(ddf, dd.DataFrame):
        return ddf.compute()
    return ddf


def parallel_groupby_apply(
    df: pd.DataFrame,
    groupby_cols: Union[str, List[str]],
    func: Callable,
    meta: Optional[pd.DataFrame] = None,
    **kwargs
) -> pd.DataFrame:
    """
    Parallel groupby-apply operation using Dask.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    groupby_cols : Union[str, List[str]]
        Columns to group by.
    func : Callable
        Function to apply to each group.
    meta : pd.DataFrame, optional
        Example output DataFrame for Dask.
    **kwargs
        Additional arguments passed to func.

    Returns
    -------
    pd.DataFrame
        Result of groupby-apply operation.
    """
    if not DASK_AVAILABLE or len(df) < 10000:
        # Use pandas for small datasets
        return df.groupby(groupby_cols).apply(func, **kwargs).reset_index(drop=True)
    
    # Convert to Dask DataFrame
    ddf = to_dask_dataframe(df)
    
    # Apply function
    if meta is not None:
        result = ddf.groupby(groupby_cols).apply(func, meta=meta, **kwargs)
    else:
        result = ddf.groupby(groupby_cols).apply(func, **kwargs)
    
    return result.compute().reset_index(drop=True)


def parallel_read_csv_files(
    filepaths: List[Union[str, Path]],
    n_workers: Optional[int] = None,
    **kwargs
) -> pd.DataFrame:
    """
    Read multiple CSV files in parallel and concatenate.

    Parameters
    ----------
    filepaths : List[Union[str, Path]]
        List of CSV file paths.
    n_workers : int, optional
        Number of parallel workers.
    **kwargs
        Additional arguments passed to pd.read_csv.

    Returns
    -------
    pd.DataFrame
        Concatenated DataFrame from all files.
    """
    if not DASK_AVAILABLE:
        dfs = [pd.read_csv(fp, **kwargs) for fp in filepaths]
        return pd.concat(dfs, ignore_index=True)
    
    @delayed
    def read_file(filepath):
        return pd.read_csv(filepath, **kwargs)
    
    tasks = [read_file(fp) for fp in filepaths]
    
    with ProgressBar():
        dfs = compute(*tasks, scheduler='threads', num_workers=n_workers)
    
    return pd.concat(dfs, ignore_index=True)


def parallel_save_dataframes(
    data_dict: Dict[str, pd.DataFrame],
    output_dir: Union[str, Path],
    file_format: str = 'csv',
    prefix: str = 'data',
    n_workers: Optional[int] = None
) -> Dict[str, Path]:
    """
    Save multiple DataFrames to files in parallel.

    Parameters
    ----------
    data_dict : Dict[str, pd.DataFrame]
        Dictionary mapping names to DataFrames.
    output_dir : Union[str, Path]
        Output directory.
    file_format : str, optional
        Output format ('csv' or 'parquet'). Default is 'csv'.
    prefix : str, optional
        Filename prefix. Default is 'data'.
    n_workers : int, optional
        Number of parallel workers.

    Returns
    -------
    Dict[str, Path]
        Dictionary mapping names to output file paths.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    def save_single(name_df_tuple: Tuple[str, pd.DataFrame]) -> Tuple[str, Path]:
        name, df = name_df_tuple
        filename = f"{prefix}_{name}.{file_format}"
        filepath = output_dir / filename
        
        if file_format == 'csv':
            df.to_csv(filepath, index=False)
        elif file_format == 'parquet':
            df.to_parquet(filepath, index=False)
        
        return name, filepath
    
    if not DASK_AVAILABLE:
        results = [save_single((name, df)) for name, df in data_dict.items()]
    else:
        tasks = [delayed(save_single)((name, df)) for name, df in data_dict.items()]
        
        with ProgressBar():
            results = compute(*tasks, scheduler='threads', num_workers=n_workers)
    
    return dict(results)


class ParallelRetrieval:
    """
    Mixin class providing parallel data retrieval capabilities.

    This class provides methods for parallel processing of multiple
    geographic queries (zip codes, sites, etc.).
    """

    def _process_single_zipcode(
        self,
        zipcode: str,
        buffer_miles: float,
        country: str,
        start_date: str,
        end_date: str
    ) -> Tuple[str, Optional[pd.DataFrame], Optional[Any]]:
        """
        Process a single zip code and return results.

        This is a standalone function suitable for parallel execution.
        """
        from .retrieval import GroundwaterRetrieval
        
        try:
            gw = GroundwaterRetrieval(start_date=start_date, end_date=end_date)
            data = gw.get_data_by_zipcode(
                zipcode=str(zipcode).zfill(5),
                buffer_miles=buffer_miles,
                country=country
            )
            
            if not data.empty:
                data['source_zipcode'] = zipcode
                wells = gw.wells.copy() if gw.wells is not None else None
                if wells is not None:
                    wells['source_zipcode'] = zipcode
                return zipcode, data, wells
            else:
                return zipcode, None, None
                
        except Exception as e:
            logger.error(f"Error processing zip code {zipcode}: {e}")
            return zipcode, None, None


def process_zipcode_worker(
    args: Tuple[str, float, str, str, str, Union[str, List[str]]]
) -> Tuple[str, Optional[pd.DataFrame], Optional[Any]]:
    """
    Worker function for parallel zip code processing.

    Parameters
    ----------
    args : Tuple
        Tuple of (zipcode, buffer_miles, country, start_date, end_date, data_sources)

    Returns
    -------
    Tuple[str, Optional[pd.DataFrame], Optional[Any]]
        Tuple of (zipcode, data, wells)
    """
    # Import here to avoid circular imports
    from .retrieval import GroundwaterRetrieval
    
    # Handle both old (5 args) and new (6 args) formats
    if len(args) == 5:
        zipcode, buffer_miles, country, start_date, end_date = args
        data_sources = 'all'
    else:
        zipcode, buffer_miles, country, start_date, end_date, data_sources = args
    
    try:
        gw = GroundwaterRetrieval(
            start_date=start_date, 
            end_date=end_date,
            data_sources=data_sources
        )
        data = gw.get_data_by_zipcode(
            zipcode=str(zipcode).zfill(5),
            buffer_miles=buffer_miles,
            country=country
        )
        
        if not data.empty:
            data['source_zipcode'] = zipcode
            wells = gw.wells.copy() if gw.wells is not None else None
            if wells is not None:
                wells['source_zipcode'] = zipcode
            return zipcode, data, wells
        else:
            logger.warning(f"No data found for zip code {zipcode}")
            return zipcode, None, None
            
    except Exception as e:
        logger.error(f"Error processing zip code {zipcode}: {e}")
        return zipcode, None, None


def process_site_batch_worker(
    args: Tuple[List[str], str, str, Optional[str]]
) -> pd.DataFrame:
    """
    Worker function for parallel site batch processing.

    Parameters
    ----------
    args : Tuple
        Tuple of (site_numbers, start_date, end_date, source)
        source can be 'gwlevels', 'dv', or 'iv'

    Returns
    -------
    pd.DataFrame
        Groundwater level data for the batch.
    """
    import dataretrieval.nwis as nwis
    
    # Handle args with or without source
    if len(args) == 3:
        site_numbers, start_date, end_date = args
        source = 'gwlevels'
    else:
        site_numbers, start_date, end_date, source = args
    
    GW_LEVEL_PARAMS = ['72019', '72020', '62610', '62611']
    
    try:
        if source == 'gwlevels':
            data, _ = nwis.get_gwlevels(
                sites=site_numbers,
                start=start_date,
                end=end_date,
                datetime_index=False
            )
            if not data.empty:
                data = data.reset_index()
                if 'lev_dt' in data.columns:
                    data['datetime'] = pd.to_datetime(data['lev_dt'], errors='coerce')
                if 'lev_va' in data.columns:
                    data['value'] = data['lev_va']
                    
        elif source == 'dv':
            data, _ = nwis.get_dv(
                sites=site_numbers,
                start=start_date,
                end=end_date,
                parameterCd=GW_LEVEL_PARAMS,
                datetime_index=False
            )
            if not data.empty:
                data = data.reset_index()
                value_cols = [c for c in data.columns if any(p in c for p in GW_LEVEL_PARAMS)]
                if value_cols:
                    data['value'] = data[value_cols[0]]
                    data['lev_va'] = data[value_cols[0]]
                    
        elif source == 'iv':
            data, _ = nwis.get_iv(
                sites=site_numbers,
                start=start_date,
                end=end_date,
                parameterCd=GW_LEVEL_PARAMS,
                datetime_index=False
            )
            if not data.empty:
                data = data.reset_index()
                value_cols = [c for c in data.columns if any(p in c for p in GW_LEVEL_PARAMS)]
                if value_cols:
                    data['value'] = data[value_cols[0]]
                    data['lev_va'] = data[value_cols[0]]
        else:
            return pd.DataFrame()
        
        if data is not None and not data.empty:
            data['data_source'] = source
            return data
        return pd.DataFrame()
        
    except Exception as e:
        logger.warning(f"Error retrieving {source} data for site batch: {e}")
        return pd.DataFrame()


def get_parallel_config() -> Dict[str, Any]:
    """
    Get current parallel processing configuration.

    Returns
    -------
    Dict[str, Any]
        Configuration dictionary with Dask settings.
    """
    config = {
        'dask_available': DASK_AVAILABLE,
        'scheduler': 'threads',
        'num_workers': None,  # Auto-detect
    }
    
    if DASK_AVAILABLE:
        try:
            import multiprocessing
            config['num_workers'] = multiprocessing.cpu_count()
            config['dask_version'] = dask.__version__
        except Exception:
            pass
    
    return config
