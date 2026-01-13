"""
Temporal aggregation module for pyGWRetrieval.

This module provides functions and classes for temporal aggregation of
groundwater level data including:
- Monthly aggregation
- Annual aggregation
- Growing season aggregation
- Custom month/period aggregation
- Parallel processing support via Dask for large datasets

Dependencies:
    - pandas
    - numpy
    - dask (optional, for parallel processing)
"""

import logging
from typing import Optional, Union, List, Dict, Callable
from datetime import datetime

import pandas as pd
import numpy as np

# Try to import Dask for parallel processing
try:
    import dask.dataframe as dd
    from dask import delayed, compute
    from dask.diagnostics import ProgressBar
    DASK_AVAILABLE = True
except ImportError:
    DASK_AVAILABLE = False

logger = logging.getLogger(__name__)


class TemporalAggregator:
    """
    Class for temporal aggregation of groundwater level data.

    This class provides methods to aggregate groundwater level data to
    different temporal resolutions including monthly, annual, growing
    season, and custom periods.

    Parameters
    ----------
    data : pd.DataFrame
        Input groundwater level data with columns including:
        - site_no: USGS site number
        - lev_dt: Date of measurement
        - lev_va: Water level value (depth to water)
    date_column : str, optional
        Name of the date column. Default is 'lev_dt'.
    value_column : str, optional
        Name of the value column. Default is 'lev_va'.
    site_column : str, optional
        Name of the site identifier column. Default is 'site_no'.

    Attributes
    ----------
    data : pd.DataFrame
        Input data with parsed dates.
    date_column : str
        Name of the date column.
    value_column : str
        Name of the value column.
    site_column : str
        Name of the site column.

    Examples
    --------
    >>> from pyGWRetrieval import TemporalAggregator
    >>> aggregator = TemporalAggregator(gw_data)
    >>> monthly = aggregator.to_monthly()
    >>> annual = aggregator.to_annual()
    >>> growing_season = aggregator.to_growing_season(start_month=4, end_month=9)
    """

    # Pre-defined growing season periods by region
    GROWING_SEASONS = {
        'northern_hemisphere': {'start_month': 4, 'end_month': 10},
        'southern_hemisphere': {'start_month': 10, 'end_month': 4},
        'western_us': {'start_month': 4, 'end_month': 9},
        'midwest_us': {'start_month': 5, 'end_month': 9},
        'southern_us': {'start_month': 3, 'end_month': 11},
    }

    def __init__(
        self,
        data: pd.DataFrame,
        date_column: str = 'lev_dt',
        value_column: str = 'lev_va',
        site_column: str = 'site_no'
    ):
        """Initialize the TemporalAggregator."""
        self.data = data.copy()
        self.date_column = date_column
        self.value_column = value_column
        self.site_column = site_column
        
        self._prepare_data()
        
        logger.info(f"Initialized TemporalAggregator with {len(self.data)} records")

    def _prepare_data(self) -> None:
        """Prepare data by ensuring date column is datetime type."""
        if self.date_column not in self.data.columns:
            raise ValueError(f"Date column '{self.date_column}' not found in data")
        
        if self.value_column not in self.data.columns:
            raise ValueError(f"Value column '{self.value_column}' not found in data")
        
        if self.site_column not in self.data.columns:
            raise ValueError(f"Site column '{self.site_column}' not found in data")
        
        # Convert to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(self.data[self.date_column]):
            self.data[self.date_column] = pd.to_datetime(
                self.data[self.date_column], format='mixed', errors='coerce'
            )
        
        # Ensure value column is numeric
        self.data[self.value_column] = pd.to_numeric(
            self.data[self.value_column], errors='coerce'
        )
        
        # Add helper columns
        self.data['year'] = self.data[self.date_column].dt.year
        self.data['month'] = self.data[self.date_column].dt.month
        self.data['day_of_year'] = self.data[self.date_column].dt.dayofyear
        self.data['week'] = self.data[self.date_column].dt.isocalendar().week

    def to_monthly(
        self,
        agg_func: Union[str, Callable] = 'mean',
        include_count: bool = True
    ) -> pd.DataFrame:
        """
        Aggregate data to monthly resolution.

        Parameters
        ----------
        agg_func : Union[str, Callable], optional
            Aggregation function. Can be 'mean', 'median', 'min', 'max',
            or a custom callable. Default is 'mean'.
        include_count : bool, optional
            Include count of observations per month. Default is True.

        Returns
        -------
        pd.DataFrame
            Monthly aggregated data with columns:
            - site_no: Site identifier
            - year: Year
            - month: Month (1-12)
            - value: Aggregated water level
            - count: Number of observations (if include_count=True)

        Examples
        --------
        >>> monthly = aggregator.to_monthly(agg_func='mean')
        >>> monthly_median = aggregator.to_monthly(agg_func='median')
        """
        logger.info(f"Aggregating to monthly using {agg_func}")
        
        group_cols = [self.site_column, 'year', 'month']
        
        agg_dict = {self.value_column: agg_func}
        if include_count:
            agg_dict[self.value_column + '_count'] = 'count'
        
        # Need to aggregate differently to include count
        if include_count:
            result = self.data.groupby(group_cols).agg(
                value=(self.value_column, agg_func),
                count=(self.value_column, 'count')
            ).reset_index()
        else:
            result = self.data.groupby(group_cols).agg(
                value=(self.value_column, agg_func)
            ).reset_index()
        
        # Add date column for convenience
        result['date'] = pd.to_datetime(
            result['year'].astype(str) + '-' + result['month'].astype(str) + '-01'
        )
        
        logger.info(f"Created {len(result)} monthly records")
        
        return result

    def to_annual(
        self,
        agg_func: Union[str, Callable] = 'mean',
        include_count: bool = True,
        water_year: bool = False,
        water_year_start_month: int = 10
    ) -> pd.DataFrame:
        """
        Aggregate data to annual resolution.

        Parameters
        ----------
        agg_func : Union[str, Callable], optional
            Aggregation function. Default is 'mean'.
        include_count : bool, optional
            Include count of observations per year. Default is True.
        water_year : bool, optional
            Use water year instead of calendar year. Default is False.
        water_year_start_month : int, optional
            Starting month for water year (1-12). Default is 10 (October).

        Returns
        -------
        pd.DataFrame
            Annual aggregated data with columns:
            - site_no: Site identifier
            - year: Year (or water_year if water_year=True)
            - value: Aggregated water level
            - count: Number of observations (if include_count=True)

        Examples
        --------
        >>> annual = aggregator.to_annual()
        >>> water_year = aggregator.to_annual(water_year=True)
        """
        logger.info(f"Aggregating to annual using {agg_func}")
        
        data = self.data.copy()
        
        if water_year:
            # Calculate water year
            data['water_year'] = data.apply(
                lambda x: x['year'] + 1 if x['month'] >= water_year_start_month else x['year'],
                axis=1
            )
            year_col = 'water_year'
        else:
            year_col = 'year'
        
        group_cols = [self.site_column, year_col]
        
        if include_count:
            result = data.groupby(group_cols).agg(
                value=(self.value_column, agg_func),
                count=(self.value_column, 'count')
            ).reset_index()
        else:
            result = data.groupby(group_cols).agg(
                value=(self.value_column, agg_func)
            ).reset_index()
        
        # Rename column if water year
        if water_year:
            result = result.rename(columns={'water_year': 'year'})
        
        logger.info(f"Created {len(result)} annual records")
        
        return result

    def to_growing_season(
        self,
        start_month: int = 4,
        end_month: int = 9,
        agg_func: Union[str, Callable] = 'mean',
        include_count: bool = True,
        region: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Aggregate data to growing season periods.

        Parameters
        ----------
        start_month : int, optional
            Starting month of growing season (1-12). Default is 4 (April).
        end_month : int, optional
            Ending month of growing season (1-12). Default is 9 (September).
        agg_func : Union[str, Callable], optional
            Aggregation function. Default is 'mean'.
        include_count : bool, optional
            Include count of observations. Default is True.
        region : str, optional
            Pre-defined region for growing season. Options:
            'northern_hemisphere', 'southern_hemisphere', 'western_us',
            'midwest_us', 'southern_us'. Overrides start_month/end_month.

        Returns
        -------
        pd.DataFrame
            Growing season aggregated data.

        Examples
        --------
        >>> growing = aggregator.to_growing_season(start_month=4, end_month=9)
        >>> western_growing = aggregator.to_growing_season(region='western_us')
        """
        if region is not None:
            if region not in self.GROWING_SEASONS:
                raise ValueError(
                    f"Unknown region '{region}'. Options: {list(self.GROWING_SEASONS.keys())}"
                )
            start_month = self.GROWING_SEASONS[region]['start_month']
            end_month = self.GROWING_SEASONS[region]['end_month']
        
        logger.info(f"Aggregating to growing season (months {start_month}-{end_month})")
        
        data = self.data.copy()
        
        # Handle growing seasons that span year boundary
        if start_month <= end_month:
            # Normal case: Apr-Sep
            mask = (data['month'] >= start_month) & (data['month'] <= end_month)
            data = data[mask].copy()
            data['season_year'] = data['year']
        else:
            # Spans year boundary: Oct-Apr
            mask = (data['month'] >= start_month) | (data['month'] <= end_month)
            data = data[mask].copy()
            # Assign to the year when the season ends
            data['season_year'] = data.apply(
                lambda x: x['year'] if x['month'] <= end_month else x['year'] + 1,
                axis=1
            )
        
        group_cols = [self.site_column, 'season_year']
        
        if include_count:
            result = data.groupby(group_cols).agg(
                value=(self.value_column, agg_func),
                count=(self.value_column, 'count')
            ).reset_index()
        else:
            result = data.groupby(group_cols).agg(
                value=(self.value_column, agg_func)
            ).reset_index()
        
        result = result.rename(columns={'season_year': 'year'})
        result['start_month'] = start_month
        result['end_month'] = end_month
        
        logger.info(f"Created {len(result)} growing season records")
        
        return result

    def to_custom_period(
        self,
        months: List[int],
        agg_func: Union[str, Callable] = 'mean',
        include_count: bool = True,
        period_name: str = 'custom'
    ) -> pd.DataFrame:
        """
        Aggregate data to custom month periods.

        Parameters
        ----------
        months : List[int]
            List of months to include (1-12).
        agg_func : Union[str, Callable], optional
            Aggregation function. Default is 'mean'.
        include_count : bool, optional
            Include count of observations. Default is True.
        period_name : str, optional
            Name for the custom period. Default is 'custom'.

        Returns
        -------
        pd.DataFrame
            Custom period aggregated data.

        Examples
        --------
        >>> # Summer months only
        >>> summer = aggregator.to_custom_period(months=[6, 7, 8], period_name='summer')
        >>> # Winter months
        >>> winter = aggregator.to_custom_period(months=[12, 1, 2], period_name='winter')
        """
        logger.info(f"Aggregating to custom period: months {months}")
        
        data = self.data.copy()
        mask = data['month'].isin(months)
        data = data[mask].copy()
        
        # Handle periods spanning year boundary
        min_month = min(months)
        max_month = max(months)
        
        if max_month - min_month + 1 != len(months):
            # Non-contiguous or spans year boundary
            # Assign to year of first month in sequence
            first_month = months[0]
            data['period_year'] = data.apply(
                lambda x: x['year'] if x['month'] >= first_month else x['year'],
                axis=1
            )
        else:
            data['period_year'] = data['year']
        
        group_cols = [self.site_column, 'period_year']
        
        if include_count:
            result = data.groupby(group_cols).agg(
                value=(self.value_column, agg_func),
                count=(self.value_column, 'count')
            ).reset_index()
        else:
            result = data.groupby(group_cols).agg(
                value=(self.value_column, agg_func)
            ).reset_index()
        
        result = result.rename(columns={'period_year': 'year'})
        result['period'] = period_name
        result['months'] = str(months)
        
        logger.info(f"Created {len(result)} {period_name} records")
        
        return result

    def to_weekly(
        self,
        agg_func: Union[str, Callable] = 'mean',
        include_count: bool = True
    ) -> pd.DataFrame:
        """
        Aggregate data to weekly resolution.

        Parameters
        ----------
        agg_func : Union[str, Callable], optional
            Aggregation function. Default is 'mean'.
        include_count : bool, optional
            Include count of observations. Default is True.

        Returns
        -------
        pd.DataFrame
            Weekly aggregated data.
        """
        logger.info(f"Aggregating to weekly using {agg_func}")
        
        group_cols = [self.site_column, 'year', 'week']
        
        if include_count:
            result = self.data.groupby(group_cols).agg(
                value=(self.value_column, agg_func),
                count=(self.value_column, 'count')
            ).reset_index()
        else:
            result = self.data.groupby(group_cols).agg(
                value=(self.value_column, agg_func)
            ).reset_index()
        
        logger.info(f"Created {len(result)} weekly records")
        
        return result

    def resample(
        self,
        freq: str,
        agg_func: Union[str, Callable] = 'mean'
    ) -> pd.DataFrame:
        """
        Resample data using pandas resample functionality.

        Parameters
        ----------
        freq : str
            Resampling frequency string (e.g., 'D', 'W', 'M', 'Q', 'Y').
        agg_func : Union[str, Callable], optional
            Aggregation function. Default is 'mean'.

        Returns
        -------
        pd.DataFrame
            Resampled data.

        Examples
        --------
        >>> quarterly = aggregator.resample('Q')
        >>> biweekly = aggregator.resample('2W')
        """
        logger.info(f"Resampling to frequency '{freq}'")
        
        results = []
        
        for site_no, site_data in self.data.groupby(self.site_column):
            site_ts = site_data.set_index(self.date_column)[self.value_column]
            resampled = site_ts.resample(freq).agg(agg_func)
            
            site_result = resampled.reset_index()
            site_result.columns = ['date', 'value']
            site_result[self.site_column] = site_no
            
            results.append(site_result)
        
        result = pd.concat(results, ignore_index=True)
        
        logger.info(f"Created {len(result)} resampled records")
        
        return result

    def calculate_statistics(
        self,
        groupby: List[str] = None
    ) -> pd.DataFrame:
        """
        Calculate comprehensive statistics for the data.

        Parameters
        ----------
        groupby : List[str], optional
            Columns to group by. Default groups by site only.

        Returns
        -------
        pd.DataFrame
            DataFrame with statistics including mean, median, std, min, max,
            count, and percentiles.
        """
        if groupby is None:
            groupby = [self.site_column]
        
        result = self.data.groupby(groupby)[self.value_column].agg([
            'count',
            'mean',
            'std',
            'min',
            ('q25', lambda x: x.quantile(0.25)),
            'median',
            ('q75', lambda x: x.quantile(0.75)),
            'max',
        ]).reset_index()
        
        return result

    def get_trends(
        self,
        period: str = 'annual',
        parallel: bool = True,
        n_workers: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Calculate linear trends for each well.

        Parameters
        ----------
        period : str, optional
            Period for trend analysis ('annual' or 'monthly'). Default is 'annual'.
        parallel : bool, optional
            If True, compute trends in parallel. Default is True.
        n_workers : int, optional
            Number of parallel workers.

        Returns
        -------
        pd.DataFrame
            DataFrame with trend statistics for each well.
        """
        from scipy import stats
        
        if period == 'annual':
            agg_data = self.to_annual(include_count=False)
        else:
            agg_data = self.to_monthly(include_count=False)
        
        # Function to calculate trend for a single site
        def calc_site_trend(site_data_tuple):
            site_no, site_data = site_data_tuple
            if len(site_data) < 3:
                return None
            
            x = np.arange(len(site_data))
            y = site_data['value'].values
            
            # Remove NaN values
            mask = ~np.isnan(y)
            if mask.sum() < 3:
                return None
            
            slope, intercept, r_value, p_value, std_err = stats.linregress(
                x[mask], y[mask]
            )
            
            return {
                self.site_column: site_no,
                'slope': slope,
                'intercept': intercept,
                'r_squared': r_value ** 2,
                'p_value': p_value,
                'std_err': std_err,
                'n_observations': mask.sum(),
                'trend_direction': 'increasing' if slope > 0 else 'decreasing',
            }
        
        # Group data by site
        grouped = list(agg_data.groupby(self.site_column))
        
        use_parallel = parallel and DASK_AVAILABLE and len(grouped) > 10
        
        if use_parallel:
            logger.info(f"Computing trends for {len(grouped)} sites in parallel")
            
            tasks = [delayed(calc_site_trend)((site_no, df)) for site_no, df in grouped]
            
            with ProgressBar():
                results = compute(*tasks, scheduler='threads', num_workers=n_workers)
            
            trends = [r for r in results if r is not None]
        else:
            trends = []
            for site_no, site_data in grouped:
                result = calc_site_trend((site_no, site_data))
                if result is not None:
                    trends.append(result)
        
        return pd.DataFrame(trends)


def aggregate_by_period(
    data: pd.DataFrame,
    period: str = 'monthly',
    date_column: str = 'lev_dt',
    value_column: str = 'lev_va',
    site_column: str = 'site_no',
    agg_func: Union[str, Callable] = 'mean',
    **kwargs
) -> pd.DataFrame:
    """
    Convenience function for temporal aggregation.

    Parameters
    ----------
    data : pd.DataFrame
        Input groundwater data.
    period : str
        Aggregation period: 'daily', 'weekly', 'monthly', 'annual',
        'growing_season', or 'water_year'.
    date_column : str
        Name of the date column.
    value_column : str
        Name of the value column.
    site_column : str
        Name of the site column.
    agg_func : Union[str, Callable]
        Aggregation function.
    **kwargs
        Additional arguments for specific aggregation methods.

    Returns
    -------
    pd.DataFrame
        Aggregated data.

    Examples
    --------
    >>> monthly = aggregate_by_period(data, period='monthly')
    >>> water_year = aggregate_by_period(data, period='water_year')
    """
    aggregator = TemporalAggregator(
        data,
        date_column=date_column,
        value_column=value_column,
        site_column=site_column
    )
    
    if period == 'daily':
        return data
    elif period == 'weekly':
        return aggregator.to_weekly(agg_func=agg_func, **kwargs)
    elif period == 'monthly':
        return aggregator.to_monthly(agg_func=agg_func, **kwargs)
    elif period == 'annual':
        return aggregator.to_annual(agg_func=agg_func, **kwargs)
    elif period == 'water_year':
        return aggregator.to_annual(agg_func=agg_func, water_year=True, **kwargs)
    elif period == 'growing_season':
        return aggregator.to_growing_season(agg_func=agg_func, **kwargs)
    else:
        raise ValueError(
            f"Unknown period '{period}'. Options: daily, weekly, monthly, "
            "annual, water_year, growing_season"
        )
