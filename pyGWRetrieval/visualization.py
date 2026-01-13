"""
Visualization module for pyGWRetrieval.

This module provides functions and classes for visualizing groundwater level
data including:
- Time series plots for individual or multiple wells
- Spatial distribution maps with automatic zoom
- Statistical summary plots
- Trend analysis visualizations
- Interactive map visualizations

Dependencies:
    - matplotlib
    - pandas
    - geopandas (optional, for spatial plots)
    - seaborn (optional, for enhanced styling)
    - contextily (optional, for basemaps)
"""

import logging
from typing import Optional, Union, List, Tuple, Dict
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.colors as mcolors
from matplotlib.patches import Patch
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Try to import optional dependencies
try:
    import seaborn as sns
    HAS_SEABORN = True
except ImportError:
    HAS_SEABORN = False

try:
    import geopandas as gpd
    from shapely.geometry import Point, box
    HAS_GEOPANDAS = True
except ImportError:
    HAS_GEOPANDAS = False

try:
    import contextily as ctx
    HAS_CONTEXTILY = True
except ImportError:
    HAS_CONTEXTILY = False


class GroundwaterPlotter:
    """
    Class for visualizing groundwater level data.

    This class provides various plotting methods for analyzing and presenting
    groundwater level time series and spatial distributions.

    Parameters
    ----------
    data : pd.DataFrame
        Groundwater level data with columns including:
        - site_no: USGS site number
        - lev_dt: Date of measurement
        - lev_va: Water level value
    date_column : str, optional
        Name of the date column. Default is 'lev_dt'.
    value_column : str, optional
        Name of the value column. Default is 'lev_va'.
    site_column : str, optional
        Name of the site column. Default is 'site_no'.
    style : str, optional
        Plotting style ('default', 'seaborn', 'ggplot'). Default is 'default'.

    Examples
    --------
    >>> plotter = GroundwaterPlotter(gw_data)
    >>> fig = plotter.plot_time_series()
    >>> fig.savefig('time_series.png')
    """

    # Default color palette
    COLORS = [
        '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
        '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'
    ]

    def __init__(
        self,
        data: pd.DataFrame,
        date_column: str = 'lev_dt',
        value_column: str = 'lev_va',
        site_column: str = 'site_no',
        style: str = 'default'
    ):
        """Initialize the GroundwaterPlotter."""
        self.data = data.copy()
        self.date_column = date_column
        self.value_column = value_column
        self.site_column = site_column
        
        self._prepare_data()
        self._set_style(style)
        
        logger.info(f"Initialized GroundwaterPlotter with {len(self.data)} records")

    def _prepare_data(self) -> None:
        """Prepare data for plotting."""
        # Ensure date column is datetime
        if not pd.api.types.is_datetime64_any_dtype(self.data[self.date_column]):
            self.data[self.date_column] = pd.to_datetime(
                self.data[self.date_column], format='mixed', errors='coerce'
            )
        
        # Ensure value column is numeric
        self.data[self.value_column] = pd.to_numeric(
            self.data[self.value_column], errors='coerce'
        )

    def _set_style(self, style: str) -> None:
        """Set the plotting style."""
        if style == 'seaborn' and HAS_SEABORN:
            sns.set_theme()
        elif style == 'ggplot':
            plt.style.use('ggplot')
        else:
            plt.style.use('default')

    def plot_time_series(
        self,
        wells: Optional[List[str]] = None,
        ax: Optional[plt.Axes] = None,
        figsize: Tuple[int, int] = (12, 6),
        title: Optional[str] = None,
        xlabel: str = 'Date',
        ylabel: str = 'Depth to Water (ft)',
        legend: bool = True,
        invert_yaxis: bool = True,
        **kwargs
    ) -> plt.Figure:
        """
        Plot time series of groundwater levels for one or more wells.

        Parameters
        ----------
        wells : List[str], optional
            List of well site numbers to plot. If None, plots all wells
            (up to 10 for readability).
        ax : plt.Axes, optional
            Matplotlib axes to plot on. Creates new figure if None.
        figsize : Tuple[int, int], optional
            Figure size in inches. Default is (12, 6).
        title : str, optional
            Plot title. Default generates automatically.
        xlabel : str, optional
            X-axis label. Default is 'Date'.
        ylabel : str, optional
            Y-axis label. Default is 'Depth to Water (ft)'.
        legend : bool, optional
            Show legend. Default is True.
        invert_yaxis : bool, optional
            Invert y-axis (deeper water = higher value). Default is True.
        **kwargs
            Additional arguments passed to plt.plot().

        Returns
        -------
        plt.Figure
            Matplotlib figure object.

        Examples
        --------
        >>> fig = plotter.plot_time_series()
        >>> fig = plotter.plot_time_series(wells=['390000119000001'])
        """
        if ax is None:
            fig, ax = plt.subplots(figsize=figsize)
        else:
            fig = ax.get_figure()
        
        # Select wells to plot
        all_wells = self.data[self.site_column].unique()
        
        if wells is None:
            wells = all_wells[:10]  # Limit to 10 wells for readability
            if len(all_wells) > 10:
                logger.warning(f"Showing only 10 of {len(all_wells)} wells")
        
        # Plot each well
        for i, well in enumerate(wells):
            well_data = self.data[self.data[self.site_column] == well].sort_values(
                self.date_column
            )
            
            if well_data.empty:
                logger.warning(f"No data for well {well}")
                continue
            
            color = self.COLORS[i % len(self.COLORS)]
            
            ax.plot(
                well_data[self.date_column],
                well_data[self.value_column],
                label=well,
                color=color,
                **kwargs
            )
        
        # Formatting
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        
        if title:
            ax.set_title(title)
        else:
            ax.set_title(f'Groundwater Levels ({len(wells)} wells)')
        
        if invert_yaxis:
            ax.invert_yaxis()
        
        if legend and len(wells) <= 10:
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        
        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        ax.xaxis.set_major_locator(mdates.YearLocator())
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        
        return fig

    def plot_single_well(
        self,
        site_no: str,
        ax: Optional[plt.Axes] = None,
        figsize: Tuple[int, int] = (12, 6),
        show_trend: bool = True,
        show_stats: bool = True,
        **kwargs
    ) -> plt.Figure:
        """
        Create a detailed plot for a single well.

        Parameters
        ----------
        site_no : str
            USGS site number of the well.
        ax : plt.Axes, optional
            Matplotlib axes to plot on.
        figsize : Tuple[int, int], optional
            Figure size. Default is (12, 6).
        show_trend : bool, optional
            Show linear trend line. Default is True.
        show_stats : bool, optional
            Show statistics annotation. Default is True.
        **kwargs
            Additional arguments passed to plt.plot().

        Returns
        -------
        plt.Figure
            Matplotlib figure object.
        """
        well_data = self.data[self.data[self.site_column] == site_no].sort_values(
            self.date_column
        )
        
        if well_data.empty:
            raise ValueError(f"No data found for well {site_no}")
        
        if ax is None:
            fig, ax = plt.subplots(figsize=figsize)
        else:
            fig = ax.get_figure()
        
        # Main time series
        ax.plot(
            well_data[self.date_column],
            well_data[self.value_column],
            'b-',
            linewidth=1,
            alpha=0.7,
            label='Observed',
            **kwargs
        )
        
        # Scatter points
        ax.scatter(
            well_data[self.date_column],
            well_data[self.value_column],
            c='blue',
            s=10,
            alpha=0.5
        )
        
        # Add trend line
        if show_trend and len(well_data) > 2:
            x_numeric = mdates.date2num(well_data[self.date_column])
            y = well_data[self.value_column].values
            
            # Remove NaN
            mask = ~np.isnan(y)
            if mask.sum() > 2:
                coeffs = np.polyfit(x_numeric[mask], y[mask], 1)
                trend_line = np.poly1d(coeffs)
                ax.plot(
                    well_data[self.date_column],
                    trend_line(x_numeric),
                    'r--',
                    linewidth=2,
                    label=f'Trend (slope={coeffs[0]:.4f})'
                )
        
        # Add statistics annotation
        if show_stats:
            stats_text = (
                f"n = {len(well_data)}\n"
                f"Mean = {well_data[self.value_column].mean():.2f}\n"
                f"Std = {well_data[self.value_column].std():.2f}\n"
                f"Min = {well_data[self.value_column].min():.2f}\n"
                f"Max = {well_data[self.value_column].max():.2f}"
            )
            ax.text(
                0.02, 0.98, stats_text,
                transform=ax.transAxes,
                fontsize=9,
                verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5)
            )
        
        # Formatting
        station_name = well_data.get('station_nm', pd.Series([site_no])).iloc[0]
        ax.set_title(f'Groundwater Levels: {station_name}\n(Site: {site_no})')
        ax.set_xlabel('Date')
        ax.set_ylabel('Depth to Water (ft)')
        ax.invert_yaxis()
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        return fig

    def plot_comparison(
        self,
        wells: List[str],
        normalize: bool = False,
        figsize: Tuple[int, int] = (12, 8),
        **kwargs
    ) -> plt.Figure:
        """
        Create comparison plots for multiple wells.

        Parameters
        ----------
        wells : List[str]
            List of well site numbers to compare.
        normalize : bool, optional
            Normalize data for comparison. Default is False.
        figsize : Tuple[int, int], optional
            Figure size. Default is (12, 8).
        **kwargs
            Additional arguments for plotting.

        Returns
        -------
        plt.Figure
            Matplotlib figure object.
        """
        n_wells = len(wells)
        
        if n_wells == 0:
            raise ValueError("At least one well must be specified")
        
        fig, axes = plt.subplots(n_wells, 1, figsize=figsize, sharex=True)
        
        if n_wells == 1:
            axes = [axes]
        
        for i, (well, ax) in enumerate(zip(wells, axes)):
            well_data = self.data[self.data[self.site_column] == well].sort_values(
                self.date_column
            )
            
            if well_data.empty:
                ax.text(0.5, 0.5, f'No data for {well}', transform=ax.transAxes,
                       ha='center', va='center')
                continue
            
            y_values = well_data[self.value_column]
            
            if normalize:
                y_values = (y_values - y_values.mean()) / y_values.std()
                ylabel = 'Normalized Depth'
            else:
                ylabel = 'Depth (ft)'
            
            color = self.COLORS[i % len(self.COLORS)]
            
            ax.plot(
                well_data[self.date_column],
                y_values,
                color=color,
                **kwargs
            )
            
            ax.set_ylabel(ylabel)
            ax.set_title(well, fontsize=10)
            ax.invert_yaxis()
            ax.grid(True, alpha=0.3)
        
        axes[-1].set_xlabel('Date')
        fig.suptitle('Groundwater Level Comparison', fontsize=14)
        plt.tight_layout()
        
        return fig

    def plot_monthly_boxplot(
        self,
        wells: Optional[List[str]] = None,
        figsize: Tuple[int, int] = (12, 6),
        **kwargs
    ) -> plt.Figure:
        """
        Create box plots of water levels by month.

        Parameters
        ----------
        wells : List[str], optional
            Wells to include. If None, uses all wells.
        figsize : Tuple[int, int], optional
            Figure size. Default is (12, 6).
        **kwargs
            Additional arguments for boxplot.

        Returns
        -------
        plt.Figure
            Matplotlib figure object.
        """
        data = self.data.copy()
        
        if wells is not None:
            data = data[data[self.site_column].isin(wells)]
        
        data['month'] = data[self.date_column].dt.month
        
        fig, ax = plt.subplots(figsize=figsize)
        
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        if HAS_SEABORN:
            sns.boxplot(
                data=data,
                x='month',
                y=self.value_column,
                ax=ax,
                **kwargs
            )
        else:
            monthly_data = [
                data[data['month'] == m][self.value_column].dropna()
                for m in range(1, 13)
            ]
            ax.boxplot(monthly_data, labels=month_names, **kwargs)
        
        ax.set_xlabel('Month')
        ax.set_ylabel('Depth to Water (ft)')
        ax.set_title('Monthly Distribution of Groundwater Levels')
        ax.set_xticklabels(month_names)
        ax.invert_yaxis()
        
        plt.tight_layout()
        
        return fig

    def plot_annual_summary(
        self,
        wells: Optional[List[str]] = None,
        agg_func: str = 'mean',
        figsize: Tuple[int, int] = (12, 6),
        **kwargs
    ) -> plt.Figure:
        """
        Plot annual summary statistics.

        Parameters
        ----------
        wells : List[str], optional
            Wells to include. If None, uses all wells.
        agg_func : str, optional
            Aggregation function. Default is 'mean'.
        figsize : Tuple[int, int], optional
            Figure size. Default is (12, 6).
        **kwargs
            Additional arguments for plotting.

        Returns
        -------
        plt.Figure
            Matplotlib figure object.
        """
        data = self.data.copy()
        
        if wells is not None:
            data = data[data[self.site_column].isin(wells)]
        
        data['year'] = data[self.date_column].dt.year
        
        # Aggregate by year
        annual = data.groupby('year')[self.value_column].agg([
            'mean', 'std', 'min', 'max', 'count'
        ]).reset_index()
        
        fig, ax = plt.subplots(figsize=figsize)
        
        # Plot mean with error bars
        ax.errorbar(
            annual['year'],
            annual['mean'],
            yerr=annual['std'],
            fmt='o-',
            capsize=3,
            label='Mean ± Std',
            **kwargs
        )
        
        # Add range
        ax.fill_between(
            annual['year'],
            annual['min'],
            annual['max'],
            alpha=0.2,
            label='Min-Max Range'
        )
        
        ax.set_xlabel('Year')
        ax.set_ylabel('Depth to Water (ft)')
        ax.set_title('Annual Groundwater Level Summary')
        ax.invert_yaxis()
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        return fig

    def plot_heatmap(
        self,
        well: str,
        figsize: Tuple[int, int] = (14, 8),
        cmap: str = 'RdYlBu',
        **kwargs
    ) -> plt.Figure:
        """
        Create a heatmap of water levels by year and month.

        Parameters
        ----------
        well : str
            Well site number.
        figsize : Tuple[int, int], optional
            Figure size. Default is (14, 8).
        cmap : str, optional
            Colormap name. Default is 'RdYlBu'.
        **kwargs
            Additional arguments for heatmap.

        Returns
        -------
        plt.Figure
            Matplotlib figure object.
        """
        well_data = self.data[self.data[self.site_column] == well].copy()
        
        if well_data.empty:
            raise ValueError(f"No data found for well {well}")
        
        well_data['year'] = well_data[self.date_column].dt.year
        well_data['month'] = well_data[self.date_column].dt.month
        
        # Pivot for heatmap
        pivot = well_data.pivot_table(
            values=self.value_column,
            index='year',
            columns='month',
            aggfunc='mean'
        )
        
        fig, ax = plt.subplots(figsize=figsize)
        
        if HAS_SEABORN:
            sns.heatmap(
                pivot,
                cmap=cmap,
                ax=ax,
                cbar_kws={'label': 'Depth to Water (ft)'},
                **kwargs
            )
        else:
            im = ax.imshow(pivot.values, cmap=cmap, aspect='auto')
            ax.set_xticks(range(12))
            ax.set_xticklabels(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                               'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
            ax.set_yticks(range(len(pivot.index)))
            ax.set_yticklabels(pivot.index)
            plt.colorbar(im, ax=ax, label='Depth to Water (ft)')
        
        ax.set_xlabel('Month')
        ax.set_ylabel('Year')
        ax.set_title(f'Groundwater Level Heatmap: {well}')
        
        plt.tight_layout()
        
        return fig

    def plot_spatial_distribution(
        self,
        wells_gdf: 'gpd.GeoDataFrame',
        value_col: Optional[str] = None,
        figsize: Tuple[int, int] = (10, 10),
        cmap: str = 'viridis',
        basemap: bool = False,
        **kwargs
    ) -> plt.Figure:
        """
        Plot spatial distribution of wells and their values.

        Parameters
        ----------
        wells_gdf : gpd.GeoDataFrame
            GeoDataFrame with well locations.
        value_col : str, optional
            Column for coloring points. Default uses mean water level.
        figsize : Tuple[int, int], optional
            Figure size. Default is (10, 10).
        cmap : str, optional
            Colormap. Default is 'viridis'.
        basemap : bool, optional
            Add contextily basemap if available. Default is False.
        **kwargs
            Additional arguments for scatter plot.

        Returns
        -------
        plt.Figure
            Matplotlib figure object.
        """
        if not HAS_GEOPANDAS:
            raise ImportError("geopandas is required for spatial plots")
        
        fig, ax = plt.subplots(figsize=figsize)
        
        # Calculate mean values per well if needed
        if value_col is None:
            mean_values = self.data.groupby(self.site_column)[self.value_column].mean()
            wells_gdf = wells_gdf.copy()
            wells_gdf['mean_value'] = wells_gdf['site_no'].map(mean_values)
            value_col = 'mean_value'
        
        wells_gdf.plot(
            column=value_col,
            cmap=cmap,
            legend=True,
            ax=ax,
            legend_kwds={'label': 'Depth to Water (ft)'},
            **kwargs
        )
        
        # Add basemap if requested
        if basemap:
            try:
                import contextily as ctx
                ctx.add_basemap(ax, crs=wells_gdf.crs, source=ctx.providers.OpenStreetMap.Mapnik)
            except ImportError:
                logger.warning("contextily not available for basemap")
        
        ax.set_title('Spatial Distribution of Groundwater Wells')
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        
        plt.tight_layout()
        
        return fig

    def plot_wells_map(
        self,
        wells_gdf: Optional['gpd.GeoDataFrame'] = None,
        value_column: Optional[str] = None,
        agg_func: str = 'mean',
        title: Optional[str] = None,
        figsize: Optional[Tuple[int, int]] = None,
        cmap: str = 'RdYlBu_r',
        marker_size: Optional[int] = None,
        add_basemap: bool = True,
        basemap_source: str = 'CartoDB.Positron',
        show_colorbar: bool = True,
        show_legend: bool = True,
        group_by_column: Optional[str] = None,
        boundary_gdf: Optional['gpd.GeoDataFrame'] = None,
        alpha: float = 0.8,
        edgecolor: str = 'black',
        linewidth: float = 0.5,
        vmin: Optional[float] = None,
        vmax: Optional[float] = None,
        **kwargs
    ) -> plt.Figure:
        """
        Create a spatial map visualization of wells colored by water levels.

        Automatically adjusts zoom level based on the spatial extent of the data:
        - Single zip code / small area: zooms in to local scale
        - Multiple cities / states: shows regional or national view
        - Full US coverage: shows continental US map

        Parameters
        ----------
        wells_gdf : gpd.GeoDataFrame, optional
            GeoDataFrame with well locations. If None, creates from data
            using dec_lat_va and dec_long_va columns.
        value_column : str, optional
            Column for coloring points. If None, uses aggregated water levels.
        agg_func : str, optional
            Aggregation function for water levels ('mean', 'median', 'min', 'max').
            Default is 'mean'.
        title : str, optional
            Map title. Auto-generated if None.
        figsize : Tuple[int, int], optional
            Figure size. Auto-determined based on extent if None.
        cmap : str, optional
            Colormap for water levels. Default is 'RdYlBu_r' (red=deep, blue=shallow).
        marker_size : int, optional
            Size of well markers. Auto-scaled based on number of wells if None.
        add_basemap : bool, optional
            Add contextily basemap. Default is True.
        basemap_source : str, optional
            Basemap provider. Options: 'CartoDB.Positron', 'CartoDB.DarkMatter',
            'OpenStreetMap.Mapnik', 'Esri.WorldImagery', 'Esri.WorldStreetMap'.
            Default is 'CartoDB.Positron'.
        show_colorbar : bool, optional
            Show colorbar for water levels. Default is True.
        show_legend : bool, optional
            Show legend. Default is True.
        group_by_column : str, optional
            Column to group wells by (e.g., 'source_zipcode'). Adds annotations.
        boundary_gdf : gpd.GeoDataFrame, optional
            GeoDataFrame with boundary polygons to overlay (e.g., study area).
        alpha : float, optional
            Transparency of markers. Default is 0.8.
        edgecolor : str, optional
            Edge color of markers. Default is 'black'.
        linewidth : float, optional
            Edge line width. Default is 0.5.
        vmin : float, optional
            Minimum value for colormap normalization.
        vmax : float, optional
            Maximum value for colormap normalization.
        **kwargs
            Additional arguments passed to scatter plot.

        Returns
        -------
        plt.Figure
            Matplotlib figure with the map.

        Examples
        --------
        >>> # Basic map with auto-zoom
        >>> fig = plotter.plot_wells_map()
        
        >>> # Map grouped by zip code
        >>> fig = plotter.plot_wells_map(group_by_column='source_zipcode')
        
        >>> # Custom styling
        >>> fig = plotter.plot_wells_map(
        ...     cmap='viridis',
        ...     marker_size=100,
        ...     basemap_source='Esri.WorldImagery'
        ... )
        """
        if not HAS_GEOPANDAS:
            raise ImportError("geopandas is required for spatial map plots")
        
        # Create GeoDataFrame from data if not provided
        if wells_gdf is None:
            wells_gdf = self._create_wells_geodataframe()
        
        if wells_gdf is None or wells_gdf.empty:
            logger.warning("No well locations available for mapping")
            return plt.figure()
        
        # Calculate aggregated values per well
        wells_gdf = self._add_aggregated_values(wells_gdf, agg_func, value_column)
        
        # Determine map extent and auto-configure
        extent = self._calculate_extent(wells_gdf)
        map_config = self._get_map_configuration(extent, len(wells_gdf))
        
        # Override with user settings
        if figsize is not None:
            map_config['figsize'] = figsize
        if marker_size is not None:
            map_config['marker_size'] = marker_size
        
        # Create figure
        fig, ax = plt.subplots(figsize=map_config['figsize'])
        
        # Reproject to Web Mercator for basemap
        wells_plot = wells_gdf.to_crs(epsg=3857)
        
        # Set color normalization
        plot_col = value_column if value_column else f'{agg_func}_water_level'
        values = wells_plot[plot_col].dropna()
        
        if vmin is None:
            vmin = values.min()
        if vmax is None:
            vmax = values.max()
        
        norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
        
        # Plot boundary if provided
        if boundary_gdf is not None:
            boundary_plot = boundary_gdf.to_crs(epsg=3857)
            boundary_plot.boundary.plot(
                ax=ax, color='darkblue', linewidth=2, 
                linestyle='--', label='Study Area'
            )
        
        # Plot wells
        scatter = ax.scatter(
            wells_plot.geometry.x,
            wells_plot.geometry.y,
            c=wells_plot[plot_col],
            cmap=cmap,
            s=map_config['marker_size'],
            alpha=alpha,
            edgecolors=edgecolor,
            linewidths=linewidth,
            norm=norm,
            zorder=5,
            **kwargs
        )
        
        # Add basemap
        if add_basemap and HAS_CONTEXTILY:
            try:
                basemap_providers = {
                    'CartoDB.Positron': ctx.providers.CartoDB.Positron,
                    'CartoDB.DarkMatter': ctx.providers.CartoDB.DarkMatter,
                    'OpenStreetMap.Mapnik': ctx.providers.OpenStreetMap.Mapnik,
                    'Esri.WorldImagery': ctx.providers.Esri.WorldImagery,
                    'Esri.WorldStreetMap': ctx.providers.Esri.WorldStreetMap,
                    'Esri.WorldTopoMap': ctx.providers.Esri.WorldTopoMap,
                    'Stamen.Terrain': ctx.providers.Stamen.Terrain,
                }
                source = basemap_providers.get(basemap_source, ctx.providers.CartoDB.Positron)
                ctx.add_basemap(ax, source=source, zoom=map_config['zoom_level'])
            except Exception as e:
                logger.warning(f"Could not add basemap: {e}")
        
        # Add colorbar
        if show_colorbar:
            cbar = plt.colorbar(scatter, ax=ax, shrink=0.7, pad=0.02)
            cbar.set_label('Depth to Water (feet below surface)', fontsize=10)
        
        # Add group annotations if requested
        if group_by_column and group_by_column in wells_plot.columns:
            self._add_group_annotations(ax, wells_plot, group_by_column)
        
        # Set title
        if title is None:
            n_wells = len(wells_gdf)
            title = f'Groundwater Wells Map\n({n_wells} wells, colored by {agg_func} water level)'
            if group_by_column:
                n_groups = wells_gdf[group_by_column].nunique()
                title += f'\n{n_groups} {group_by_column.replace("_", " ")}s'
        
        ax.set_title(title, fontsize=12, fontweight='bold')
        
        # Add scale indicator text
        self._add_scale_info(ax, extent)
        
        # Remove axis labels for cleaner map look
        ax.set_xlabel('')
        ax.set_ylabel('')
        
        # Add legend if requested
        if show_legend:
            legend_elements = [
                plt.scatter([], [], c='gray', s=map_config['marker_size'], 
                           alpha=alpha, edgecolors=edgecolor, linewidths=linewidth,
                           label=f'Well ({agg_func} level)')
            ]
            if boundary_gdf is not None:
                legend_elements.append(
                    plt.Line2D([0], [0], color='darkblue', linewidth=2, 
                              linestyle='--', label='Study Area')
                )
            ax.legend(handles=legend_elements, loc='lower right', fontsize=9)
        
        plt.tight_layout()
        
        return fig

    def _create_wells_geodataframe(self) -> Optional['gpd.GeoDataFrame']:
        """Create GeoDataFrame from data with lat/lon columns."""
        if 'dec_lat_va' not in self.data.columns or 'dec_long_va' not in self.data.columns:
            logger.warning("Latitude/longitude columns not found in data")
            return None
        
        # Get unique well locations
        wells = self.data.groupby(self.site_column).agg({
            'dec_lat_va': 'first',
            'dec_long_va': 'first',
            'station_nm': 'first' if 'station_nm' in self.data.columns else lambda x: None
        }).reset_index()
        
        # Add source_zipcode if present
        if 'source_zipcode' in self.data.columns:
            zipcode_map = self.data.groupby(self.site_column)['source_zipcode'].first()
            wells['source_zipcode'] = wells[self.site_column].map(zipcode_map)
        
        # Create geometry
        geometry = [
            Point(lon, lat) 
            for lon, lat in zip(wells['dec_long_va'], wells['dec_lat_va'])
        ]
        
        gdf = gpd.GeoDataFrame(wells, geometry=geometry, crs='EPSG:4326')
        
        return gdf

    def _add_aggregated_values(
        self, 
        wells_gdf: 'gpd.GeoDataFrame', 
        agg_func: str,
        value_column: Optional[str]
    ) -> 'gpd.GeoDataFrame':
        """Add aggregated water level values to wells GeoDataFrame."""
        if value_column and value_column in wells_gdf.columns:
            return wells_gdf
        
        # Calculate aggregated values
        agg_dict = {
            'mean': 'mean',
            'median': 'median',
            'min': 'min',
            'max': 'max',
            'std': 'std',
            'count': 'count'
        }
        
        func = agg_dict.get(agg_func, 'mean')
        agg_values = self.data.groupby(self.site_column)[self.value_column].agg(func)
        
        wells_gdf = wells_gdf.copy()
        wells_gdf[f'{agg_func}_water_level'] = wells_gdf[self.site_column].map(agg_values)
        
        return wells_gdf

    def _calculate_extent(self, wells_gdf: 'gpd.GeoDataFrame') -> Dict:
        """Calculate spatial extent of wells."""
        bounds = wells_gdf.total_bounds  # minx, miny, maxx, maxy
        
        extent = {
            'min_lon': bounds[0],
            'min_lat': bounds[1],
            'max_lon': bounds[2],
            'max_lat': bounds[3],
            'width_deg': bounds[2] - bounds[0],
            'height_deg': bounds[3] - bounds[1],
            'center_lon': (bounds[0] + bounds[2]) / 2,
            'center_lat': (bounds[1] + bounds[3]) / 2,
        }
        
        # Calculate approximate extent in miles
        # Rough approximation: 1 degree latitude ≈ 69 miles
        # 1 degree longitude ≈ 69 * cos(lat) miles
        avg_lat = extent['center_lat']
        extent['width_miles'] = extent['width_deg'] * 69 * np.cos(np.radians(avg_lat))
        extent['height_miles'] = extent['height_deg'] * 69
        extent['max_extent_miles'] = max(extent['width_miles'], extent['height_miles'])
        
        return extent

    def _get_map_configuration(self, extent: Dict, n_wells: int) -> Dict:
        """Get map configuration based on spatial extent and number of wells."""
        max_miles = extent['max_extent_miles']
        
        # Determine scale category
        if max_miles < 20:
            # Local scale (single zip code or small area)
            config = {
                'scale': 'local',
                'figsize': (10, 10),
                'marker_size': 150,
                'zoom_level': 12,
            }
        elif max_miles < 100:
            # Regional scale (multiple zip codes, county level)
            config = {
                'scale': 'regional',
                'figsize': (12, 10),
                'marker_size': 100,
                'zoom_level': 10,
            }
        elif max_miles < 500:
            # State scale
            config = {
                'scale': 'state',
                'figsize': (14, 10),
                'marker_size': 60,
                'zoom_level': 7,
            }
        elif max_miles < 1500:
            # Multi-state scale
            config = {
                'scale': 'multi-state',
                'figsize': (16, 10),
                'marker_size': 40,
                'zoom_level': 5,
            }
        else:
            # Continental US scale
            config = {
                'scale': 'national',
                'figsize': (18, 12),
                'marker_size': 30,
                'zoom_level': 4,
            }
        
        # Adjust marker size based on number of wells
        if n_wells > 500:
            config['marker_size'] = max(10, config['marker_size'] // 3)
        elif n_wells > 200:
            config['marker_size'] = max(15, config['marker_size'] // 2)
        elif n_wells > 100:
            config['marker_size'] = int(config['marker_size'] * 0.75)
        
        logger.info(
            f"Map configuration: {config['scale']} scale, "
            f"extent ~{max_miles:.0f} miles, {n_wells} wells"
        )
        
        return config

    def _add_group_annotations(
        self, 
        ax: plt.Axes, 
        wells_gdf: 'gpd.GeoDataFrame', 
        group_column: str
    ) -> None:
        """Add annotations for well groups (e.g., zip codes)."""
        from matplotlib.patches import FancyBboxPatch
        
        groups = wells_gdf.groupby(group_column)
        
        for group_name, group_data in groups:
            # Get centroid of group
            centroid_x = group_data.geometry.x.mean()
            centroid_y = group_data.geometry.y.mean()
            n_wells = len(group_data)
            
            # Add annotation
            ax.annotate(
                f'{group_name}\n({n_wells})',
                xy=(centroid_x, centroid_y),
                fontsize=8,
                ha='center',
                va='center',
                bbox=dict(
                    boxstyle='round,pad=0.3',
                    facecolor='white',
                    edgecolor='gray',
                    alpha=0.7
                ),
                zorder=10
            )

    def _add_scale_info(self, ax: plt.Axes, extent: Dict) -> None:
        """Add scale information to the map."""
        scale_text = f"Extent: ~{extent['max_extent_miles']:.0f} miles"
        ax.text(
            0.02, 0.02, scale_text,
            transform=ax.transAxes,
            fontsize=8,
            verticalalignment='bottom',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.7)
        )

    def save_figure(
        self,
        fig: plt.Figure,
        filepath: Union[str, Path],
        dpi: int = 300,
        **kwargs
    ) -> None:
        """
        Save a figure to file.

        Parameters
        ----------
        fig : plt.Figure
            Figure to save.
        filepath : Union[str, Path]
            Output file path.
        dpi : int, optional
            Resolution in dots per inch. Default is 300.
        **kwargs
            Additional arguments for savefig.
        """
        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        fig.savefig(filepath, dpi=dpi, bbox_inches='tight', **kwargs)
        logger.info(f"Saved figure to {filepath}")


def quick_plot(
    data: pd.DataFrame,
    wells: Optional[List[str]] = None,
    date_column: str = 'lev_dt',
    value_column: str = 'lev_va',
    site_column: str = 'site_no',
    **kwargs
) -> plt.Figure:
    """
    Quick plotting function for groundwater data.

    Parameters
    ----------
    data : pd.DataFrame
        Groundwater level data.
    wells : List[str], optional
        Wells to plot. If None, plots first 5 wells.
    date_column : str
        Date column name.
    value_column : str
        Value column name.
    site_column : str
        Site column name.
    **kwargs
        Additional plotting arguments.

    Returns
    -------
    plt.Figure
        Matplotlib figure.

    Examples
    --------
    >>> fig = quick_plot(gw_data)
    >>> plt.show()
    """
    plotter = GroundwaterPlotter(
        data,
        date_column=date_column,
        value_column=value_column,
        site_column=site_column
    )
    
    return plotter.plot_time_series(wells=wells, **kwargs)


def plot_wells_map(
    data: pd.DataFrame,
    wells_gdf: Optional['gpd.GeoDataFrame'] = None,
    agg_func: str = 'mean',
    title: Optional[str] = None,
    cmap: str = 'RdYlBu_r',
    add_basemap: bool = True,
    group_by_column: Optional[str] = None,
    date_column: str = 'lev_dt',
    value_column: str = 'lev_va',
    site_column: str = 'site_no',
    **kwargs
) -> plt.Figure:
    """
    Quick function to create a spatial map of groundwater wells.

    This function automatically adjusts the zoom level based on the spatial
    extent of the wells:
    - Single zip code: local zoom
    - Multiple cities: regional view
    - Multiple states: national view

    Parameters
    ----------
    data : pd.DataFrame
        Groundwater level data with columns for site_no, lat, lon, and values.
    wells_gdf : gpd.GeoDataFrame, optional
        Pre-existing GeoDataFrame with well locations.
    agg_func : str, optional
        Aggregation function ('mean', 'median', 'min', 'max'). Default 'mean'.
    title : str, optional
        Map title. Auto-generated if None.
    cmap : str, optional
        Colormap. Default is 'RdYlBu_r' (red=deep, blue=shallow).
    add_basemap : bool, optional
        Add contextily basemap. Default is True.
    group_by_column : str, optional
        Column to group wells by (e.g., 'source_zipcode').
    date_column : str
        Date column name. Default 'lev_dt'.
    value_column : str
        Value column name. Default 'lev_va'.
    site_column : str
        Site column name. Default 'site_no'.
    **kwargs
        Additional arguments passed to plot_wells_map.

    Returns
    -------
    plt.Figure
        Matplotlib figure with the map.

    Examples
    --------
    >>> # Quick map from data
    >>> fig = plot_wells_map(gw_data)
    >>> plt.show()
    
    >>> # Map grouped by zip code with custom colormap
    >>> fig = plot_wells_map(
    ...     gw_data,
    ...     group_by_column='source_zipcode',
    ...     cmap='viridis'
    ... )
    """
    plotter = GroundwaterPlotter(
        data,
        date_column=date_column,
        value_column=value_column,
        site_column=site_column
    )
    
    return plotter.plot_wells_map(
        wells_gdf=wells_gdf,
        agg_func=agg_func,
        title=title,
        cmap=cmap,
        add_basemap=add_basemap,
        group_by_column=group_by_column,
        **kwargs
    )


def create_comparison_map(
    data: pd.DataFrame,
    wells_gdf: Optional['gpd.GeoDataFrame'] = None,
    date_column: str = 'lev_dt',
    value_column: str = 'lev_va',
    site_column: str = 'site_no',
    figsize: Tuple[int, int] = (18, 12),
    add_basemap: bool = True
) -> plt.Figure:
    """
    Create a multi-panel comparison map showing different statistics.

    Creates a 2x2 panel with:
    - Mean water level
    - Data availability (count of records)
    - Min water level (shallowest)
    - Max water level (deepest)

    Parameters
    ----------
    data : pd.DataFrame
        Groundwater level data.
    wells_gdf : gpd.GeoDataFrame, optional
        Pre-existing GeoDataFrame with well locations.
    date_column : str
        Date column name.
    value_column : str
        Value column name.
    site_column : str
        Site column name.
    figsize : Tuple[int, int]
        Figure size. Default (18, 12).
    add_basemap : bool
        Add contextily basemap. Default True.

    Returns
    -------
    plt.Figure
        Matplotlib figure with 4 map panels.
    """
    if not HAS_GEOPANDAS:
        raise ImportError("geopandas is required for map plots")
    
    plotter = GroundwaterPlotter(
        data,
        date_column=date_column,
        value_column=value_column,
        site_column=site_column
    )
    
    # Create wells GeoDataFrame if needed
    if wells_gdf is None:
        wells_gdf = plotter._create_wells_geodataframe()
    
    if wells_gdf is None or wells_gdf.empty:
        logger.warning("No well locations for mapping")
        return plt.figure()
    
    # Calculate all statistics
    stats = data.groupby(site_column)[value_column].agg(['mean', 'min', 'max', 'count'])
    wells_gdf = wells_gdf.merge(stats, left_on=site_column, right_index=True, how='left')
    
    fig, axes = plt.subplots(2, 2, figsize=figsize)
    
    # Reproject for basemap
    wells_plot = wells_gdf.to_crs(epsg=3857)
    
    panels = [
        ('mean', 'Mean Water Level (ft)', 'RdYlBu_r', axes[0, 0]),
        ('count', 'Number of Records', 'YlGn', axes[0, 1]),
        ('min', 'Min (Shallowest) Level (ft)', 'Blues_r', axes[1, 0]),
        ('max', 'Max (Deepest) Level (ft)', 'Reds', axes[1, 1]),
    ]
    
    for col, title, cmap, ax in panels:
        scatter = ax.scatter(
            wells_plot.geometry.x,
            wells_plot.geometry.y,
            c=wells_plot[col],
            cmap=cmap,
            s=50,
            alpha=0.8,
            edgecolors='black',
            linewidths=0.5,
            zorder=5
        )
        
        # Add basemap
        if add_basemap and HAS_CONTEXTILY:
            try:
                ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, zoom='auto')
            except Exception as e:
                logger.warning(f"Could not add basemap: {e}")
        
        plt.colorbar(scatter, ax=ax, shrink=0.7)
        ax.set_title(title, fontsize=11, fontweight='bold')
        ax.set_xlabel('')
        ax.set_ylabel('')
    
    plt.suptitle(f'Groundwater Wells Comparison Map ({len(wells_gdf)} wells)', 
                 fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    
    return fig
